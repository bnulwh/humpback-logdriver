package driver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/containerd/fifo"
	"github.com/docker/docker/api/types/plugins/logdriver"
	"github.com/docker/docker/daemon/logger"
	"github.com/docker/docker/daemon/logger/jsonfilelog"
	"github.com/docker/docker/daemon/logger/loggerutils"
	protoio "github.com/gogo/protobuf/io"
	hblogs "github.com/humpback/gounits/logger"
	"github.com/humpback/gounits/network"
	"github.com/humpback/gounits/rand"
	"github.com/humpback/humpback-logdriver/conf"
	"github.com/humpback/humpback-logdriver/driver/node"
	"github.com/humpback/humpback-logdriver/driver/provider/factory"
	"github.com/humpback/humpback-logdriver/driver/stay"
	"github.com/pkg/errors"
)

const (
	pluginName      = "humpback-logdriver:1.0"
	openFifoTimeout = 5 * time.Second
)

var (
	pluginHostIP string
)

type logContext struct {
	active bool
	fd     logger.Logger
	entry  LogEntry
	stream io.ReadCloser
	info   logger.Info
}

type PluginDriver struct {
	sync.Mutex
	Node            *node.Node
	StayBlocks      *stay.StayBlocks
	ProviderFactory *factory.ProviderFactory
	logCache        *LogCache
	logPairs        map[string]*logContext
}

func New() (*PluginDriver, error) {

	var (
		err   error
		pNode *node.Node
	)

	pluginHostIP = conf.HostIP()
	if pluginHostIP == "" {
		pluginHostIP = network.GetDefaultIP()
	}
	hblogs.INFO("[#driver#] plugin host ipaddr %s.", pluginHostIP)

	nodeConfig := conf.NodeConfigArgs()
	if nodeConfig != nil {
		pNode, err = node.New(pluginName, nodeConfig)
		if err != nil {
			return nil, err
		}
		pNode.Register()
	}

	pluginDriver := &PluginDriver{
		logPairs: make(map[string]*logContext),
	}

	if pNode != nil {
		pluginDriver.Node = pNode
		pluginDriver.Node.WatchProviders(pluginDriver.WatchProvidersHandleFunc)
	}

	dataMap := conf.Providers()
	hblogs.INFO("[#driver#] init providers data %+v.", dataMap)
	stayConfig := conf.StayBlocksConfig()
	hblogs.INFO("[#driver#] stay blocks config %+v.", stayConfig)
	pluginDriver.StayBlocks = stay.NewStayBlocks(stayConfig, pluginDriver.BlockHandleFunc)
	providerFactory := factory.New(conf.Environment())
	providerFactory.Create(dataMap)
	pluginDriver.ProviderFactory = providerFactory
	logCache := NewLogCache(pluginDriver.LogCacheHandleFunc)
	pluginDriver.logCache = logCache
	go pluginDriver.initLogPairs()
	return pluginDriver, nil
}

func (pluginDriver *PluginDriver) initLogPairs() {

	metaInfos := loadMeta()
	nSize := len(metaInfos)
	hblogs.INFO("[#driver#] local meta infos %d...", nSize)
	if nSize == 0 {
		return
	}

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(nSize)
	for metaFile, metaInfo := range metaInfos {
		go func(mFile string, mInfo *MetaInfo) {
			defer waitGroup.Done()
			hblogs.INFO("[#driver#] meta %s >>> %+v...", mFile, mInfo.Info)
			logCtx, err := runLogger(mInfo.File, *mInfo.Info, pluginDriver.logCache)
			if err != nil {
				removeMeta(mFile)
				os.Remove(mInfo.File)
				hblogs.ERROR("[#driver#] runlogger %s(%s) fail. %s", mInfo.Info.Name(), mInfo.Info.ID(), err)
			} else {
				pluginDriver.Lock()
				pluginDriver.logPairs[mFile] = logCtx
				pluginDriver.Unlock()
				hblogs.INFO("[#driver#] runlogger %s(%s) is ready %p.", mInfo.Info.Name(), mInfo.Info.ID(), logCtx)
			}
		}(metaFile, metaInfo)
	}
	waitGroup.Wait()
}

func (pluginDriver *PluginDriver) Close() {

	if pluginDriver.Node != nil {
		pluginDriver.Node.Close()
	}
	pluginDriver.StayBlocks.Close()
	pluginDriver.logCache.Close()
	pluginDriver.ProviderFactory.Close()
	hblogs.INFO("[#driver#] driver closed.")
}

func (pluginDriver *PluginDriver) WatchProvidersHandleFunc(data []byte) {

	if data != nil {
		var err error
		dataMap := map[string]interface{}{}
		if err = json.Unmarshal(data, &dataMap); err != nil {
			hblogs.ERROR("[#driver#] watch providers decode error, %s.", err)
			return
		}
		hblogs.INFO("[#driver#] watch providers data %+v.", dataMap)
		pluginDriver.ProviderFactory.Create(dataMap)
	}
}

func (pluginDriver *PluginDriver) BlockHandleFunc(block string, data []byte) error {

	size := len(data)
	hblogs.INFO("[#driver#] block handle %s -> %d.", block, size)
	var err error
	if len(data) > 0 {
		if err = pluginDriver.ProviderFactory.Write(data); err != nil {
			hblogs.ERROR("[#driver#] block re-send error, %s.", err)
		}
	}
	return err
}

func (pluginDriver *PluginDriver) LogCacheHandleFunc(entries []*LogEntry) {

	if len(entries) > 0 {
		buffer := codecLogsPool.Get().(*bytes.Buffer)
		buffer.Reset()
		codecLogsPool.Put(buffer)
		if err := json.NewEncoder(buffer).Encode(entries); err == nil {
			data := buffer.Bytes()
			if err = pluginDriver.ProviderFactory.Write(data); err != nil {
				hblogs.ERROR("[#driver#] providers write data error, %s.", err)
				pluginDriver.StayBlocks.Write(data)
			}
		}
	}
}

func (pluginDriver *PluginDriver) StartLogging(file string, info logger.Info) error {

	hblogs.INFO("[#driver#] startlogging %s(%s), stream %s.", info.Name(), info.ID(), file)
	baseFile := path.Base(file)
	pluginDriver.Lock()
	if _, exists := pluginDriver.logPairs[baseFile]; exists {
		pluginDriver.Unlock()
		return fmt.Errorf("logger for %q already exists", file)
	}
	pluginDriver.Unlock()

	hblogs.INFO("[#driver#] runlogger %s(%s)....", info.Name(), info.ID())
	logCtx, err := runLogger(file, info, pluginDriver.logCache)
	if err != nil {
		hblogs.ERROR("[#driver#] runlogger %s(%s) fail. %s", info.Name(), info.ID(), err)
		return err
	}

	if err = writeMeta(file, &info); err != nil {
		hblogs.WARN("[#driver#] write meta %s error, %s", baseFile, err)
	}

	hblogs.INFO("[#driver#] runlogger %s(%s) is ready %p.", info.Name(), info.ID(), logCtx)
	pluginDriver.Lock()
	pluginDriver.logPairs[baseFile] = logCtx
	pluginDriver.Unlock()
	return nil
}

func (pluginDriver *PluginDriver) StopLogging(file string) error {

	hblogs.INFO("[#driver#] stoplogging stream %s.", file)
	baseFile := path.Base(file)
	if err := removeMeta(file); err != nil {
		hblogs.WARN("[#driver#] remove meta %s error, %s", baseFile, err)
	}

	pluginDriver.Lock()
	if logCtx, exists := pluginDriver.logPairs[baseFile]; exists {
		stoplogger(file, logCtx)
		delete(pluginDriver.logPairs, baseFile)
		hblogs.INFO("[#driver#] stoplogging %s(%s) successed.", logCtx.info.Name(), logCtx.info.ID())
	}
	pluginDriver.Unlock()
	return nil
}

func (pluginDriver *PluginDriver) ReadLogs(info logger.Info, logConfig logger.ReadConfig) (io.ReadCloser, error) {

	var logCtx *logContext
	pluginDriver.Lock()
	for _, value := range pluginDriver.logPairs {
		if value.info.FullID() == info.ContainerID {
			logCtx = value
			break
		}
	}
	pluginDriver.Unlock()

	if logCtx == nil {
		return nil, fmt.Errorf("logs does not found in local.")
	}

	r, w := io.Pipe()
	logReader, ok := logCtx.fd.(logger.LogReader)
	if !ok {
		return nil, fmt.Errorf("logs does not support reading")
	}

	go func() {
		watcher := logReader.ReadLogs(logConfig)
		enc := protoio.NewUint32DelimitedWriter(w, binary.BigEndian)
		defer func() {
			enc.Close()
			watcher.Close()
		}()

		var buf logdriver.LogEntry
		for {
			select {
			case msg, ok := <-watcher.Msg:
				if !ok {
					w.Close()
					return
				}
				buf.Line = msg.Line
				buf.Partial = msg.Partial
				buf.TimeNano = msg.Timestamp.UnixNano()
				buf.Source = msg.Source
				if err := enc.WriteMsg(&buf); err != nil {
					w.CloseWithError(err)
					return
				}
			case err := <-watcher.Err:
				w.CloseWithError(err)
				return
			}
			buf.Reset()
		}
	}()
	return r, nil
}

func runLogger(file string, info logger.Info, logCache *LogCache) (*logContext, error) {

	if info.LogPath == "" {
		info.LogPath = filepath.Join("/var/log/docker", info.ContainerID)
	}

	if err := os.MkdirAll(filepath.Dir(info.LogPath), 0755); err != nil {
		return nil, errors.Wrap(err, "error setting up logger dir")
	}

	info.Config["max-size"] = "10m"
	info.Config["max-file"] = "1"
	fd, err := jsonfilelog.New(info)
	if err != nil {
		return nil, errors.Wrap(err, "error creating jsonfile logger")
	}

	ctx, _ := context.WithTimeout(context.Background(), openFifoTimeout)
	stream, err := fifo.OpenFifo(ctx, file, syscall.O_RDONLY, 0700)
	if err != nil {
		fd.Close()
		return nil, errors.Wrapf(err, "error opening logger fifo: %q", file)
	}

	tag, _ := loggerutils.ParseLogTag(info, loggerutils.DefaultTemplate)
	hostName, _ := info.Hostname()
	extra, _ := info.ExtraAttributes(nil)
	if extra == nil {
		extra = map[string]string{}
	}

	logCtx := &logContext{
		active: true,
		fd:     fd,
		entry: LogEntry{
			ContainerID:      info.FullID(),
			ContainerName:    info.Name(),
			ContainerCreated: JsonTime{info.ContainerCreated},
			ImageID:          info.ImageFullID(),
			ImageName:        info.ImageName(),
			Command:          info.Command(),
			Tag:              tag,
			Extra:            extra,
			HostName:         hostName,
			HostIP:           pluginHostIP,
		},
		stream: stream,
		info:   info,
	}
	go consumeLogStream(logCache, logCtx)
	return logCtx, nil
}

func stoplogger(file string, logCtx *logContext) error {

	if logCtx != nil {
		logCtx.active = false
		shutdown(logCtx)
	}
	return nil
}

func consumeLogStream(logCache *LogCache, logCtx *logContext) {

	dec := protoio.NewUint32DelimitedReader(logCtx.stream, binary.BigEndian, 1e6)
	defer func() {
		dec.Close()
		shutdown(logCtx)
	}()

	var buf logdriver.LogEntry
	for {
		if !logCtx.active {
			hblogs.INFO("[#driver#] shutting down %s(%s) logger.", logCtx.info.Name(), logCtx.info.ID())
			return
		}

		if err := dec.ReadMsg(&buf); err != nil {
			if err == io.EOF {
				hblogs.INFO("[#driver#] read %s(%s) logger EOF.", logCtx.info.Name(), logCtx.info.ID())
				return
			}
			dec = protoio.NewUint32DelimitedReader(logCtx.stream, binary.BigEndian, 1e6)
			continue
		}

		var msg logger.Message
		msg.Line = buf.Line
		msg.Source = buf.Source
		msg.Partial = buf.Partial
		msg.Timestamp = time.Unix(0, buf.TimeNano)
		if err := logCtx.fd.Log(&msg); err != nil {
			buf.Reset()
			hblogs.ERROR("[#driver#] stream %s(%s) logger write error, %s", logCtx.info.Name(), logCtx.info.ID(), err)
			continue
		}

		seedTime := JsonTime{time.Now()}
		logEntry := LogEntry(logCtx.entry)
		logEntry.LogID = rand.UUID(true)
		logEntry.LogCreated = seedTime
		logEntry.LogTimestamp = seedTime.UnixNano()
		logEntry.Message = string(buf.Line[:])
		logCache.Write(&logEntry)
		buf.Reset()
	}
}

func shutdown(logCtx *logContext) {

	if logCtx != nil {
		if logCtx.fd != nil {
			logCtx.fd.Close()
		}

		if logCtx.stream != nil {
			logCtx.stream.Close()
		}
		logCtx.active = false
	}
}
