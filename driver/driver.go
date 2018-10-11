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
	"github.com/humpback/gounits/rand"
	"github.com/humpback/humpback-logdriver/conf"
	"github.com/humpback/humpback-logdriver/driver/node"
	"github.com/humpback/humpback-logdriver/driver/provider/factory"
	"github.com/pkg/errors"
)

const pluginName string = "humpback-logdriver:1.0"

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
	ProviderFactory *factory.ProviderFactory
	logCache        *LogCache
	logPairs        map[string]*logContext
}

func New() (*PluginDriver, error) {

	var (
		err   error
		pNode *node.Node
	)

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
	providerFactory := factory.New(conf.Environment())
	providerFactory.Create(dataMap)
	pluginDriver.ProviderFactory = providerFactory
	logCache := NewLogCache(pluginDriver.LogCacheHandleFunc)
	pluginDriver.logCache = logCache
	return pluginDriver, nil
}

func (pdriver *PluginDriver) Close() {

	if pdriver.Node != nil {
		pdriver.Node.Close()
	}
	pdriver.logCache.Close()
	pdriver.ProviderFactory.Close()
	hblogs.INFO("[#driver#] driver closed.")
}

func (pdriver *PluginDriver) WatchProvidersHandleFunc(data []byte) {

	if data != nil {
		var err error
		dataMap := map[string]interface{}{}
		if err = json.Unmarshal(data, &dataMap); err != nil {
			hblogs.ERROR("[#driver#] watch providers decode error, %s.", err)
			return
		}
		hblogs.INFO("[#driver#] watch providers data %+v.", dataMap)
		pdriver.ProviderFactory.Create(dataMap)
	}
}

func (pdriver *PluginDriver) LogCacheHandleFunc(entries []*LogEntry) {

	if len(entries) > 0 {
		buffer := codecLogsPool.Get().(*bytes.Buffer)
		buffer.Reset()
		codecLogsPool.Put(buffer)
		if err := json.NewEncoder(buffer).Encode(entries); err == nil {
			err = pdriver.ProviderFactory.Write(buffer.Bytes())
			if err != nil {
				hblogs.ERROR("[#driver#] providers write data error, %s.", err)
				//save this block buffer.
				return
			}
		}
	}
}

func (pdriver *PluginDriver) StartLogging(file string, info logger.Info) error {

	hblogs.INFO("[#driver#] >>>> startlogging %s(%s), stream %s.", info.Name(), info.ID(), file)
	pdriver.Lock()
	if _, exists := pdriver.logPairs[path.Base(file)]; exists {
		pdriver.Unlock()
		return fmt.Errorf("logger for %q already exists", file)
	}
	pdriver.Unlock()

	if info.LogPath == "" {
		info.LogPath = filepath.Join("/var/log/docker", info.ContainerID)
	}

	if err := os.MkdirAll(filepath.Dir(info.LogPath), 0755); err != nil {
		return errors.Wrap(err, "error setting up logger dir")
	}

	info.Config["max-size"] = "10m"
	info.Config["max-file"] = "1"
	fd, err := jsonfilelog.New(info)
	if err != nil {
		return errors.Wrap(err, "error creating jsonfile logger")
	}

	stream, err := fifo.OpenFifo(context.Background(), file, syscall.O_RDONLY, 0700)
	if err != nil {
		return errors.Wrapf(err, "error opening logger fifo: %q", file)
	}

	tag, err := loggerutils.ParseLogTag(info, loggerutils.DefaultTemplate)
	if err != nil {
		return err
	}

	extra, err := info.ExtraAttributes(nil)
	if err != nil {
		return err
	}

	hostname, err := info.Hostname()
	if err != nil {
		return err
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
			HostName:         hostname,
		},
		stream: stream,
		info:   info,
	}

	hblogs.INFO("[#driver#] >>>> logContext %s(%s) is ready %p.", info.Name(), info.ID(), logCtx)
	pdriver.Lock()
	pdriver.logPairs[path.Base(file)] = logCtx
	pdriver.Unlock()
	go consumeLogStream(pdriver.logCache, logCtx)
	return nil
}

func (pdriver *PluginDriver) StopLogging(file string) error {

	hblogs.INFO("[#driver#] >>>> stoplogging stream %s.", file)
	pdriver.Lock()
	if logCtx, exists := pdriver.logPairs[path.Base(file)]; exists {
		logCtx.active = false
		delete(pdriver.logPairs, path.Base(file))
		hblogs.INFO("[#driver#] >>>> stoplogging %s(%s) successed.", logCtx.info.Name(), logCtx.info.ID())
	}
	pdriver.Unlock()
	return nil
}

func (pdriver *PluginDriver) ReadLogs(info logger.Info, logConfig logger.ReadConfig) (io.ReadCloser, error) {

	var logCtx *logContext
	pdriver.Lock()
	for _, value := range pdriver.logPairs {
		if value.info.FullID() == info.ContainerID {
			logCtx = value
			break
		}
	}
	pdriver.Unlock()

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
				if logCtx.active {
					hblogs.ERROR("[#driver#] read %s(%s) logger EOF.", logCtx.info.Name(), logCtx.info.ID())
				}
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
