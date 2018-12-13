package driver

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
)

type JsonTime struct {
	time.Time
}

func (t JsonTime) MarshalJSON() ([]byte, error) {

	str := fmt.Sprintf("\"%s\"", t.Format(time.RFC3339Nano))
	return []byte(str), nil
}

type LogEntry struct {
	LogID            string            `json:"logID"`
	LogCreated       JsonTime          `json:"logCreated"`
	LogTimestamp     int64             `json:"logTimestamp"`
	ContainerID      string            `json:"containerID"`
	ContainerName    string            `json:"containerName"`
	ContainerCreated JsonTime          `json:"containerCreated"`
	ImageID          string            `json:"imageID"`
	ImageName        string            `json:"imageName"`
	Command          string            `json:"command"`
	Tag              string            `json:"tag"`
	Extra            map[string]string `json:"extra"`
	HostName         string            `json:"hostName"`
	HostIP           string            `json:"hostIP"`
	Message          string            `json:"message"`
}

const (
	maxCacheSize         = 20
	maxWaitDelayDuration = time.Second * 5
)

var codecLogsPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 512<<10))
	},
}

type LogCacheHandleFunc func(entries []*LogEntry)

type LogCache struct {
	sync.Mutex
	entries  []*LogEntry
	callback LogCacheHandleFunc
	stopCh   chan struct{}
}

func NewLogCache(callback LogCacheHandleFunc) *LogCache {

	return &LogCache{
		entries:  []*LogEntry{},
		callback: callback,
		stopCh:   make(chan struct{}),
	}
}

func (cache *LogCache) Close() {

	close(cache.stopCh)
}

func (cache *LogCache) Write(logEntry *LogEntry) {

	cache.Lock()
	if len(cache.entries) == 0 {
		go cache.waitFor()
	}
	cache.entries = append(cache.entries, logEntry)
	cache.Unlock()
}

func (cache *LogCache) waitFor() {

	delayCh := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), maxWaitDelayDuration)
	go func() {
		for {
			select {
			case <-delayCh:
				{
					return
				}
			default:
				{
					time.Sleep(time.Millisecond * 100)
					cache.Lock()
					if len(cache.entries) >= maxCacheSize {
						cancel()
					}
					cache.Unlock()
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		{
			close(delayCh)
			entries := []*LogEntry{}
			cache.Lock()
			entries = append(entries, cache.entries...)
			cache.entries = cache.entries[0:0]
			cache.Unlock()
			cache.callback(entries)
		}
	case <-cache.stopCh:
		{
			close(delayCh)
		}
	}
}
