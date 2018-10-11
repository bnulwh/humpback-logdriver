package logstash

import (
	"fmt"
	"net"
	"sync"

	hblogs "github.com/humpback/gounits/logger"
	"github.com/humpback/humpback-logdriver/driver/provider"
	"gopkg.in/fatih/pool.v3"
)

const (
	ProviderName        = "logstash"
	DefaultServer       = "127.0.0.1:5000"
	DefaultMaxPoolConns = 5
)

type LogstashProvider struct {
	sync.RWMutex
	Name      string
	config    provider.OptionConfig
	connsPool pool.Pool
}

func New(config provider.OptionConfig) (provider.Provider, error) {

	return &LogstashProvider{
		Name:      ProviderName,
		config:    config,
		connsPool: nil,
	}, nil
}

func (plogstash *LogstashProvider) String() string {

	return plogstash.Name
}

func (plogstash *LogstashProvider) Config() provider.OptionConfig {

	return plogstash.config
}

func (plogstash *LogstashProvider) Open() error {

	hblogs.INFO("[#provider#] logstash provider open...")
	host := DefaultServer
	if value, ret := plogstash.config["host"]; ret {
		if value != nil && value != "" {
			host = value.(string)
		}
	}

	dailerFactory := func() (net.Conn, error) {
		tcpAddr, err := net.ResolveTCPAddr("tcp4", host)
		if err != nil {
			return nil, err
		}
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			return nil, err
		}
		conn.SetNoDelay(true)
		return conn, nil
	}

	connsPool, err := pool.NewChannelPool(0, DefaultMaxPoolConns, dailerFactory)
	if err != nil {
		return err
	}

	plogstash.Lock()
	plogstash.connsPool = connsPool
	hblogs.INFO("[#provider#] logstash provider is ready...")
	plogstash.Unlock()
	return nil
}

func (plogstash *LogstashProvider) Close() error {

	hblogs.INFO("[#provider#] logstash provider close...")
	plogstash.Lock()
	if plogstash.connsPool != nil {
		plogstash.connsPool.Close()
		plogstash.connsPool = nil
	}
	plogstash.Unlock()
	return nil
}

func (plogstash *LogstashProvider) Write(data []byte) error {

	var (
		err  error
		conn net.Conn
	)

	plogstash.RLock()
	if plogstash.connsPool == nil {
		err = fmt.Errorf("logstash provider not yet ready")
	} else {
		conn, err = plogstash.connsPool.Get()
	}
	plogstash.RUnlock()

	if err != nil {
		return err
	}

	if pConn, ok := conn.(*pool.PoolConn); ok {
		if _, err = conn.Write(data); err != nil {
			pConn.MarkUnusable()
			pConn.Close()
			return err
		}
	}
	conn.Close()
	return nil
}
