package redis

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	redigo "github.com/gomodule/redigo/redis"
	hblogs "github.com/humpback/gounits/logger"
	"github.com/humpback/humpback-logdriver/driver/provider"
)

const (
	ProviderName       = "redis"
	DefaultListName    = "humpback-logs"
	DefaultServer      = "127.0.0.1:6379"
	DefaultDatabase    = 0
	DefaultReadTime    = time.Duration(time.Second * 5)
	DefaultWriteTime   = time.Duration(time.Second * 5)
	DefaultConnectTime = time.Duration(time.Second * 10)
)

type RedisProvider struct {
	sync.RWMutex
	Name     string
	listName string
	config   provider.OptionConfig
	pool     *redigo.Pool
}

func New(config provider.OptionConfig) (provider.Provider, error) {

	listName := DefaultListName
	if value, ret := config["list"]; ret {
		if value != nil && value != "" {
			listName = value.(string)
		}
	}

	return &RedisProvider{
		Name:     ProviderName,
		listName: listName,
		config:   config,
		pool: &redigo.Pool{
			MaxActive:   5,
			MaxIdle:     5,
			Wait:        true,
			IdleTimeout: 120 * time.Second,
		},
	}, nil
}

func (predis *RedisProvider) String() string {

	return predis.Name
}

func (predis *RedisProvider) Config() provider.OptionConfig {

	return predis.config
}

func (predis *RedisProvider) Open() error {

	hblogs.INFO("[#provider#] redis provider open...")
	server := DefaultServer
	if value, ret := predis.config["host"]; ret {
		if value != nil && value != "" {
			server = value.(string)
		}
	}

	database := DefaultDatabase
	if value, ret := predis.config["database"]; ret {
		if value != nil {
			if reflect.TypeOf(value).Kind() == reflect.String {
				if db, err := strconv.Atoi(value.(string)); err == nil {
					database = db
				}
			} else if reflect.TypeOf(value).Kind() == reflect.Float64 {
				database = (int)(value.(float64))
			}
		}
	}

	var password string
	if value, ret := predis.config["password"]; ret {
		if value != nil {
			password = value.(string)
		}
	}

	predis.pool.Dial = func() (redigo.Conn, error) {
		return redigo.Dial("tcp", server,
			redigo.DialDatabase(database),
			redigo.DialPassword(password),
			redigo.DialConnectTimeout(DefaultConnectTime),
			redigo.DialReadTimeout(DefaultReadTime),
			redigo.DialWriteTimeout(DefaultWriteTime),
		)
	}

	go func() {
		if predis.pool != nil {
			conn := predis.pool.Get()
			defer conn.Close()
			if err := conn.Err(); err != nil {
				hblogs.ERROR("[#provider#] redis provider open error, %s", err)
			} else {
				hblogs.INFO("[#provider#] redis provider is ready...")
			}
		}
	}()
	return nil
}

func (predis *RedisProvider) Close() error {

	hblogs.INFO("[#provider#] redis provider close...")
	predis.Lock()
	defer predis.Unlock()
	if predis.pool != nil {
		return predis.pool.Close()
	}
	return nil
}

func (predis *RedisProvider) Write(data []byte) error {

	var (
		err  error
		conn redigo.Conn
	)

	predis.RLock()
	if predis.pool == nil {
		err = fmt.Errorf("redis provider not yet ready")
	} else {
		conn = predis.pool.Get()
	}
	predis.RUnlock()

	if err != nil {
		return err
	}

	defer conn.Close()
	_, err = conn.Do("RPUSH", predis.listName, data)
	return err
}
