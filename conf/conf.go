package conf

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/humpback/gounits/convert"
	"github.com/humpback/gounits/logger"
	"github.com/humpback/humpback-logdriver/driver/stay"
	yaml "gopkg.in/yaml.v2"
)

var configuration *Configuration

//NodeConfig is exported
type NodeConfig struct {
	Hosts         string
	Environment   string
	RetryInterval time.Duration
	Heartbeat     time.Duration
	TTL           time.Duration
}

//Configuration is exported
type Configuration struct {
	Environment string `yaml:"environment" json:"environment"`
	HostIP      string `yaml:"hostip" json:"hostip"`
	Discovery   struct {
		Hosts         map[string]interface{} `yaml:"hosts" json:"hosts"`
		RetryInterval time.Duration          `yaml:"retryinterval" json:"retryinterval"`
		Heartbeat     time.Duration          `yaml:"heartbeat" json:"heartbeat"`
		TTL           time.Duration          `yaml:"ttl" json:"ttl"`
	} `yaml:"discovery" json:"discovery"`
	Providers interface{} `yaml:"providers" json:"providers"`
	Blocks    struct {
		Enable        bool          `yaml:"enable" json:"enable"`
		MaxSize       int64         `yaml:"maxsize" json:"maxsize"`
		MaxCount      int           `yaml:"maxcount" json:"maxcount"`
		RetryInterval time.Duration `yaml:"retryinterval" json:"retryinterval"`
	} `yaml:"blocks" json:"blocks"`
	Logs struct {
		FileName string `yaml:"filename" json:"filename"`
		Level    string `yaml:"level" json:"level"`
		MaxSize  int64  `yaml:"maxsize" json:"maxsize"`
	} `yaml:"logs" json:"logs"`
}

func New(fname string) error {

	fd, err := os.OpenFile(fname, os.O_RDWR, 0777)
	if err != nil {
		return err
	}

	defer fd.Close()
	data, err := ioutil.ReadAll(fd)
	if err != nil {
		return err
	}

	c := Configuration{}
	if err = yaml.Unmarshal([]byte(data), &c); err != nil {
		return err
	}

	pluginEnv := os.Getenv("PLUGIN_ENV")
	if pluginEnv != "" {
		c.Environment = pluginEnv
	}

	pluginLogsLevel := os.Getenv("PLUGIN_LOGS_LEVEL")
	if pluginLogsLevel != "" {
		c.Logs.Level = pluginLogsLevel
	}

	pluginBlockEnable := os.Getenv("PLUGIN_BLOCK_ENABLE")
	if pluginBlockEnable != "" {
		if value, err := strconv.ParseBool(pluginBlockEnable); err == nil {
			c.Blocks.Enable = value
		}
	}

	pluginBlockMaxSize := os.Getenv("PLUGIN_BLOCK_MAXSIZE")
	if pluginBlockMaxSize != "" {
		if value, err := strconv.Atoi(pluginBlockMaxSize); err == nil {
			c.Blocks.MaxSize = (int64)(value)
		}
	}

	pluginBlockMaxCount := os.Getenv("PLUGIN_BLOCK_MAXCOUNT")
	if pluginBlockMaxCount != "" {
		if value, err := strconv.Atoi(pluginBlockMaxCount); err == nil {
			c.Blocks.MaxCount = value
		}
	}

	pluginBlockRetryInterval := os.Getenv("PLUGIN_BLOCK_RETRYINTERVAL")
	if pluginBlockRetryInterval != "" {
		if value, err := time.ParseDuration(pluginBlockRetryInterval); err == nil {
			c.Blocks.RetryInterval = value
		}
	}

	pluginHostIP := os.Getenv("PLUGIN_HOST_IP")
	if pluginHostIP != "" {
		c.HostIP = pluginHostIP
	}
	configuration = &c
	return nil
}

func Environment() string {

	if configuration != nil {
		if configuration.Environment != "" {
			return configuration.Environment
		}
	}
	return "dev"
}

func HostIP() string {

	if configuration != nil {
		return configuration.HostIP
	}
	return ""
}

func NodeConfigArgs() *NodeConfig {

	if configuration != nil {
		environment := Environment()
		for env, value := range configuration.Discovery.Hosts {
			if strings.EqualFold(env, environment) {
				return &NodeConfig{
					Hosts:         value.(string),
					Environment:   environment,
					RetryInterval: configuration.Discovery.RetryInterval,
					Heartbeat:     configuration.Discovery.Heartbeat,
					TTL:           configuration.Discovery.Heartbeat,
				}
			}
		}
	}
	return nil
}

func StayBlocksConfig() *stay.StayBlocksConfig {

	if configuration != nil {
		return &stay.StayBlocksConfig{
			Enable:        configuration.Blocks.Enable,
			MaxSize:       configuration.Blocks.MaxSize,
			MaxCount:      configuration.Blocks.MaxCount,
			RetryInterval: configuration.Blocks.RetryInterval,
		}
	}
	return nil
}

func Providers() map[string]interface{} {

	providers := map[string]interface{}{}
	if configuration != nil {
		providers = convert.CleanupMapValue(configuration.Providers).(map[string]interface{})
	}
	return providers
}

func LoggerArgs() *logger.Args {

	if configuration != nil {
		return &logger.Args{
			FileName: configuration.Logs.FileName,
			Level:    configuration.Logs.Level,
			MaxSize:  configuration.Logs.MaxSize,
		}
	}
	return nil
}
