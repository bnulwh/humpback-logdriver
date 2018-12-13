package ngmq

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/humpback/gounits/httpx"
	hblogs "github.com/humpback/gounits/logger"
	"github.com/humpback/humpback-logdriver/driver/provider"
)

const (
	ProviderName     = "ngmq"
	DefaultMQName    = "humpback-logs"
	DefaultKeepalive = time.Duration(120)
	DefaultTimeout   = time.Duration(30)
)

type MessageContent struct {
	MessageName string `json:"MessageName"`
	Password    string `json:"Password"`
	MessageBody string `json:"MessageBody"`
	ContentType string `json:"ContentType"`
	CallbackURI string `json:"CallbackUri"`
	InvokeType  string `json:"InvokeType"`
}

type NgMQProvider struct {
	sync.RWMutex
	Name        string
	APIURL      string
	MQName      string
	Password    string
	ContentType string
	CallbackURI string
	Invoke      string
	client      *httpx.HttpClient
	config      provider.OptionConfig
}

func New(config provider.OptionConfig) (provider.Provider, error) {

	ngMQProvider := &NgMQProvider{
		Name:   ProviderName,
		MQName: DefaultMQName,
		config: config,
	}

	if value, ret := config["apiurl"]; ret {
		if value != nil && value != "" {
			ngMQProvider.APIURL = value.(string)
		}
	}

	if value, ret := config["mqname"]; ret {
		if value != nil && value != "" {
			ngMQProvider.MQName = value.(string)
		}
	}

	if value, ret := config["password"]; ret {
		if value != nil && value != "" {
			ngMQProvider.Password = value.(string)
		}
	}

	if value, ret := config["contenttype"]; ret {
		if value != nil && value != "" {
			ngMQProvider.ContentType = value.(string)
		}
	}

	if value, ret := config["callbackuri"]; ret {
		if value != nil && value != "" {
			ngMQProvider.CallbackURI = value.(string)
		}
	}

	if value, ret := config["invoke"]; ret {
		if value != nil && value != "" {
			ngMQProvider.Invoke = value.(string)
		}
	}
	return ngMQProvider, nil
}

func (ngMQ *NgMQProvider) String() string {

	return ngMQ.Name
}

func (ngMQ *NgMQProvider) Config() provider.OptionConfig {

	return ngMQ.config
}

func (ngMQ *NgMQProvider) Open() error {

	hblogs.INFO("[#provider#] ngMQ provider open...")
	keepalive := DefaultKeepalive
	if value, ret := ngMQ.config["keepalive"]; ret {
		if value != nil && value != "" {
			inKeepAlive, err := time.ParseDuration(value.(string))
			if err == nil {
				keepalive = inKeepAlive
			}
		}
	}

	timeout := DefaultTimeout
	if value, ret := ngMQ.config["timeout"]; ret {
		if value != nil && value != "" {
			inTimeout, err := time.ParseDuration(value.(string))
			if err == nil {
				timeout = inTimeout
			}
		}
	}

	ngMQ.client = httpx.NewClient().
		SetTransport(&http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: keepalive,
			}).DialContext,
			DisableKeepAlives:     false,
			MaxIdleConns:          20,
			MaxIdleConnsPerHost:   20,
			IdleConnTimeout:       keepalive + time.Duration(10),
			TLSHandshakeTimeout:   http.DefaultTransport.(*http.Transport).TLSHandshakeTimeout,
			ExpectContinueTimeout: http.DefaultTransport.(*http.Transport).ExpectContinueTimeout,
		})
	hblogs.INFO("[#provider#] ngMQ provider is ready...")
	return nil
}

func (ngMQ *NgMQProvider) Close() error {

	hblogs.INFO("[#provider#] ngMQ provider close...")
	if ngMQ.client != nil {
		ngMQ.client.Close()
	}
	return nil
}

func (ngMQ *NgMQProvider) Write(data []byte) error {

	msgContent := MessageContent{
		MessageName: ngMQ.MQName,
		Password:    ngMQ.Password,
		MessageBody: string(data),
		ContentType: ngMQ.ContentType,
		CallbackURI: ngMQ.CallbackURI,
		InvokeType:  ngMQ.Invoke,
	}

	resp, err := ngMQ.client.PostJSON(context.Background(), ngMQ.APIURL, nil, msgContent, nil)
	if err != nil {
		return err
	}

	defer resp.Close()
	statusCode := resp.StatusCode()
	if statusCode >= http.StatusBadRequest {
		return fmt.Errorf("request send fail %d", statusCode)
	}
	return nil
}
