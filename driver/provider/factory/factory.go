package factory

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	hblogs "github.com/humpback/gounits/logger"
	"github.com/humpback/gounits/utils"
	"github.com/humpback/humpback-logdriver/driver/provider"
	"github.com/humpback/humpback-logdriver/driver/provider/kafka"
	"github.com/humpback/humpback-logdriver/driver/provider/logstash"
	"github.com/humpback/humpback-logdriver/driver/provider/redis"
)

type ProviderFactoryFunc func(config provider.OptionConfig) (provider.Provider, error)

var Factories = map[string]ProviderFactoryFunc{
	kafka.ProviderName:    kafka.New,
	logstash.ProviderName: logstash.New,
	redis.ProviderName:    redis.New,
}

var maxSwapWaitForDuration = time.Duration(time.Second * 5)

type ProviderFactory struct {
	sync.RWMutex
	Environment string
	providers   map[string]provider.Provider
	instance    provider.Provider
	swapDelay   <-chan time.Time
}

func New(environment string) *ProviderFactory {

	return &ProviderFactory{
		Environment: environment,
		providers:   make(map[string]provider.Provider),
		instance:    nil,
	}
}

func (pfactory *ProviderFactory) Create(data map[string]interface{}) {

	providers := map[string]provider.Provider{}
	for key, value := range data {
		if dataMap, ret := value.(map[string]interface{}); ret {
			provider, err := pfactory.createProvider(dataMap)
			if err != nil {
				hblogs.ERROR("[#provider#] provider create failed, %s", err)
				continue
			}
			providers[key] = provider
			hblogs.INFO("[#provider#] provider create %s, %+v", key, provider)
		}
	}
	pfactory.swapProvider(providers)
}

func (pfactory *ProviderFactory) Close() {

	pfactory.Lock()
	if pfactory.instance != nil {
		pfactory.instance.Close()
		pfactory.instance = nil
		hblogs.INFO("[#provider#] provider closed.")
	}
	pfactory.Unlock()
}

func (pfactory *ProviderFactory) Write(data []byte) error {

	pfactory.RLock()
	defer pfactory.RUnlock()
	if pfactory.instance != nil {
		return pfactory.instance.Write(data)
	}
	return fmt.Errorf("provider invalid")
}

func (pfactory *ProviderFactory) createProvider(data map[string]interface{}) (provider.Provider, error) {

	for key, value := range data {
		if utils.Contains(key, Factories) {
			if factory := Factories[key]; factory != nil {
				config := value.(map[string]interface{})
				provider, err := factory(config)
				if err != nil {
					return nil, fmt.Errorf("factory %s create %s ", key, err)
				}
				return provider, nil
			}
		}
	}
	return nil, fmt.Errorf("data invalid")
}

func (pfactory *ProviderFactory) swapProvider(providers map[string]provider.Provider) {

	pfactory.Lock()
	defer pfactory.Unlock()
	if reflect.DeepEqual(pfactory.providers, providers) {
		hblogs.INFO("[#provider#] providers swap equal is true.")
		return
	}

	pfactory.providers = providers
	if pfactory.swapDelay == nil {
		pfactory.swapDelay = time.After(maxSwapWaitForDuration)
		go func() {
			<-pfactory.swapDelay
			pfactory.Lock()
			if provider := pfactory.providers[pfactory.Environment]; provider != nil {
				bSwap := true
				if pfactory.instance != nil {
					if reflect.DeepEqual(provider.Config(), pfactory.instance.Config()) {
						bSwap = false
					}
				}
				if bSwap {
					provider.Open()
					if pfactory.instance != nil {
						pfactory.instance.Close()
					}
					pfactory.instance = provider
					hblogs.INFO("[#provider#] providers swap to %s, %+v", provider.String(), provider.Config())
				}
			}
			pfactory.swapDelay = nil
			pfactory.Unlock()
		}()
	}
}
