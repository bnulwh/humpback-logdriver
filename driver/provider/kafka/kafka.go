package kafka

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	hblogs "github.com/humpback/gounits/logger"
	"github.com/humpback/gounits/rand"
	"github.com/humpback/humpback-logdriver/driver/provider"
)

const (
	ProviderName     = "kafka"
	DefaultServer    = "127.0.0.1:9092"
	DefaultTopic     = "humpback-logs"
	DefaultPartition = 1
)

type KafkaProvider struct {
	sync.RWMutex
	Name      string
	Topic     string
	Partition int32
	config    provider.OptionConfig
	producer  sarama.AsyncProducer
	stopCh    chan struct{}
}

func New(config provider.OptionConfig) (provider.Provider, error) {

	topic := DefaultTopic
	if value, ret := config["topic"]; ret {
		if value != nil && value != "" {
			topic = value.(string)
		}
	}

	partition := DefaultPartition
	if value, ret := config["partition"]; ret {
		if value != nil {
			if reflect.TypeOf(value).Kind() == reflect.String {
				if n, err := strconv.Atoi(value.(string)); err == nil {
					partition = n
				}
			} else if reflect.TypeOf(value).Kind() == reflect.Float64 {
				partition = (int)(value.(float64))
			}
		}
	}

	return &KafkaProvider{
		Name:      ProviderName,
		Topic:     topic,
		Partition: (int32)(partition),
		config:    config,
		producer:  nil,
		stopCh:    make(chan struct{}),
	}, nil
}

func (pkafka *KafkaProvider) String() string {

	return pkafka.Name
}

func (pkafka *KafkaProvider) Config() provider.OptionConfig {

	return pkafka.config
}

func (pkafka *KafkaProvider) Open() error {

	hblogs.INFO("[#provider#] kafka provider open...")
	addr := []string{DefaultServer}
	if value, ret := pkafka.config["host"]; ret {
		if value != nil && value != "" {
			addr = strings.Split(value.(string), ",")
		}
	}

	go func() {
		config := sarama.NewConfig()
		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Timeout = time.Second * 5
		retryDelay := time.Millisecond * 100
		for {
			ticker := time.NewTicker(retryDelay)
			select {
			case <-ticker.C:
				{
					ticker.Stop()
					producer, err := sarama.NewAsyncProducer(addr, config)
					if err != nil {
						hblogs.ERROR("[#provider#] kafka provider open error, %s", err)
						retryDelay = time.Second * 5
					} else {
						pkafka.Lock()
						pkafka.producer = producer
						pkafka.Unlock()
						hblogs.INFO("[#provider#] kafka provider is ready...")
						return
					}
				}
			case <-pkafka.stopCh:
				{
					ticker.Stop()
					hblogs.INFO("[#provider#] kafka retry exit...")
					return
				}
			}
		}
	}()
	return nil
}

func (pkafka *KafkaProvider) Close() error {

	hblogs.INFO("[#provider#] kafka provider close...")
	pkafka.Lock()
	if pkafka.producer != nil {
		pkafka.producer.AsyncClose()
		pkafka.producer = nil
	}
	pkafka.Unlock()
	close(pkafka.stopCh)
	return nil
}

func (pkafka *KafkaProvider) Write(data []byte) error {

	pkafka.RLock()
	defer pkafka.RUnlock()
	if pkafka.producer == nil {
		return fmt.Errorf("kafka provider not yet ready")
	}

	msg := &sarama.ProducerMessage{
		Topic:     pkafka.Topic,
		Partition: pkafka.Partition,
		Key:       sarama.StringEncoder(rand.UUID(true)),
		Value:     sarama.ByteEncoder(data),
	}

	var err error
	pkafka.producer.Input() <- msg
	select {
	case pErr := <-pkafka.producer.Errors():
		{
			err = pErr.Err
		}
	case <-pkafka.producer.Successes():
		{
		}
	}
	return err
}
