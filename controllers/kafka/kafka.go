package kafka

import (
	"errors"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/caarlos0/env"
	"time"
)

type Kafka struct {
	Topic []string `env:"TOPIC" envSeparator:","`
	Brokers []string `env:"BROKERS" envSeparator:","`
	ConsumerGroup string `env:"CONSUMER_GROUP"`
}

func (k *Kafka) NewConsumer() (*cluster.Consumer, error){
	conf := cluster.NewConfig()
	conf.Consumer.Return.Errors = true
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Group.Return.Notifications = true
	conf.Consumer.Offsets.CommitInterval = time.Second

	consumer, err := cluster.NewConsumer(k.Brokers, k.ConsumerGroup, k.Topic, conf)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func InitKafka() (*Kafka, error){
	conf := Kafka{}
	if err := env.Parse(&conf); err != nil {
		return nil, errors.New("cloud not load kafka environment variables")
	}

	return &conf, nil
}