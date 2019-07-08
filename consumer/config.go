package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/log"
	"github.com/pickme-go/metrics"
	"time"
)

type PartitionConsumerConfig struct {
	Id               string
	ClientId         string //client.id
	BootstrapServers []string
	*sarama.Config
	MetricsReporter metrics.Reporter
	Logger          logger.Logger
}

func NewConsumerConfig() *Config {
	c := new(Config)
	c.setDefaults()
	return c
}

func NewPartitionConsumerConfig() *PartitionConsumerConfig {
	c := new(PartitionConsumerConfig)
	c.setDefaults()
	return c
}

func (c *PartitionConsumerConfig) validate() error {
	if c.Id == `` {
		return errors.New(`k-stream.partitionConsumer.config`,
			`k-stream.PartitionConsumerConfig: Consumer.Id] cannot be empty`)
	}

	if len(c.BootstrapServers) < 1 {
		return errors.New(`k-stream.partitionConsumer.config`,
			`k-stream.consumer.Config: Consumer.BootstrapServers cannot be empty`)
	}

	if err := c.Config.Validate(); err != nil {
		return err
	}

	return nil
}

func (c *PartitionConsumerConfig) setDefaults() {
	c.Config = sarama.NewConfig()
	c.Config.Version = sarama.V2_0_0_0
	c.ChannelBufferSize = 1000
	c.Consumer.Return.Errors = true
	c.MetricsReporter = metrics.NoopReporter()
	c.Config = sarama.NewConfig()
}

type Config struct {
	Id               string
	GroupId          string
	Host             string
	BootstrapServers []string
	MetricsReporter  metrics.Reporter
	Logger           logger.Logger
	*cluster.Config
}

func (c *Config) setDefaults() {
	c.Config = cluster.NewConfig()
	c.Config.Version = sarama.V2_0_0_0
	c.Group.Mode = cluster.ConsumerModePartitions
	c.Group.Heartbeat.Interval = 100 * time.Millisecond
	c.Group.PartitionStrategy = cluster.StrategyRoundRobin
	c.Consumer.Return.Errors = true
	c.ChannelBufferSize = 1000
	c.Group.Return.Notifications = true
	c.MetricsReporter = metrics.NoopReporter()
	c.Logger = log.NewPrefixedNoopLogger()
}

func (c *Config) validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}

	if c.GroupId == `` {
		return errors.New(`k-stream.consumer.Config`,
			`k-stream.consumer.Config: Consumer.GroupId cannot be empty`)
	}

	if c.Group.Mode != cluster.ConsumerModePartitions {
		return errors.New(`k-stream.consumer.Config`,
			`k-stream.consumer.Config: Consumer.Group.Mode should be ConsumerModePartitions`)
	}

	if len(c.BootstrapServers) < 1 {
		return errors.New(`k-stream.consumer.Config`,
			`k-stream.consumer.Config: Consumer.BootstrapServers cannot be empty`)
	}

	if c.Group.PartitionStrategy != cluster.StrategyRoundRobin {
		return errors.New(`k-stream.consumer.Config`,
			`k-stream.consumer.Config: Consumer.Group.PartitionStrategy should be StrategyRoundRobin`)
	}

	if !c.Group.Return.Notifications {
		return errors.New(`k-stream.consumer.Config`,
			`k-stream.consumer.Config: Consumer.Group.Return.Notifications should be true`)
	}

	return nil
}
