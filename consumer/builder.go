package consumer

import (
	"github.com/pickme-go/log"
	"github.com/pickme-go/metrics"
)

type BuilderOption func(config *Config)

func BuilderWithId(id string) BuilderOption {
	return func(config *Config) {
		config.Id = id
	}
}

func BuilderWithGroupId(id string) BuilderOption {
	return func(config *Config) {
		config.GroupId = id
	}
}

func BuilderWithMetricsReporter(reporter metrics.Reporter) BuilderOption {
	return func(config *Config) {
		config.MetricsReporter = reporter
	}
}

func BuilderWithLogger(logger log.Logger) BuilderOption {
	return func(config *Config) {
		config.Logger = logger
	}
}

type Builder interface {
	Config() *Config
	Build(options ...BuilderOption) (Consumer, error)
}

type builder struct {
	config *Config
}

func NewBuilder() Builder {
	return &builder{
		config: NewConsumerConfig(),
	}
}

func (b *builder) Config() *Config {
	return b.config
}

//func (b *builder) Configure(c *Config) Builder {
//	return &builder{
//		config: c,
//	}
//}

func (b *builder) Build(options ...BuilderOption) (Consumer, error) {
	conf := *b.config
	for _, option := range options {
		option(&conf)
	}
	return NewConsumer(&conf)
}

type PartitionConsumerBuilder interface {
	Config() *Config
	Build(options ...BuilderOption) (PartitionConsumer, error)
}

type partitionConsumerBuilder struct {
	config *Config
}

func NewPartitionConsumerBuilder() PartitionConsumerBuilder {
	return &partitionConsumerBuilder{
		config: NewConsumerConfig(),
	}
}

func (b *partitionConsumerBuilder) Config() *Config {
	return &*b.config
}

func (b *partitionConsumerBuilder) Configure(c *Config) PartitionConsumerBuilder {
	return &partitionConsumerBuilder{
		config: c,
	}
}

func (b *partitionConsumerBuilder) Build(options ...BuilderOption) (PartitionConsumer, error) {
	conf := *b.config
	for _, option := range options {
		option(&conf)
	}
	return NewPartitionConsumer(&conf)
}
