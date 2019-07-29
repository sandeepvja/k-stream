package consumer

type Builder interface {
	Configure(*Config)
	Config() *Config
	Build() (Consumer, error)
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

func (b *builder) Configure(c *Config) {
	b.config = c
}

func (b *builder) Build() (Consumer, error) {
	return NewConsumer(b.config)
}

type PartitionConsumerBuilder interface {
	Configure(*Config)
	Config() *Config
	Build() (PartitionConsumer, error)
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
	return b.config
}

func (b *partitionConsumerBuilder) Configure(c *Config) {
	b.config = c
}

func (b *partitionConsumerBuilder) Build() (PartitionConsumer, error) {
	return NewPartitionConsumer(b.config)
}
