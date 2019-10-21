package kstream

import (
	"github.com/pickme-go/k-stream/admin"
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/backend/memory"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/k-stream/changelog"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"github.com/pickme-go/k-stream/k-stream/offsets"
	"github.com/pickme-go/k-stream/k-stream/store"
	"github.com/pickme-go/k-stream/producer"
)

type DefaultBuilders struct {
	Producer          producer.Builder
	changelog         changelog.Builder
	Consumer          consumer.Builder
	PartitionConsumer consumer.PartitionConsumerBuilder
	Store             store.Builder
	Backend           backend.Builder
	StateStore        store.StateStoreBuilder
	OffsetManager     offsets.Manager
	KafkaAdmin        admin.KafkaAdmin
	configs           *StreamBuilderConfig
}

func (dbs *DefaultBuilders) build() {
	// default backend builder will be memory
	if dbs.configs.Store.BackendBuilder == nil {
		backendBuilderConfig := memory.NewConfig()
		backendBuilderConfig.Logger = dbs.configs.Logger
		backendBuilderConfig.MetricsReporter = dbs.configs.MetricsReporter
		dbs.Backend = memory.Builder(backendBuilderConfig)
		dbs.configs.Store.BackendBuilder = dbs.Backend
	}

	dbs.Backend = dbs.configs.Store.BackendBuilder
	dbs.configs.Store.BackendBuilder = dbs.Backend

	dbs.Store = func(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...store.Options) (store.Store, error) {
		return store.NewStore(name, keyEncoder(), valEncoder(), dbs.configs.Logger, append(options, store.WithBackendBuilder(dbs.configs.Store.BackendBuilder))...)
	}

	if dbs.Producer == nil {
		pool, err := producer.NewPool(dbs.configs.Producer.Pool.NumOfWorkers, func(options *producer.Config) (producer.Producer, error) {
			options = dbs.configs.Producer
			options.BootstrapServers = dbs.configs.BootstrapServers
			options.Logger = dbs.configs.Logger
			options.MetricsReporter = dbs.configs.MetricsReporter
			return producer.NewProducer(options)
		})
		if err != nil {
			dbs.configs.Logger.Fatal(err)
		}
		dbs.Producer = func(options *producer.Config) (producer.Producer, error) {
			return pool, nil
		}
	}

	if dbs.Consumer == nil {
		dbs.Consumer = consumer.NewBuilder()
	}
	dbs.Consumer.Config().GroupId = dbs.configs.ApplicationId
	dbs.Consumer.Config().BootstrapServers = dbs.configs.BootstrapServers
	dbs.Consumer.Config().MetricsReporter = dbs.configs.MetricsReporter
	dbs.Consumer.Config().Logger = dbs.configs.Logger

	if dbs.OffsetManager == nil {
		dbs.OffsetManager = offsets.NewManager(&offsets.Config{
			Config:           dbs.configs.Config,
			BootstrapServers: dbs.configs.BootstrapServers,
			Logger:           dbs.configs.Logger,
		})
	}

	if dbs.KafkaAdmin == nil {
		dbs.KafkaAdmin = admin.NewKafkaAdmin(&admin.KafkaAdminConfig{
			BootstrapServers: dbs.configs.BootstrapServers,
			KafkaVersion:     dbs.configs.Consumer.Version,
			Logger:           dbs.configs.Logger,
		})
	}

	if dbs.PartitionConsumer == nil {
		dbs.PartitionConsumer = consumer.NewPartitionConsumerBuilder()
	}
	dbs.PartitionConsumer.Config().BootstrapServers = dbs.configs.BootstrapServers
	dbs.PartitionConsumer.Config().MetricsReporter = dbs.configs.MetricsReporter
	dbs.PartitionConsumer.Config().Logger = dbs.configs.Logger
}
