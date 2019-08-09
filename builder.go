/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/changelog"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/encoding"
	"github.com/pickme-go/k-stream/graph"
	"github.com/pickme-go/k-stream/internal/node"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/k-stream/store"
	"github.com/pickme-go/k-stream/store_backend"
	"github.com/pickme-go/k-stream/task_pool"
	"github.com/pickme-go/k-stream/topic-manager"
	"github.com/pickme-go/metrics"
	saramaMetrics "github.com/rcrowley/go-metrics"
	"reflect"
	"strconv"
	"time"
)

type defaultBuilders struct {
	producer          producer.Builder
	changelog         changelog.Builder
	consumer          consumer.Builder
	partitionConsumer consumer.PartitionConsumerBuilder
	store             store.Builder
	backend           store_backend.Builder
	stateStore        store.StateStoreBuilder
}

type StreamBuilder struct {
	config                  *StreamBuilderConfig
	topicManager            *topic_manager.TopicManager   // topic manager instance
	streams                 map[string]*streamConfig      // map[topic]topology
	globalTables            map[string]*globalTableConfig // map[topic]globalTable
	storeRegistry           store.Registry
	graph                   *graph.Graph
	logger                  logger.Logger
	metricsReporter         metrics.Reporter
	defaultBuilders         defaultBuilders
	saramaClient            sarama.Client
	changelogReplicaManager *changelog.ReplicaManager
}

type changelogConfig struct {
	topic struct {
		name              string
		suffix            string
		replicationFactor int
		numOfPartitions   int
		minInSycReplicas  int
	}
	replicated bool
	enabled    bool
	buffer     struct {
		enabled       bool
		size          int
		flushInterval time.Duration
	}
}

type streamConfig struct {
	topic            string
	workerPoolConfig task_pool.PoolConfig
	changelog        *changelogConfig
	topologyBuilder  *node.TopologyBuilder
	taskPoolConfig   *task_pool.PoolConfig
}

type globalTableConfig struct {
	topic string
	store struct {
		changelog changelogConfig
	}
	table *globalKTable
}

func init() {
	saramaMetrics.UseNilMetrics = true
}

func NewStreamBuilder(config *StreamBuilderConfig) *StreamBuilder {

	logger.DefaultLogger.Info(``, `

		 _    _    _
		| |  / )  | |   _
		| | / /    \ \ | |_   ____ ____ ____ ____
		| |< <      \ \|  _) / ___) _  ) _  |    \
		| | \ \ _____) ) |__| |  ( (/ ( ( | | | | |
		|_|  \_|______/ \___)_|   \____)_||_|_|_|_|
		                         𝐆𝐨𝐥𝐚𝐧𝐠 𝐊𝐚𝐟𝐤𝐚 𝐒𝐭𝐫𝐞𝐚𝐦𝐬


		`)

	config.validate()

	// sarama client
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_0_0_0
	client, err := sarama.NewClient(config.BootstrapServers, saramaConfig)
	if err != nil {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, fmt.Sprintf(`cannot initiate builder deu to [%+v]`, err))
	}

	b := &StreamBuilder{
		config:          config,
		topicManager:    topic_manager.NewTopicManager(client),
		streams:         make(map[string]*streamConfig),
		globalTables:    make(map[string]*globalTableConfig),
		logger:          logger.DefaultLogger,
		metricsReporter: config.MetricsReporter,
		graph:           graph.NewGraph(),
		saramaClient:    client,
	}

	b.config.Consumer.BootstrapServers = config.BootstrapServers
	b.config.Consumer.GroupId = config.ApplicationId
	b.config.Consumer.Logger = logger.DefaultLogger
	b.config.Consumer.MetricsReporter = config.MetricsReporter

	b.setDefaultBuilders()

	b.storeRegistry = store.NewRegistry(&store.RegistryConfig{
		Host:              config.Store.Http.Host,
		StoreBuilder:      b.defaultBuilders.store,
		StateStoreBuilder: b.defaultBuilders.stateStore,
		Logger:            logger.DefaultLogger,
		MetricsReporter:   b.metricsReporter,
	})

	return b
}

func (b *StreamBuilder) StoreRegistry() store.Registry {
	return b.storeRegistry
}

func (b *StreamBuilder) Build(streams ...Stream) error {

	for _, stream := range streams {
		switch s := stream.(type) {
		case *kStream:
			if err := b.buildKStream(s); err != nil {
				return err
			}

		case *globalKTable:
			b.buildGlobalKTable(s)

		default:
			return errors.New(`k-stream.stream-builder`, fmt.Sprintf(`unsupported stream type [%v]:`, reflect.TypeOf(s)))
		}
	}

	b.renderGTableGraph()

	for _, stream := range b.streams {
		b.graph.RenderTopology(stream.topologyBuilder)
	}

	logger.DefaultLogger.Info(`k-stream.stream-builder`, b.graph.Build())

	// create changelog topics
	b.createChangelogTopics()

	printInfo(b)

	return nil
}

func (b *StreamBuilder) buildKStream(kStream *kStream) error {

	streams, err := kStream.Build(b)
	if err != nil {
		return err
	}

	for _, stream := range streams {
		streamConfig := new(streamConfig)

		streamConfig.topic = stream.topic(b.config.ApplicationId + `_`)
		streamConfig.taskPoolConfig = &task_pool.PoolConfig{
			NumOfWorkers:     b.config.WorkerPool.NumOfWorkers,
			WorkerBufferSize: b.config.WorkerPool.WorkerBufferSize,
			Order:            b.config.WorkerPool.Order,
		}

		streamConfig.changelog = new(changelogConfig)
		streamConfig.changelog.enabled = stream.config.changelog.enabled

		if streamConfig.changelog.enabled {
			suffix := b.config.ChangeLog.Suffix
			replicationFactor := b.config.ChangeLog.ReplicationFactor
			minInSycReplicas := b.config.ChangeLog.MinInSycReplicas
			replicated := b.config.ChangeLog.Replicated
			buffered := b.config.ChangeLog.Buffer.Enabled
			bufferSize := b.config.ChangeLog.Buffer.Size
			bufferFlush := b.config.ChangeLog.Buffer.FlushInterval

			if stream.config.changelog.suffix != `` {
				suffix = stream.config.changelog.suffix
			}

			if stream.config.changelog.replicationFactor > 0 {
				replicationFactor = stream.config.changelog.replicationFactor
			}

			if stream.config.changelog.minInSycReplicas > 0 {
				minInSycReplicas = stream.config.changelog.minInSycReplicas
			}

			if stream.config.changelog.replicated {
				replicated = true
			}

			if stream.config.changelog.buffer.enabled {
				buffered = true
			}

			if stream.config.changelog.buffer.size > 0 {
				bufferSize = stream.config.changelog.buffer.size
			}

			if stream.config.changelog.buffer.flushInterval > 0 {
				bufferFlush = stream.config.changelog.buffer.flushInterval
			}

			streamConfig.changelog.topic.name = b.config.ApplicationId + `_` + stream.topic(b.config.ApplicationId+`_`) + suffix
			streamConfig.changelog.topic.suffix = suffix
			streamConfig.changelog.topic.replicationFactor = replicationFactor
			streamConfig.changelog.topic.minInSycReplicas = minInSycReplicas
			streamConfig.changelog.replicated = replicated
			streamConfig.changelog.buffer.enabled = buffered
			streamConfig.changelog.buffer.size = bufferSize
			streamConfig.changelog.buffer.flushInterval = bufferFlush
		}

		streamConfig.topologyBuilder = stream.topology
		b.streams[stream.topic(b.config.ApplicationId+`_`)] = streamConfig

	}

	return nil
}

func (b *StreamBuilder) buildGlobalKTable(table *globalKTable) {

	table.store = b.storeRegistry.Store(table.storeName)
	tableConfig := new(globalTableConfig)

	tableConfig.table = table
	tableConfig.store.changelog.enabled = table.config.changelog.enabled

	if _, ok := table.store.(store.RecoverableStore); ok && table.config.changelog.enabled {
		suffix := b.config.Store.ChangeLog.Suffix
		replicationFactor := b.config.Store.ChangeLog.ReplicationFactor
		minInSycReplicas := b.config.Store.ChangeLog.MinInSycReplicas
		if table.config.changelog.suffix != `` {
			suffix = table.config.changelog.suffix
		}
		if table.config.changelog.replicationFactor > 0 {
			replicationFactor = table.config.changelog.replicationFactor
		}
		if table.config.changelog.minInSycReplicas > 0 {
			minInSycReplicas = table.config.changelog.minInSycReplicas
		}
		tableConfig.store.changelog.topic.name = b.config.ApplicationId + `_` + table.topic(b.config.ApplicationId+`_`) + suffix
		tableConfig.store.changelog.topic.suffix = suffix
		tableConfig.store.changelog.topic.replicationFactor = replicationFactor
		tableConfig.store.changelog.topic.minInSycReplicas = minInSycReplicas
	}

	b.globalTables[table.topic(b.config.ApplicationId+`_`)] = tableConfig
}

func (b *StreamBuilder) renderGTableGraph() {

	b.graph.GTableStreams(`kstreams`, `globalTables`, map[string]string{
		`style`:     `filled`,
		`fillcolor`: `grey`,
	})

	for topic, tableConfig := range b.globalTables {
		b.graph.Source(`globalTables`, `g_table_`+topic, map[string]string{
			`label`: fmt.Sprintf(`"topic = %s"`, topic),
		})

		b.graph.Store(`g_table_`+topic, tableConfig.table.store, map[string]string{
			`label`: fmt.Sprintf(`"Name: %s\nBackend: %s"`, tableConfig.table.store.Name(), tableConfig.table.store.Backend().Name()),
		})
	}
}

func (b *StreamBuilder) createChangelogTopics() {

	logger.DefaultLogger.Info(`k-stream.stream-builder`, `fetching changelog topics...`)

	var topics []string
	type topicConfig struct {
		topicName           string
		suffix              string
		replicationFactor   int
		numOfPartitions     int
		minInSycReplicas    int
		changelogReplicated bool
	}
	changelogTopics := make(map[string]topicConfig)

	// stream changelog configs
	for _, flowConfig := range b.streams {
		if !flowConfig.changelog.enabled {
			continue
		}

		topics = append(topics, flowConfig.topic)
		changelogTopics[flowConfig.topic] = topicConfig{
			topicName:           flowConfig.changelog.topic.name,
			minInSycReplicas:    flowConfig.changelog.topic.minInSycReplicas,
			replicationFactor:   flowConfig.changelog.topic.replicationFactor,
			changelogReplicated: flowConfig.changelog.replicated,
		}
	}

	// global table changelog configs
	for _, tableConfig := range b.globalTables {
		if tableConfig.store.changelog.topic.name == `` {
			continue
		}

		topics = append(topics, tableConfig.store.changelog.topic.name)
		changelogTopics[tableConfig.topic] = topicConfig{
			topicName:           tableConfig.store.changelog.topic.name,
			minInSycReplicas:    tableConfig.store.changelog.topic.minInSycReplicas,
			replicationFactor:   tableConfig.store.changelog.topic.replicationFactor,
			changelogReplicated: tableConfig.store.changelog.replicated,
		}
	}

	if len(topics) < 1 {
		return
	}

	topicMeta, err := b.topicManager.FetchInfo(topics)
	if err != nil {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `cannot fetch topics `, err)
		return
	}

	logger.DefaultLogger.Info(`k-stream.stream-builder`, `creating changelog topics...`)

	cleanupPolicy := `compact`
	deleteRetentionMs := `60000`
	minCleanableDirtyRatio := `0.01`
	segmentMs := `60000`

	tps := make(map[string]*sarama.TopicDetail, 0)
	for _, tp := range topicMeta.Topics {

		if tp.Err != sarama.ErrNoError {
			logger.DefaultLogger.Fatal(`k-stream.stream-builder`, fmt.Sprintf(`cannot fetch infomatins for topic [%s]`, tp.Name), tp.Err)
		}

		b.streams[tp.Name].changelog.topic.numOfPartitions = len(tp.Partitions)

		name := changelogTopics[tp.Name].topicName
		minInSyncReplicas := strconv.Itoa(changelogTopics[tp.Name].minInSycReplicas)
		tps[name] = &sarama.TopicDetail{
			ReplicationFactor: int16(changelogTopics[tp.Name].replicationFactor),
			NumPartitions:     int32(len(tp.Partitions)),
			ConfigEntries: map[string]*string{
				`cleanup.policy`:            &cleanupPolicy,
				`delete.retention.ms`:       &deleteRetentionMs,
				`min.cleanable.dirty.ratio`: &minCleanableDirtyRatio,
				`min.insync.replicas`:       &minInSyncReplicas,
				`segment.ms`:                &segmentMs,
			},
		}
	}

	if err := b.topicManager.CreateTopics(tps); err != nil {
		logger.DefaultLogger.Fatal(`k-stream.builder`, err)
	}

	// setup replica syncers for changelog topics
	// first get changelog replica Enabled topics from stream topic configs
	var replicaTps []consumer.TopicPartition

	for _, conf := range changelogTopics {
		if conf.changelogReplicated {
			for i := int32(0); i <= tps[conf.topicName].NumPartitions; i++ {
				replicaTps = append(replicaTps, consumer.TopicPartition{
					Topic:     conf.topicName,
					Partition: i,
				})
			}
		}
	}

	//setting up chnagelog replica manager

	if len(replicaTps) > 0 {
		rep, err := changelog.NewReplicaManager(changelog.ReplicaManagerConf{
			Client:   b.saramaClient,
			Backend:  b.defaultBuilders.backend,
			Consumer: b.defaultBuilders.partitionConsumer,
			Tps:      replicaTps,
		})
		if err != nil {
			logger.DefaultLogger.Fatal(
				`k-stream.stream-builder`, `changelog replica manager init failed due to`, err)
		}

		b.changelogReplicaManager = rep
	}

	b.defaultBuilders.changelog = func(id string, topic string, partition int32, opts ...changelog.Options) (changelog.Changelog, error) {
		markProducer, err := producer.NewProducer(&producer.Options{
			Logger:           logger.DefaultLogger,
			MetricsReporter:  b.metricsReporter,
			BootstrapServers: b.config.BootstrapServers,
			Id:               producer.NewProducerId(``),
		})
		if err != nil {
			logger.DefaultLogger.Fatal(`k-stream.stream-builder`, err)
		}

		if b.streams[topic].changelog.buffer.enabled {
			opts = append(opts, changelog.Buffered(b.streams[topic].changelog.buffer.size))
			opts = append(opts, changelog.FlushInterval(b.streams[topic].changelog.buffer.flushInterval))
		}

		conf := &changelog.StateChangelogConfig{
			Logger:        b.logger,
			Metrics:       b.metricsReporter,
			Topic:         topic,
			Partition:     partition,
			ChangelogId:   id,
			ApplicationId: b.config.ApplicationId,
			Producer:      markProducer,
			Consumer:      b.defaultBuilders.partitionConsumer,
		}

		if changelogTopics[topic].changelogReplicated {
			conf.ReplicaManager = b.changelogReplicaManager
		}

		return changelog.NewStateChangelog(conf, opts...)
	}

	logger.DefaultLogger.Info(`k-stream.stream-builder`, `changelog topics created`)

}

// setDefaultBuilders applies default builders for k-stream components
func (b *StreamBuilder) setDefaultBuilders() {

	topic_manager.DefaultTopicManager = func() *topic_manager.TopicManager {
		return b.topicManager
	}

	// setting up backend builders
	// default backend builder will be memory
	if b.config.Store.BackendBuilder == nil {
		backendBuilderConfig := memory.NewConfig()
		backendBuilderConfig.Logger = logger.DefaultLogger
		backendBuilderConfig.MetricsReporter = b.config.MetricsReporter
		b.defaultBuilders.backend = memory.Builder(backendBuilderConfig)
		b.config.Store.BackendBuilder = b.defaultBuilders.backend
	}

	b.defaultBuilders.backend = b.config.Store.BackendBuilder
	b.config.Store.BackendBuilder = b.defaultBuilders.backend

	b.defaultBuilders.store = func(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...store.Options) (store.Store, error) {
		return store.NewStore(name, keyEncoder(), valEncoder(), append(options, store.WithBackendBuilder(b.config.Store.BackendBuilder))...)
	}

	//dlq.DefaultBuilder = func() (dlq.DLQ, error) {
	//	return dlq.NewDLQ(&dlq.Options{
	//		BootstrapServers: b.config.DLQ.BootstrapServers,
	//		TopicFormat:      b.config.DLQ.TopicFormat,
	//		Type:             b.config.DLQ.Type,
	//		Topic:            b.config.DLQ.Topic,
	//	})
	//}

	// producer pooling
	pool := producer.NewPool(b.config.Producer.NumOfWorkers, func(options *producer.Options) (producer.Producer, error) {
		options = b.config.Producer
		options.BootstrapServers = b.config.BootstrapServers
		options.Logger = b.logger
		options.MetricsReporter = b.metricsReporter
		return producer.NewProducer(options)
	})

	b.defaultBuilders.producer = func(options *producer.Options) (producer.Producer, error) {
		return pool, nil
	}

	b.defaultBuilders.consumer = func(conf *consumer.Config) (consumer.Consumer, error) {
		config := b.config.Consumer
		config.Id = conf.Id
		config.OnRebalanced = conf.OnRebalanced
		return consumer.NewConsumer(config)
	}

	b.defaultBuilders.partitionConsumer = func(conf *consumer.PartitionConsumerConfig) (consumer.PartitionConsumer, error) {
		config := b.config.Consumer.PartitionConsumerConfig
		config.Id = conf.Id
		return consumer.NewPartitionConsumer(config)
	}
}
