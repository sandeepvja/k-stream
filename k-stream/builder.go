/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"fmt"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/admin"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/k-stream/changelog"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"github.com/pickme-go/k-stream/k-stream/graph"
	"github.com/pickme-go/k-stream/k-stream/offsets"
	"github.com/pickme-go/k-stream/k-stream/store"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/log"
	"github.com/pickme-go/metrics"
	saramaMetrics "github.com/rcrowley/go-metrics"
	"reflect"
	"strings"
)

//type defaultBuilders struct {
//	producer          producer.Builder
//	changelog         changelog.Builder
//	consumer          consumer.Builder
//	partitionConsumer consumer.PartitionConsumerBuilder
//	store             store.Builder
//	backend           backend.Builder
//	stateStore        store.StateStoreBuilder
//}

type StreamBuilder struct {
	config                  *StreamBuilderConfig
	streams                 map[string]*kStream      // map[topic]topology
	globalTables            map[string]*globalKTable // map[topic]topology
	storeRegistry           store.Registry
	graph                   *graph.Graph
	logger                  log.Logger
	metricsReporter         metrics.Reporter
	defaultBuilders         *DefaultBuilders
	kafkaAdmin              admin.KafkaAdmin
	offsetManager           offsets.Manager
	topicBuilder            *topicBuilder
	changelogTopics         map[string]*admin.Topic
	changelogReplicaManager *changelog.ReplicaManager
}

func init() {
	saramaMetrics.UseNilMetrics = true
}

func NewStreamBuilder(config *StreamBuilderConfig) *StreamBuilder {

	config.Logger.Info(`
		 _    _    _
		| |  / )  | |   _
		| | / /    \ \ | |_   ____ ____ ____ ____
		| |< <      \ \|  _) / ___) _  ) _  |    \
		| | \ \ _____) ) |__| |  ( (/ ( ( | | | | |
		|_|  \_|______/ \___)_|   \____)_||_|_|_|_|
		                         𝐆𝐨𝐥𝐚𝐧𝐠 𝐊𝐚𝐟𝐤𝐚 𝐒𝐭𝐫𝐞𝐚𝐦𝐬

		`)

	config.validate()

	config.DefaultBuilders.build()

	b := &StreamBuilder{
		config:          config,
		streams:         make(map[string]*kStream),
		globalTables:    make(map[string]*globalKTable),
		changelogTopics: make(map[string]*admin.Topic),
		logger:          config.Logger,
		metricsReporter: config.MetricsReporter,
		defaultBuilders: config.DefaultBuilders,
		graph:           graph.NewGraph(),
		kafkaAdmin:      config.DefaultBuilders.KafkaAdmin,
		offsetManager:   config.DefaultBuilders.OffsetManager,
		topicBuilder: &topicBuilder{
			topics: make(map[string]*admin.Topic),
			admin:  config.DefaultBuilders.KafkaAdmin,
			logger: config.Logger.NewLog(log.Prefixed(`topic-builder`)),
		},
	}

	b.config.Consumer.BootstrapServers = config.BootstrapServers
	b.config.Consumer.GroupId = config.ApplicationId
	b.config.Consumer.Logger = config.Logger
	b.config.Consumer.MetricsReporter = config.MetricsReporter

	b.storeRegistry = store.NewRegistry(&store.RegistryConfig{
		Host:              config.Store.Http.Host,
		StoreBuilder:      b.defaultBuilders.Store,
		StateStoreBuilder: b.defaultBuilders.StateStore,
		Logger:            config.Logger,
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
			return errors.Errorf(`unsupported stream type [%v]:`, reflect.TypeOf(s))
		}
	}

	b.renderGTableGraph()

	for _, stream := range b.streams {
		b.graph.RenderTopology(stream.topology)
	}

	b.config.Logger.Info(b.graph.Build())

	b.config.Logger.Info(fmt.Sprintf("\n%s", b.config.String(b)))

	if err := b.createChangelogTopics(); err != nil {
		return err
	}

	b.setUpChangelogs()

	return nil
}

func (b *StreamBuilder) Stream(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Option) Stream {
	if keyEncoder == nil {
		log.Fatal(`keyEncoder cannot be null`)
	}

	if valEncoder == nil {
		log.Fatal(`valEncoder cannot be null`)
	}

	opts := []Option{
		withBuilder(b),
		WithWorkerPool(b.config.WorkerPool),
		WithConfig(StreamConfigs{
			//`stream.processor.retry`: 2,
			//`stream.processor.retry.interval`: 100,
			`stream.processor.changelog.enabled`:                 b.config.ChangeLog.Enabled,
			`stream.processor.changelog.topic.name`:              fmt.Sprintf(`%s-%s-changelog`, b.config.ApplicationId, topic),
			`stream.processor.changelog.topic.minInSyncReplicas`: b.config.ChangeLog.MinInSycReplicas,
			`stream.processor.changelog.buffer.enabled`:          b.config.ChangeLog.Buffer.Enabled,
			`stream.processor.changelog.buffer.flushInterval`:    b.config.ChangeLog.Buffer.FlushInterval,
			`stream.processor.changelog.buffer.size`:             b.config.ChangeLog.Buffer.Size,
			`stream.processor.changelog.replicated`:              b.config.ChangeLog.Replicated,
			`stream.processor.changelog.topic.replicationFactor`: b.config.ChangeLog.ReplicationFactor,
			`stream.processor.dlq.enabled`:                       false,
		}),
	}
	return newKStream(func(s string) string { return topic }, keyEncoder, valEncoder, nil, append(opts, options...)...)
}

func (b *StreamBuilder) GlobalTable(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, store string, options ...globalTableOption) GlobalTable {

	//apply options
	opts := new(globalTableOptions)
	opts.initialOffset = GlobalTableOffsetDefault
	for _, o := range options {
		o(opts)
	}

	if keyEncoder == nil {
		opts.logger.Fatal(`keyEncoder cannot be null`)
	}

	if valEncoder == nil {
		opts.logger.Fatal(`valEncode cannot be null`)
	}

	s := b.Stream(topic, keyEncoder, valEncoder)
	stream := &globalKTable{
		kStream:   s.(*kStream),
		storeName: store,
		options:   opts,
		logger:    opts.logger,
	}

	return stream
}

func (b *StreamBuilder) buildKStream(kStream *kStream) error {

	streams, err := kStream.Build()
	if err != nil {
		return err
	}

	for _, stream := range streams {
		//streamConfig := new(streamConfig)

		//streamConfig.topic = stream.topic(b.config.ApplicationId + `_`)
		//streamConfig.taskPoolConfig = &task_pool.PoolConfig{
		//	NumOfWorkers:     b.config.WorkerPool.NumOfWorkers,
		//	WorkerBufferSize: b.config.WorkerPool.WorkerBufferSize,
		//	Order:            b.config.WorkerPool.Order,
		//}

		//streamConfig.changelog = new(changelogConfig)
		//streamConfig.changelog.enabled = stream.config.changelog.enabled
		//
		//if streamConfig.changelog.enabled {
		//	suffix := b.config.ChangeLog.Suffix
		//	replicationFactor := b.config.ChangeLog.ReplicationFactor
		//	minInSycReplicas := b.config.ChangeLog.MinInSycReplicas
		//	replicated := b.config.ChangeLog.Replicated
		//	buffered := b.config.ChangeLog.Buffer.Enabled
		//	bufferSize := b.config.ChangeLog.Buffer.Size
		//	bufferFlush := b.config.ChangeLog.Buffer.FlushInterval
		//
		//	if stream.config.changelog.suffix != `` {
		//		suffix = stream.config.changelog.suffix
		//	}
		//
		//	if stream.config.changelog.replicationFactor > 0 {
		//		replicationFactor = stream.config.changelog.replicationFactor
		//	}
		//
		//	if stream.config.changelog.minInSycReplicas > 0 {
		//		minInSycReplicas = stream.config.changelog.minInSycReplicas
		//	}
		//
		//	if stream.config.changelog.replicated {
		//		replicated = true
		//	}
		//
		//	if stream.config.changelog.buffer.enabled {
		//		buffered = true
		//	}
		//
		//	if stream.config.changelog.buffer.size > 0 {
		//		bufferSize = stream.config.changelog.buffer.size
		//	}
		//
		//	if stream.config.changelog.buffer.flushInterval > 0 {
		//		bufferFlush = stream.config.changelog.buffer.flushInterval
		//	}
		//
		//	streamConfig.changelog.topic.name = b.config.ApplicationId + `_` + stream.topic(b.config.ApplicationId+`_`) + suffix
		//	streamConfig.changelog.topic.suffix = suffix
		//	streamConfig.changelog.topic.replicationFactor = replicationFactor
		//	streamConfig.changelog.topic.minInSycReplicas = minInSycReplicas
		//	streamConfig.changelog.replicated = replicated
		//	streamConfig.changelog.buffer.enabled = buffered
		//	streamConfig.changelog.buffer.size = bufferSize
		//	streamConfig.changelog.buffer.flushInterval = bufferFlush
		//}

		b.streams[stream.topic(b.config.ApplicationId+`_`)] = stream

	}

	return nil
}

func (b *StreamBuilder) buildGlobalKTable(table *globalKTable) {

	table.store = b.storeRegistry.Store(table.storeName)
	//tableConfig := new(globalKTable)
	//tableConfig.table = table
	/*tableConfig.store.changelog.enabled = table.config.changelog.enabled

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
	}*/

	b.globalTables[table.topic(b.config.ApplicationId+`_`)] = table
}

func (b *StreamBuilder) renderGTableGraph() {

	b.graph.GTableStreams(`kstreams`, `globalTables`, map[string]string{
		`style`:     `filled`,
		`fillcolor`: `grey`,
	}, nil)

	for topic, tableConfig := range b.globalTables {
		topicU := strings.ReplaceAll(topic, `-`, `_`)
		b.graph.Source(`globalTables`, `g_table_`+topicU, map[string]string{
			`label`: fmt.Sprintf(`"topic = %s"`, topic),
		}, nil)

		b.graph.Store(`g_table_`+topicU, tableConfig.store, map[string]string{
			`label`: fmt.Sprintf(`"Name: %s\nBackend: %s"`, tableConfig.store.Name(), tableConfig.store.Backend().Name()),
		}, nil)
	}
}

func (b *StreamBuilder) createChangelogTopics() error {
	b.config.Logger.Info(`fetching changelog topics...`)

	var topics []string
	// stream changelog configs
	for _, stream := range b.streams {
		if !stream.config.changelog.enabled {
			continue
		}
		topics = append(topics, stream.topic(``))
	}

	// global table changelog configs
	/*for _, tableConfig := range b.globalTables {
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
	}*/

	// fetch topic info
	info, err := b.kafkaAdmin.FetchInfo(topics)
	if err != nil {
		return err
	}

	for tp, config := range info {

		if config.Error != nil {
			return err
		}

		b.streams[tp].config.changelog.topic.NumPartitions = config.NumPartitions
		b.streams[tp].config.changelog.topic.ConfigEntries[`cleanup.policy`] = `compact`
		b.changelogTopics[tp] = b.streams[tp].config.changelog.topic
		b.topicBuilder.apply(b.streams[tp].config.changelog.topic)
	}

	b.topicBuilder.build()
	b.config.Logger.Info(`changelog topics created`)
	return nil
}

func (b *StreamBuilder) setUpChangelogs() {
	// setup replica syncers for changelog topics
	// first get changelog replica Enabled topics from stream topic configs
	var replicaTps []consumer.TopicPartition

	for _, stream := range b.streams {
		if !stream.config.changelog.enabled || !stream.config.changelog.replicated {
			continue
		}

		b.logger.Error(b.changelogTopics)

		pts := b.changelogTopics[stream.topic(``)].NumPartitions
		for i := int32(0); i <= pts; i++ {
			replicaTps = append(replicaTps, consumer.TopicPartition{
				Topic:     stream.config.changelog.topic.Name,
				Partition: i,
			})
		}
	}

	//setting up chnagelog replica manager
	if len(replicaTps) > 0 {
		rep, err := changelog.NewReplicaManager(&changelog.ReplicaManagerConf{
			OffsetManager: b.offsetManager,
			Backend:       b.defaultBuilders.Backend,
			Consumer:      b.defaultBuilders.PartitionConsumer,
			Tps:           replicaTps,
			Logger:        b.logger,
		})
		if err != nil {
			b.config.Logger.Fatal(
				`changelog replica manager init failed due to`, err)
		}

		b.changelogReplicaManager = rep
	}

	b.defaultBuilders.changelog = func(id string, topic string, partition int32, opts ...changelog.Options) (changelog.Changelog, error) {
		markProducer, err := producer.NewProducer(&producer.Config{
			Logger:           b.config.Logger,
			MetricsReporter:  b.metricsReporter,
			BootstrapServers: b.config.BootstrapServers,
			Id:               `test`,
		})
		if err != nil {
			b.config.Logger.Fatal(err)
		}

		if b.streams[topic].config.changelog.buffer.enabled {
			opts = append(opts, changelog.Buffered(b.streams[topic].config.changelog.buffer.size))
			opts = append(opts, changelog.FlushInterval(b.streams[topic].config.changelog.buffer.flushInterval))
		}

		conf := &changelog.StateChangelogConfig{
			Logger:        b.logger,
			Metrics:       b.metricsReporter,
			Topic:         topic,
			Partition:     partition,
			ChangelogId:   id,
			ApplicationId: b.config.ApplicationId,
			Producer:      markProducer,
			Consumer:      b.defaultBuilders.PartitionConsumer,
		}

		if b.streams[topic].config.changelog.replicated {
			conf.ReplicaManager = b.changelogReplicaManager
		}

		return changelog.NewStateChangelog(conf, opts...)
	}
}
