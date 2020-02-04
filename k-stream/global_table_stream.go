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
	"github.com/pickme-go/k-stream/admin"
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/data"
	"github.com/pickme-go/k-stream/k-stream/offsets"
	"github.com/pickme-go/k-stream/k-stream/store"
	"github.com/pickme-go/log/v2"
	"github.com/pickme-go/metrics/v2"

	"sync"
)

var offsetBackendName = `__k-table-offsets`

type StoreWriter func(r *data.Record, store store.Store) error

type tp struct {
	topic     string
	partition int32
}

func (tp *tp) String() string {
	return fmt.Sprintf(`%s_%d`, tp.topic, tp.partition)
}

// Global Table Stream is a special type of stream which run in background in async manner and will
// create a partition consumer for each global table upstream topic+partition. Once the stream started it will sync all
// the tables up to broker latest offset
type globalTableStream struct {
	tables                map[string]*tableInstance
	restartOnFailure      bool
	restartOnFailureCount int
	logger                log.Logger
}

type GlobalTableStreamConfig struct {
	ConsumerBuilder consumer.PartitionConsumerBuilder
	BackendBuilder  backend.Builder
	OffsetManager   offsets.Manager
	KafkaAdmin      admin.KafkaAdmin
	Metrics         metrics.Reporter
	Logger          log.Logger
}

// newGlobalTableStream starts a
func newGlobalTableStream(tables map[string]*globalKTable, config *GlobalTableStreamConfig) *globalTableStream {
	offsetBackend, err := config.BackendBuilder(offsetBackendName)
	if err != nil {
		config.Logger.Fatal(err)
	}

	stream := &globalTableStream{
		tables:                make(map[string]*tableInstance),
		logger:                config.Logger.NewLog(log.Prefixed(`global-tables`)),
	}

	var topics []string
	for t := range tables {
		topics = append(topics, t)
	}

	// get partition information's for topics
	info, err := config.KafkaAdmin.FetchInfo(topics)
	if err != nil {
		config.Logger.Fatal(
			fmt.Sprintf(`cannot fetch topic info - %+v`, err))
	}

	consumedLatency := config.Metrics.Observer(metrics.MetricConf{
		Path:   `k_stream_global_table_stream_consumed_latency_microseconds`,
		Labels: []string{`topic`, `partition`},
	})

	for _, topic := range info {

		if topic.Error != nil && topic.Error != sarama.ErrNoError {
			config.Logger.Fatal(
				fmt.Sprintf(`cannot get topic info for %s due to %s`, topic.Name, topic.Error.Error()))
		}
		for i := int32(len(topic.Partitions)) - 1; i >= 0; i-- {
			partitionConsumer, err := config.ConsumerBuilder.Build(
				consumer.BuilderWithId(fmt.Sprintf(`global_table_consumer_%s_%d`, topic.Name, i)),
				consumer.BuilderWithLogger(config.Logger.NewLog(log.Prefixed(fmt.Sprintf(`global-table.%s-%d`, topic.Name, i)))),
			)
			if err != nil {
				config.Logger.Fatal(err)
			}

			t := new(tableInstance)
			t.tp.topic = topic.Name
			t.tp.partition = i
			t.config = tables[t.tp.topic]
			t.offsetBackend = offsetBackend
			t.offsetKey = []byte(t.tp.String())
			t.store = tables[t.tp.topic].store
			t.storeWriter = tables[t.tp.topic].options.backendWriter
			t.restartOnFailure = true
			t.restartOnFailureCount = 1
			t.consumer = partitionConsumer
			t.offsets = config.OffsetManager
			t.logger = config.Logger.NewLog(log.Prefixed(fmt.Sprintf(`global-table.%s-%d`, t.tp.topic, t.tp.partition)))
			t.metrics.consumedLatency = consumedLatency

			stream.tables[t.tp.String()] = t
		}
	}

	return stream
}

// StartStreams starts all the tables
func (s *globalTableStream) StartStreams(runWg *sync.WaitGroup) {
	s.logger.Info(`sync started...`)
	defer s.logger.Info(`syncing completed`)

	// create a waitgroup with the num of tables for table syncing
	syncWg := new(sync.WaitGroup)
	syncWg.Add(len(s.tables))
	go func() {

		// run waitgroup is for running table go routine
		for _, table := range s.tables {
			runWg.Add(1)
			go func(t *tableInstance, syncWg *sync.WaitGroup) {
				t.Init()
				syncWg.Done()

				// once the table stopped mark run waitgroup as done
				<-t.stopped
				runWg.Done()
			}(table, syncWg)
		}
	}()

	// method should be blocked until table syncing is done
	syncWg.Wait()
	s.printSyncInfo()
}

func (s *globalTableStream) printSyncInfo() {
	for _, t := range s.tables {
		t.print()
	}
}

func (s *globalTableStream) stop() {
	s.logger.Info(`streams closing...`)
	defer s.logger.Info(`streams closed`)
	for _, t := range s.tables {
		if err := t.consumer.Close(); err != nil {
			t.logger.Error(err)
			continue
		}
		t.logger.Info(`stream closed`)
	}
}
