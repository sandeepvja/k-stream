/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/olekukonko/tablewriter"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/store"
	"github.com/pickme-go/k-stream/store_backend"
	"github.com/pickme-go/k-stream/topic-manager"
	"github.com/pickme-go/metrics"
	"strconv"
	"sync"
	"time"
)

var offsetBackendName = `__k-table-offsets`

type tp struct {
	topic     string
	partition int32
}

func (tp *tp) string() string {
	return fmt.Sprintf(`%s_%d`, tp.topic, tp.partition)
}

type tableInstance struct {
	tp                    tp
	offsetBackend         store_backend.Backend
	offsetKey             []byte
	backend               store_backend.Backend
	config                *globalTableConfig
	restartOnFailure      bool
	restartOnFailureCount int
	consumer              consumer.PartitionConsumer
	synced, stopped       chan bool
	syncedCount           int64
	startOffset           int64
	endOffset             int64
	localOffset           int64
	logger                logger.Logger
	metrics               struct {
		consumedLatency metrics.Observer
	}
}

func (t *tableInstance) init() {

	t.synced = make(chan bool, 1)
	t.stopped = make(chan bool, 1)
	// by default broker start offset is offset beginning
	t.localOffset = t.offsetLocal()

	// check storeName is recoverable or not
	if recoverableStore, ok := t.config.table.store.(store.RecoverableStore); ok {
		if err := recoverableStore.Recover(context.Background()); err != nil {
			logger.DefaultLogger.Fatal(`k-stream.globalTableStream`,
				fmt.Sprintf(`store recovery error on %s[%d] - %+v`, t.tp.topic, t.tp.partition, err))
		}
	}

	startOffset, err := t.consumer.GetOldestOffset(t.tp.topic, t.tp.partition)
	if err != nil {
		logger.DefaultLogger.Fatal(`k-stream.globalTableStream`,
			fmt.Sprintf(`cannot read local offsetLocal on %s[%d] - %+v`, t.tp.topic, t.tp.partition, err))
	}

	endOffset, err := t.consumer.GetLatestOffset(t.tp.topic, t.tp.partition)
	if err != nil {
		logger.DefaultLogger.Fatal(`k-stream.globalTableStream`,
			fmt.Sprintf(`cannot read local offsetLocal on %s[%d] - %+v`, t.tp.topic, t.tp.partition, err))
	}

	logger.DefaultLogger.Info(`k-stream.globalTableStream`, fmt.Sprintf(
		`brocker offsets for %s[%d] is %d and %d`, t.tp.topic, t.tp.partition, startOffset, endOffset))

	defer logger.DefaultLogger.Info(`k-stream.globalTableStream`,
		fmt.Sprintf(`table %s[%d] sync done with [%d] records`, t.tp.topic, t.tp.partition, t.syncedCount))

	if t.config.table.options.initialOffset == GlobalTableOffsetLatest {
		t.startOffset = -1
		go t.start()
		<-t.synced
		return
	}

	// now get local offset from offset storeName
	// if local offset is > 0 and > beginOffset then reset beginOffset = local offset
	// non persistence backends dose not have a local offset
	if t.localOffset >= startOffset && t.localOffset < endOffset && t.backend.Persistent() {
		startOffset = t.localOffset
		logger.DefaultLogger.Info(`k-stream.globalTableStream`, fmt.Sprintf(
			`local offset %d valid for for %s[%d]`, t.localOffset, t.tp.topic, t.tp.partition))
	}

	t.startOffset = startOffset
	t.endOffset = endOffset

	go t.start()
	<-t.synced
}

func (t *tableInstance) start() error {

	events, err := t.consumer.Consume(t.tp.topic, t.tp.partition, t.startOffset)
	if err != nil {
		//return errors.New(`k-stream.globalTableStream`, `cannot start globalTableStream`, err)
		logger.DefaultLogger.Fatal(`k-stream.globalTableStream`,
			fmt.Sprintf(`cannot start globalTableStream for %s[%d] due to %+v`, t.tp.topic, t.tp.partition, err))
	}

	logger.DefaultLogger.Info(`k-stream.globalTableStream`,
		fmt.Sprintf(`table %s[%d] syncing...`, t.tp.topic, t.tp.partition))

	ticker := time.NewTicker(1 * time.Second)
	go func(tic *time.Ticker, topic string, partition int32) {
		for range tic.C {
			logger.DefaultLogger.Info(`k-stream.globalTableStream`,
				fmt.Sprintf(
					`table %s[%d] sync progress - [%d]%% done (%d/%d)`,
					topic, partition, t.syncedCount*100/t.endOffset, t.syncedCount, t.endOffset))
		}
	}(ticker, t.tp.topic, t.tp.partition)

	synced := false

	for event := range events {
		switch e := event.(type) {
		case *consumer.Record:

			t.metrics.consumedLatency.Observe(float64(time.Since(e.Timestamp).Nanoseconds()/1e3), map[string]string{
				`topic`:     e.Topic,
				`partition`: fmt.Sprint(e.Partition),
			})

			t.syncedCount++

			if err := t.process(e, t.restartOnFailureCount, nil); err != nil {
				logger.DefaultLogger.Error(`k-stream.globalTableStream`,
					fmt.Sprintf(`cannot process message due to %+v`, err))
				continue
			}

		case *consumer.PartitionEnd:
			logger.DefaultLogger.Info(`k-stream.globalTableStream`,
				fmt.Sprintf(`partition ended for %s`, e.String()))

			if !synced {
				ticker.Stop()
				synced = true
				t.synced <- true
			}

		case *consumer.Error:
			logger.DefaultLogger.Error(`k-stream.globalTableStream`, e)
		}
	}

	t.stopped <- true

	return nil
}

func (t *tableInstance) stop() {

}

func (t *tableInstance) markOffset(offset int64) error {
	val := []byte(strconv.Itoa(int(offset)))
	return t.offsetBackend.Set(t.offsetKey, val, 0)
}

func (t *tableInstance) offsetLocal() int64 {

	if !t.backend.Persistent() {
		return 0
	}

	key := []byte(fmt.Sprintf(`%s_%d`, t.tp.topic, t.tp.partition))
	byt, err := t.offsetBackend.Get(key)
	if err != nil {
		logger.DefaultLogger.Fatal(`k-stream.globalTableStream`,
			fmt.Sprintf(`cannot read local offsetLocal for %s[%d] - %+v`, t.tp.topic, t.tp.partition, err))
	}

	if len(byt) == 0 {
		logger.DefaultLogger.Warn(`k-stream.globalTableStream`,
			fmt.Sprintf(`offsetLocal dose not exist for %s[%d]`, t.tp.topic, t.tp.partition))
		if err := t.markOffset(0); err != nil {
			logger.DefaultLogger.Fatal(`k-stream.globalTableStream`, err)
		}
		return 0
	}

	offset, err := strconv.Atoi(string(byt))
	if err != nil {
		logger.DefaultLogger.Fatal(`k-stream.globalTableStream`, err)
	}

	logger.DefaultLogger.Info(`k-stream.globalTableStream`,
		fmt.Sprintf(`last synced offsetLocal for %s[%d] (%d)`, t.tp.topic, t.tp.partition, offset))

	return int64(offset)
}

func (t *tableInstance) process(r *consumer.Record, retry int, err error) error {

	if retry == 0 {
		return errors.WithPrevious(err, `k-stream.globalTableStream`, `cannot process message for []`)
	}

	if err := t.processRecord(r); err != nil {
		logger.DefaultLogger.Warn(`k-stream.globalTableStream`,
			fmt.Sprintf(`cannot process message on %s[%d] due to [%s], retring...`, r.Topic, r.Partition, err.Error()))
		t.process(r, retry-1, err)
	}

	return nil
}

func (t *tableInstance) processRecord(r *consumer.Record) error {
	// log compaction (tombstone)
	if r.Value == nil {
		if err := t.backend.Delete(r.Key); err != nil {
			return err
		}
	} else {
		if err := t.backend.Set(r.Key, r.Value, 0); err != nil {
			return err
		}
	}

	// if backend is non persistent no need to store the offset locally
	if !t.backend.Persistent() {
		return nil
	}

	return t.markOffset(r.Offset)
}

func (t *tableInstance) print() {
	b := new(bytes.Buffer)
	table := tablewriter.NewWriter(b)
	table.SetHeader([]string{`GlobalTableConfig`, fmt.Sprint(t.tp.topic)})
	tableData := [][]string{
		{`partition`, fmt.Sprint(t.tp.partition)},
		{`offset.local`, fmt.Sprint(t.localOffset)},
		{`offset.broker.start`, fmt.Sprint(t.startOffset)},
		{`offset.broker.end`, fmt.Sprint(t.endOffset)},
		{`synced`, fmt.Sprint(t.syncedCount)},
	}

	for _, v := range tableData {
		table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT})
		table.Append(v)
	}
	table.Render()
	logger.DefaultLogger.Info(`k-stream.globalTableStream`, "\n"+b.String())
}

// Global Table Stream is a special type of stream which run in background in async manner and will
// create a partition consumer for each global table upstream topic+partition. Once the stream started it will sync all
// the tables up to broker latest offset
type globalTableStream struct {
	tables                map[string]*tableInstance
	syncing               chan bool
	restartOnFailure      bool
	restartOnFailureCount int
	logger                logger.Logger
}

type GlobalTableStreamConfig struct {
	ConsumerBuilder consumer.PartitionConsumerBuilder
	BackendBuilder  store_backend.Builder
	Metrics         metrics.Reporter
	Logger          logger.Logger
}

func newGlobalTableStream(tables map[string]*globalTableConfig, config *GlobalTableStreamConfig) *globalTableStream {

	tpManager := topic_manager.DefaultTopicManager()

	offsetBackend, err := config.BackendBuilder(offsetBackendName)
	if err != nil {
		logger.DefaultLogger.Fatal(`k-stream.globalTableStream`, err)
	}

	stream := &globalTableStream{
		restartOnFailure:      true,
		restartOnFailureCount: 5,
		tables:                make(map[string]*tableInstance),
		logger:                config.Logger,
	}

	var topics []string
	for t := range tables {
		topics = append(topics, t)
	}

	// get partition information's for topics
	info, err := tpManager.FetchInfo(topics)
	if err != nil {
		logger.DefaultLogger.Fatal(`k-stream.globalTableStream`,
			fmt.Sprintf(`cannot fetch topic info - %+v`, err))
	}

	consumedLatency := config.Metrics.Observer(`k_stream_global_table_stream_consumed_latency_microseconds`, []string{`topic`, `partition`})

	for _, topic := range info.Topics {
		if topic.Err != sarama.ErrNoError {
			logger.DefaultLogger.Fatal(`k-stream.globalTableStream`,
				fmt.Sprintf(`cannot get topic info for %s due to %s`, topic.Name, topic.Err.Error()))
		}

		for i := int32(len(topic.Partitions)) - 1; i >= 0; i-- {
			c := consumer.NewSimpleConsumerConfig()
			c.Id = fmt.Sprintf(`global_table_consumer_%s_%d`, topic.Name, i)
			partitionConsumer, err := config.ConsumerBuilder(c)
			if err != nil {
				logger.DefaultLogger.Fatal(`k-stream.globalTableStream`, err)
			}

			t := new(tableInstance)
			t.tp.topic = topic.Name
			t.tp.partition = i
			t.config = tables[t.tp.topic]
			t.offsetBackend = offsetBackend
			t.offsetKey = []byte(t.tp.string())
			t.backend = tables[t.tp.topic].table.store.Backend()
			t.restartOnFailure = true
			t.restartOnFailureCount = 1
			t.consumer = partitionConsumer
			t.metrics.consumedLatency = consumedLatency

			stream.tables[t.tp.string()] = t
		}
	}

	return stream
}

func (s *globalTableStream) startStreams(runWg *sync.WaitGroup) {

	logger.DefaultLogger.Info(`k-stream.globalTableStream`, `global table sync started...`)

	// create a waitgroup with the lenght of num of tables for table syncing
	syncWg := new(sync.WaitGroup)
	syncWg.Add(len(s.tables))

	go func() {
		// run waitgroup is for running table go routine
		//runWg := new(sync.WaitGroup)
		for _, table := range s.tables {
			runWg.Add(1)
			go func(t *tableInstance, syncWg *sync.WaitGroup) {
				t.init()
				syncWg.Done()
				// once the table stopped mark un waitgroup as done
				<-t.stopped
				runWg.Done()
			}(table, syncWg)
		}

		// wait until all the running table stopped and mark parent waitgroup as done
		//runWg.Wait()
	}()

	// method should be blocked until table syncing is done
	syncWg.Wait()
	s.printSyncInfo()
	logger.DefaultLogger.Info(`k-stream.globalTableStream`, `global table syncing completed`)

}

func (s *globalTableStream) printSyncInfo() {
	for _, t := range s.tables {
		t.print()
	}
}

func (s *globalTableStream) stop() {
	logger.DefaultLogger.Info(`k-stream.globalTableStream`, `global stream closing...`)
	for _, t := range s.tables {
		if err := t.consumer.Close(); err != nil {
			logger.DefaultLogger.Error(`k-stream.globalTableStream`, err)
			continue
		}
		logger.DefaultLogger.Info(`k-stream.globalTableStream`,
			fmt.Sprintf(`global stream for %s closed`, t.tp.string()))
	}
	logger.DefaultLogger.Info(`k-stream.globalTableStream`, `global stream closed`)
}
