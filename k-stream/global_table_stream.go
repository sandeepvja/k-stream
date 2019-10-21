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
	"github.com/pickme-go/k-stream/admin"
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/data"
	"github.com/pickme-go/k-stream/k-stream/offsets"
	"github.com/pickme-go/k-stream/k-stream/store"
	"github.com/pickme-go/log/v2"
	"github.com/pickme-go/metrics/v2"
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

//type globalTableConfig struct {
//	table *globalKTable
//}

type tableInstance struct {
	tp                    tp
	offsetBackend         backend.Backend
	offsetKey             []byte
	backend               backend.Backend
	config                *globalKTable
	restartOnFailure      bool
	restartOnFailureCount int
	consumer              consumer.PartitionConsumer
	offsets               offsets.Manager
	synced, stopped       chan bool
	syncedCount           int64
	startOffset           int64
	endOffset             int64
	localOffset           int64
	logger                log.Logger
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
	if recoverableStore, ok := t.config.store.(store.RecoverableStore); ok {
		if err := recoverableStore.Recover(context.Background()); err != nil {
			t.logger.Fatal(fmt.Sprintf(`store recovery failed due to %s`, err))
		}
	}

	startOffset, err := t.offsets.GetOffsetOldest(t.tp.topic, t.tp.partition)
	if err != nil {
		t.logger.Fatal(
			fmt.Sprintf(`cannot read local offsetLocal due to %+v`, err))
	}

	endOffset, err := t.offsets.GetOffsetLatest(t.tp.topic, t.tp.partition)
	if err != nil {
		t.logger.Fatal(
			fmt.Sprintf(`cannot read local offsetLocal due to %+v`, err))
	}

	t.logger.Info(fmt.Sprintf(
		`brocker offsets found start:%d and end:%d`, startOffset, endOffset))

	defer t.logger.Info(
		fmt.Sprintf(`table sync done with [%d] records`, t.syncedCount))

	if t.config.options.initialOffset == GlobalTableOffsetLatest {
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
		t.logger.Info(fmt.Sprintf(`offset %d found locally`, t.localOffset))
	}

	t.startOffset = startOffset
	t.endOffset = endOffset

	go t.start()
	<-t.synced
}

func (t *tableInstance) start() error {

	events, err := t.consumer.Consume(t.tp.topic, t.tp.partition, consumer.Offset(t.startOffset))
	if err != nil {
		//return errors.New( `cannot start globalTableStream`, err)
		t.logger.Fatal(fmt.Sprintf(`cannot start globalTableStream for due to %+v`, err))
	}

	t.logger.Info(fmt.Sprintf(`table syncing...`))

	ticker := time.NewTicker(1 * time.Second)
	go func(tic *time.Ticker, topic string, partition int32) {
		for range tic.C {
			t.logger.Info(fmt.Sprintf(`sync progress - [%d]%% done (%d/%d)`, t.syncedCount*100/t.endOffset, t.syncedCount, t.endOffset))
		}
	}(ticker, t.tp.topic, t.tp.partition)

	synced := false

	for event := range events {
		switch e := event.(type) {
		case *data.Record:

			t.metrics.consumedLatency.Observe(float64(time.Since(e.Timestamp).Nanoseconds()/1e3), map[string]string{
				`topic`:     e.Topic,
				`partition`: fmt.Sprint(e.Partition),
			})

			t.syncedCount++

			if err := t.process(e, t.restartOnFailureCount, nil); err != nil {
				t.logger.Error(
					fmt.Sprintf(`cannot process message due to %+v`, err))
				continue
			}

		case *consumer.PartitionEnd:
			t.logger.Info(`partition ended`)
			if !synced {
				ticker.Stop()
				synced = true
				t.synced <- true
			}

		case *consumer.Error:
			t.logger.Error(e)
		}
	}

	t.stopped <- true

	return nil
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
		t.logger.Fatal(
			fmt.Sprintf(`cannot read local offsetLocal for %s[%d] - %+v`, t.tp.topic, t.tp.partition, err))
	}

	if len(byt) == 0 {
		t.logger.Warn(
			fmt.Sprintf(`offsetLocal dose not exist for %s[%d]`, t.tp.topic, t.tp.partition))
		if err := t.markOffset(0); err != nil {
			t.logger.Fatal(err)
		}
		return 0
	}

	offset, err := strconv.Atoi(string(byt))
	if err != nil {
		t.logger.Fatal(err)
	}

	t.logger.Info(
		fmt.Sprintf(`last synced offsetLocal for %s[%d] (%d)`, t.tp.topic, t.tp.partition, offset))

	return int64(offset)
}

func (t *tableInstance) process(r *data.Record, retry int, err error) error {

	if retry == 0 {
		return errors.WithPrevious(err, `cannot process message for []`)
	}

	if err := t.processRecord(r); err != nil {
		t.logger.Warn(
			fmt.Sprintf(`cannot process message on %s[%d] due to [%s], retring...`, r.Topic, r.Partition, err.Error()))
		t.process(r, retry-1, err)
	}

	return nil
}

func (t *tableInstance) processRecord(r *data.Record) error {
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
	t.logger.Info("\n" + b.String())
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

func newGlobalTableStream(tables map[string]*globalKTable, config *GlobalTableStreamConfig) *globalTableStream {

	offsetBackend, err := config.BackendBuilder(offsetBackendName)
	if err != nil {
		config.Logger.Fatal(err)
	}

	stream := &globalTableStream{
		restartOnFailure:      true,
		restartOnFailureCount: 5,
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
		println(len(topic.Partitions))
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
			t.offsetKey = []byte(t.tp.string())
			t.backend = tables[t.tp.topic].store.Backend()
			t.restartOnFailure = true
			t.restartOnFailureCount = 1
			t.consumer = partitionConsumer
			t.offsets = config.OffsetManager
			t.logger = config.Logger.NewLog(log.Prefixed(fmt.Sprintf(`global-table.%s-%d`, t.tp.topic, t.tp.partition)))
			t.metrics.consumedLatency = consumedLatency

			stream.tables[t.tp.string()] = t
		}
	}

	return stream
}

func (s *globalTableStream) startStreams(runWg *sync.WaitGroup) {

	s.logger.Info(`sync started...`)
	println(len(s.tables))
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
	s.logger.Info(`syncing completed`)

}

func (s *globalTableStream) printSyncInfo() {
	for _, t := range s.tables {
		t.print()
	}
}

func (s *globalTableStream) stop() {
	s.logger.Info(`streams closing...`)
	for _, t := range s.tables {

		if err := t.consumer.Close(); err != nil {
			t.logger.Error(err)
			continue
		}
		t.logger.Info(`stream closed`)
	}
	s.logger.Info(`streams closed`)
}
