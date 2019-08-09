package changelog

import (
	"context"
	"fmt"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/metrics"
	"time"
)

type stateChangelog struct {
	applicationId   string
	id              string
	topic           string
	partition       int32
	recovering      bool
	stopRecovery    chan bool
	recoveryStopped chan bool
	buffer          *Buffer
	options         *options
	changelogSuffix string
	metrics         *changeLogMetrics
	logger          logger.Logger
	consumer        consumer.PartitionConsumerBuilder
	replicaManager  *ReplicaManager
}

type changeLogMetrics struct {
	marksLatency    metrics.Observer
	recoveryRate    metrics.Counter
	recoveryLatency metrics.Observer
	deleteLatency   metrics.Observer
}

type StateChangelogConfig struct {
	ChangelogId    string
	ApplicationId  string
	Producer       producer.Producer
	Topic          string
	Partition      int32
	Logger         logger.Logger
	ReplicaManager *ReplicaManager
	Metrics        metrics.Reporter
	Consumer       consumer.PartitionConsumerBuilder
}

func NewStateChangelog(config *StateChangelogConfig, opts ...Options) (Changelog, error) {

	options := new(options)
	if err := options.apply(fmt.Sprintf(`%s_%d`, config.ChangelogId, config.Partition), opts...); err != nil {
		return nil, err
	}

	var metricsLabels = []string{`topic`, `partition`}

	buffer := NewBuffer(config.Producer, options.bufferSize, options.flushInterval)
	buffer.metrics.flushLatency = config.Metrics.Observer(`k_stream_changelog_buffer_flush_latency_microseconds`, nil)

	return &stateChangelog{
		topic:           config.Topic,
		partition:       config.Partition,
		id:              config.ChangelogId,
		applicationId:   config.ApplicationId,
		options:         options,
		stopRecovery:    make(chan bool),
		recoveryStopped: make(chan bool, 1),
		buffer:          buffer,
		changelogSuffix: `_changelog`,
		replicaManager:  config.ReplicaManager,
		logger:          config.Logger,
		consumer:        config.Consumer,
		metrics: &changeLogMetrics{
			marksLatency:    config.Metrics.Observer(`k_stream_changelog_mark_latency_microseconds`, metricsLabels),
			deleteLatency:   config.Metrics.Observer(`k_stream_changelog_delete_latency_microseconds`, metricsLabels),
			recoveryRate:    config.Metrics.Counter(`k_stream_changelog_recovery_rate`, metricsLabels),
			recoveryLatency: config.Metrics.Observer(`k_stream_changelog_recovery_latency_microseconds`, metricsLabels),
		},
	}, nil
}

func (c *stateChangelog) ReadAll(ctx context.Context) ([]*consumer.Record, error) {
	begin := time.Now()
	c.recovering = true
	config := consumer.NewSimpleConsumerConfig()
	config.Id = fmt.Sprintf(`changelog_state_consumer_%s_%d`, c.topic, c.partition)
	simpleConsumer, err := c.consumer(config)
	if err != nil {
		return nil, err
	}

	ticker := time.NewTicker(1 * time.Second)

	c.logger.InfoContext(ctx, `k-stream.StateChangelog`,
		fmt.Sprintf(`changelog recovery started for %s - %s[%d]`, c.id, c.topic, c.partition))

	// get replica local cache
	tp := consumer.TopicPartition{
		Topic:     c.changelogTopic(),
		Partition: c.partition,
	}

	messages := make([]*consumer.Record, 0)

	// default offset position will be beginning offset of the partition
	lastSyncedOffset := int64(consumer.Earliest)

	// if cache available for the partition validate and recover
	if c.replicaManager != nil {
		cache, err := c.replicaManager.GetCache(tp)
		if err != nil {
			return nil, err
		}

		cachedOffset, err := cache.LastSynced()
		if err != nil {
			return nil, err
		}

		valid, _, err := c.replicaManager.offsetManager.OffsetValid(tp, cachedOffset)
		if err != nil {
			return nil, err
		}

		if valid {
			lastSyncedOffset = cachedOffset
			cachedRecords := cache.ReadAll()
			cachedRecordsLen := len(cachedRecords)
			messages = make([]*consumer.Record, 0, cachedRecordsLen)
			messages = append(messages, cachedRecords...)
			c.logger.Info(`k-stream.StateChangelog`, fmt.Sprintf(`[%d] messages recovered from local cache for [%s]`, cachedRecordsLen, tp))
		}

	}

	// manually assign partition to the consumer
	c.logger.Info(`k-stream.StateChangelog`, fmt.Sprintf(`recovery consumer started for [%s] from [%d]`, tp, lastSyncedOffset))
	events, err := simpleConsumer.Consume(c.changelogTopic(), c.partition, lastSyncedOffset)
	if err != nil {
		return nil, errors.WithPrevious(err, `k-stream.StateChangelog`, fmt.Sprintf(`cannot read partition %s - %s[%d]`,
			c.id, c.changelogTopic(), c.partition))
	}

	c.logger.InfoContext(ctx, `k-stream.StateChangelog`,
		fmt.Sprintf(`recovering from changelog %s - %s[%d]`, c.id, c.changelogTopic(), c.partition))

	go func() {
		for range ticker.C {
			c.logger.InfoContext(ctx, `k-stream.StateChangelog`,
				fmt.Sprintf(`%s - %s[%d] [%d] messages done`, c.id, c.changelogTopic(), c.partition, len(messages)))
		}
	}()

	go func() {
		for range c.stopRecovery {
			if err := simpleConsumer.Close(); err != nil {
				c.logger.Error(`k-stream.StateChangelog`,
					fmt.Sprintf(`closing consumer for %s[%d] failed due to %+v`, c.changelogTopic(), c.partition, err))
			}
		}
	}()

	go func() {
		for range simpleConsumer.Errors() {
			c.logger.ErrorContext(ctx, `k-stream.StateChangelog`,
				fmt.Sprintf(`recovery failed for %s - %s[%d], err : %+v`, c.id, c.topic, c.partition, err))
		}
	}()

MainLoop:
	for ev := range events {
		switch e := ev.(type) {
		case *consumer.Record:
			e.Topic = c.topic
			messages = append(messages, e)

			c.metrics.recoveryRate.Count(1, map[string]string{
				`topic`:     c.changelogTopic(),
				`partition`: fmt.Sprint(c.partition),
			})

		case *consumer.PartitionEnd:
			c.logger.InfoContext(ctx, `k-stream.StateChangelog`,
				fmt.Sprintf(`end of changelog partition for %s - %s[%d]`, c.id, c.topic, c.partition))

			if err := simpleConsumer.Close(); err != nil {
				c.logger.Error(`k-stream.StateChangelog`,
					fmt.Sprintf(`closing consumer for %s[%d] failed due to %+v`, c.changelogTopic(), c.partition, err))
			}

			break MainLoop
		}
	}

	ticker.Stop()

	if len(messages) > 0 {
		deDuplicated := deDuplicate(messages)
		c.logger.Info(`k-stream.StateChangelog`,
			fmt.Sprintf(`%d duplicates were removed while recovering `, len(messages)-len(deDuplicated)))
		messages = deDuplicated

	}

	timeTaken := time.Since(begin).Nanoseconds()

	c.metrics.recoveryLatency.Observe(float64(timeTaken/1e3), map[string]string{
		`topic`:     c.changelogTopic(),
		`partition`: fmt.Sprint(c.partition),
	})

	c.logger.InfoContext(ctx, `k-stream.StateChangelog`,
		fmt.Sprintf(`%d messages recovered in %d miliseconds for %s - %s[%d] `,
			len(messages), timeTaken/1000000, c.id, c.topic, c.partition))

	c.recovering = false
	c.recoveryStopped <- true

	c.logger.InfoContext(ctx, `k-stream.StateChangelog`,
		fmt.Sprintf(`changelog recovery done for %s[%d] `, c.topic, c.partition))

	return messages, nil
}

func (c *stateChangelog) Put(ctx context.Context, record *consumer.Record) error {
	c.buffer.Store(c.prepareRecord(record))
	return nil
}

func (c *stateChangelog) PutAll(ctx context.Context, records []*consumer.Record) error {
	panic(`implement me`)
}

// changelog topics will be compaction enabled and keys with null  records will be deleted
func (c *stateChangelog) Delete(ctx context.Context, record *consumer.Record) error {
	c.buffer.Delete(c.prepareRecord(record))
	return nil

}

func (c *stateChangelog) DeleteAll(ctx context.Context, records []*consumer.Record) error {
	panic(`implement me`)
}

func (c *stateChangelog) Close() {
	if c.recovering {
		c.stopRecovery <- true
		<-c.recoveryStopped
	}
	c.buffer.Close()
	c.logger.Info(`k-stream.StateChangelog`,
		fmt.Sprintf(`state changelog %s_%d stopped `, c.topic, c.partition))

}

func (c *stateChangelog) prepareRecord(r *consumer.Record) *consumer.Record {
	return &consumer.Record{
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: r.Timestamp,
		Topic:     c.changelogTopic(),
		Partition: r.Partition,
	}
}

func (c *stateChangelog) changelogTopic() string {
	return c.applicationId + `_` + c.topic + c.changelogSuffix
}

func deDuplicate(duplicates []*consumer.Record) []*consumer.Record {

	deDuplicated := make(map[string]*consumer.Record)
	for _, record := range duplicates {

		// ignore deleted but still not removed from the changelog topic messages
		if len(record.Value) < 1 {
			delete(deDuplicated, string(record.Key))
			continue
		}

		deDuplicated[string(record.Key)] = record
	}

	records := make([]*consumer.Record, 0, len(deDuplicated))
	for _, record := range deDuplicated {
		records = append(records, record)
	}

	return records
}
