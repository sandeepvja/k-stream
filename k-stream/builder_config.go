/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"bytes"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/olekukonko/tablewriter"
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/k-stream/task_pool"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/log/v2"
	"github.com/pickme-go/metrics/v2"
	"strings"
	"time"
)

type StreamBuilderConfig struct {
	ApplicationId    string
	AsyncProcessing  bool
	BootstrapServers []string // kafka Brokers
	WorkerPool       *task_pool.PoolConfig
	Store            struct {
		BackendBuilder backend.Builder
		ChangeLog      struct {
			MinInSycReplicas  int // min number of insync replications in other nodes
			ReplicationFactor int
			Suffix            string
			Buffered          bool
			BufferedSize      int
		}
		Http struct {
			Host string
		}
	}
	DLQ struct {
		Enabled          bool
		BootstrapServers []string
		TopicFormat      string
		//Type             dlq.DqlType // G, T
		Topic string // if global
	}
	Host      string
	ChangeLog struct {
		Enabled           bool
		Replicated        bool
		MinInSycReplicas  int // min number of insync replications in other nodes
		ReplicationFactor int
		Suffix            string
		Buffer            struct {
			Enabled       bool
			Size          int
			FlushInterval time.Duration
		}
	}
	Consumer      *consumer.Config
	ConsumerCount int
	*sarama.Config
	Producer        *producer.Config
	MetricsReporter metrics.Reporter
	Logger          log.Logger
	DefaultBuilders *DefaultBuilders
}

var logger = log.NewLog(log.WithColors(true), log.WithFilePath(true), log.WithLevel(log.TRACE), log.Prefixed(`k-stream`)).Log()

func NewStreamBuilderConfig() *StreamBuilderConfig {
	config := &StreamBuilderConfig{}
	config.Producer = producer.NewConfig()
	config.Consumer = consumer.NewConsumerConfig()
	config.Config = sarama.NewConfig()

	config.ConsumerCount = 1

	config.ChangeLog.Suffix = `_changelog`
	config.ChangeLog.Replicated = false
	config.ChangeLog.MinInSycReplicas = 2
	config.ChangeLog.ReplicationFactor = 3
	config.ChangeLog.Buffer.Enabled = true
	config.ChangeLog.Buffer.Size = 100
	config.ChangeLog.Buffer.FlushInterval = 100 * time.Millisecond

	config.Producer.Pool.NumOfWorkers = 1
	//config.Producer.Producer.Retry.Backoff = time.Millisecond * 30
	//config.Producer.Retry = 5
	//config.Producer.Idempotent = true
	config.Producer.RequiredAcks = producer.WaitForAll
	//config.Producer.BatchNumMessages = 1
	//config.Producer.QueueBufferingMax = 1

	//set default task execution order
	config.WorkerPool = &task_pool.PoolConfig{
		Order:            task_pool.OrderByKey,
		NumOfWorkers:     100,
		WorkerBufferSize: 10,
	}

	// default metrics reporter
	config.MetricsReporter = metrics.NoopReporter()
	config.Logger = logger
	config.DefaultBuilders = &DefaultBuilders{configs: config}

	return config
}

func (c *StreamBuilderConfig) validate() {

	c.Logger = c.Logger.NewLog(log.Prefixed(`k-stream`))

	if c.ApplicationId == `` {
		c.Logger.Fatal(`[ApplicationId] cannot be empty`)
	}

	//if c.Host == `` {
	//	c.logger.Fatal( `[Host] cannot be empty`)
	//}

	if len(c.BootstrapServers) < 1 {
		c.Logger.Fatal(`[BootstrapServers] cannot be empty`)
	}

	if c.ChangeLog.MinInSycReplicas < 1 {
		c.Logger.Fatal(`[ChangeLog.MinInSycReplicas] cannot be zero`)
	}

	if c.ChangeLog.ReplicationFactor < 1 {
		c.Logger.Fatal(`[ChangeLog.ReplicationFactor] cannot be zero`)
	}

	if c.ChangeLog.Buffer.FlushInterval < 1 {
		c.Logger.Fatal(`[ChangeLog.Buffer.FlushInterval] cannot be zero`)
	}

	if c.ChangeLog.Buffer.Size < 1 {
		c.Logger.Fatal(`[ChangeLog.Buffer.Size] cannot be zero`)
	}

	// producer configurations
	//if c.Producer.QueueBufferingMax < 1 {
	//	c.logger.Fatal( `[Producer.QueueBufferingMax] should be greater than zero`)
	//}
	//
	//if c.Producer.BatchNumMessages < 1 {
	//	c.logger.Fatal( `[Producer.BatchNumMessages] should be greater than zero`)
	//}
	//
	//if c.Producer.Retry < 1 {
	//	c.logger.Fatal( `[Producer.Retry] should be greater than zero`)
	//}
	//
	//if c.Producer.RetryBackOff < 1*time.Millisecond {
	//	c.logger.Fatal( `[Producer.RetryBackOff] should be equal or greater than 1ms`)
	//}

	//DLQ configurations
	//if c.DLQ.Enabled {
	//	if len(c.DLQ.BootstrapServers) < 1 {
	//		c.logger.Fatal( `[DLQ.BootstrapServers] cannot be empty`)
	//	}
	//
	//	if c.DLQ.Type == dlq.DqlGlobal && c.DLQ.TopicFormat == `` {
	//		c.logger.Fatal(
	//			`[DLQ.BootstrapServers] global topic format cannot be empty when topic type is [dlq.DqlGlobal]`)
	//	}
	//}

	//Worker Pool options
	if c.WorkerPool.Order > 2 || c.WorkerPool.Order < 0 {
		c.Logger.Fatal(
			`Invalid WorkerPool Order`)
	}

	if c.WorkerPool.WorkerBufferSize < 1 {
		c.Logger.Fatal(
			`WorkerPool WorkerBufferSize should be greater than 0`)
	}

	if c.WorkerPool.NumOfWorkers < 1 {
		c.Logger.Fatal(
			`WorkerPool NumOfWorkers should be greater than 0`)
	}
}

func (c *StreamBuilderConfig) String(b *StreamBuilder) string {
	data := [][]string{
		{"kstream.ApplicationId", fmt.Sprint(c.ApplicationId)},
		{"kstream.BootstrapServers", strings.Join(c.BootstrapServers, `, `)},
		{"kstream.ConsumerCount", fmt.Sprint(c.ConsumerCount)},
		{"kstream.Consumer.AutoCommitInterval", fmt.Sprint(c.Consumer.Config.Consumer.Offsets.AutoCommit.Interval)},
		{"kstream.Consumer.OffsetBegin", fmt.Sprint(c.Consumer.Consumer.Offsets.Initial)},
		{"kstream.Consumer.AsyncProcessing", fmt.Sprint(c.AsyncProcessing)},
		//{"kstream.Consumer.AutoCommitEnable", fmt.Sprint(c.Consumer.AutoCommitEnable)},
		{"kstream.Consumer.ClientId", fmt.Sprint(c.ApplicationId)},
		{"kstream.Consumer.EventChannelSize", fmt.Sprint(c.Consumer.ChannelBufferSize)},
		{"kstream.Consumer.groupId", fmt.Sprint(c.ApplicationId)},
		{"kstream.Consumer.BootstrapServers", fmt.Sprint(c.BootstrapServers)},
		{"kstream.Consumer.FetchErrorBackOffMs", fmt.Sprint(c.Consumer.Consumer.Fetch.Default)},
		//{"kstream.Consumer.FetchMinBytes", fmt.Sprint(c.Consumer.FetchMinBytes)},
		//{"kstream.Consumer.FetchWaitMaxMs", fmt.Sprint(c.Consumer.FetchWaitMaxMs)},
		//{"kstream.Consumer.HeartbeatIntervalMs", fmt.Sprint(c.Consumer.HeartbeatIntervalMs)},
		//{"kstream.Consumer.SessionTimeoutMs", fmt.Sprint(c.Consumer.SessionTimeoutMs)},
		//{"kstream.Consumer.MetadataMaxAgeMs", fmt.Sprint(c.Consumer.MetadataMaxAgeMs)},
		{``, ``},
		{"kstream.WorkerPool.NumOfWorkers (Per Partition)", fmt.Sprint(c.WorkerPool.NumOfWorkers)},
		{"kstream.WorkerPool.WorkerBufferSize (Per Partition)", fmt.Sprint(c.WorkerPool.WorkerBufferSize)},
		{"kstream.WorkerPool.Order", fmt.Sprint(c.WorkerPool.Order)},
		{``, ``},
		{"kstream.Producer.NumOfWorkers", fmt.Sprint(c.Producer.Pool.NumOfWorkers)},
		//{"kstream.Producer.Idempotent", fmt.Sprint(c.Producer.Idempotent)},
		//{"kstream.Producer.BatchNumMessages", fmt.Sprint(c.Producer.BatchNumMessages)},
		//{"kstream.Producer.QueueBufferingMax", fmt.Sprint(c.Producer.QueueBufferingMax)},
		{"kstream.Producer.RequiredAcks", fmt.Sprint(c.Producer.RequiredAcks)},
		//{"kstream.Producer.Retry", fmt.Sprint(c.Producer.Retry)},
		//{"kstream.Producer.RetryMax", fmt.Sprint(c.Producer.RetryBackOff)},
		{``, ``},
		{"kstream.ChangeLog.Buffered.Enabled", fmt.Sprint(c.ChangeLog.Buffer.Enabled)},
		{"kstream.ChangeLog.BufferedSize", fmt.Sprint(c.ChangeLog.Buffer.Size)},
		{"kstream.ChangeLog.BufferedFlush", c.ChangeLog.Buffer.FlushInterval.String()},
		{"kstream.ChangeLog.MinInSycReplicas", fmt.Sprint(c.ChangeLog.MinInSycReplicas)},
		{"kstream.ChangeLog.ReplicationFactor", fmt.Sprint(c.ChangeLog.ReplicationFactor)},
		{"kstream.ChangeLog.Replicated", fmt.Sprint(c.ChangeLog.Replicated)},
		{"kstream.ChangeLog.Suffix", fmt.Sprint(c.ChangeLog.Suffix)},
		{``, ``},
		{"kstream.Store.Http.Host", fmt.Sprint(c.Store.Http.Host)},
	}

	data = append(data, []string{``})
	data = append(data, []string{`Stream configs`, ``})
	for topic, stream := range b.streams {

		b := new(bytes.Buffer)
		flowTable := tablewriter.NewWriter(b)
		flowData := [][]string{
			{`changeLog.Enabled`, fmt.Sprint(stream.config.changelog.enabled)},
		}
		if stream.config.changelog.enabled {
			flowData = append(flowData,
				[]string{`changeLog.Buffered`, fmt.Sprint(stream.config.changelog.buffer.enabled)},
				[]string{`changeLog.Buffer.Size`, fmt.Sprint(stream.config.changelog.buffer.size)},
				[]string{`changeLog.Buffer.Flush`, stream.config.changelog.buffer.flushInterval.String()},
				[]string{`changeLog.MinInSycReplicas`, fmt.Sprint(stream.config.changelog.topic.ConfigEntries[`min.insync.replicas`])},
				[]string{`changeLog.ReplicationFactor`, fmt.Sprint(stream.config.changelog.topic.ReplicationFactor)},
				[]string{`changeLog.Replicated`, fmt.Sprint(stream.config.changelog.replicated)},
				[]string{`changeLog.Suffix`, fmt.Sprint(stream.config.changelog.suffix)},
			)
		}

		flowData = append(flowData,
			[]string{`worker-pool.order`, fmt.Sprint(stream.config.workerPool.Order)},
			[]string{`worker-pool.NumOfWorker`, fmt.Sprint(stream.config.workerPool.NumOfWorkers)},
			[]string{`worker-pool.WorkerBufferSize.`, fmt.Sprint(stream.config.workerPool.WorkerBufferSize)},
		)

		for _, v := range flowData {
			flowTable.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT})
			flowTable.Append(v)
		}
		flowTable.Render()
		data = append(data, []string{topic, b.String()})
	}

	data = append(data, []string{``})
	data = append(data, []string{`Global table configs`, ``})
	for topic, t := range b.globalTables {
		b := new(bytes.Buffer)
		flowTable := tablewriter.NewWriter(b)

		tableData := [][]string{
			{`store`, fmt.Sprint(t.store.Name())},
		}
		//if t.store.changelog.enabled {
		//	tableData = append(tableData,
		//		[]string{`changeLog.Buffered`, fmt.Sprint(t.store.changelog.buffer.enabled)},
		//		[]string{`changeLog.Buffer.Size`, fmt.Sprint(t.store.changelog.buffer.size)},
		//		[]string{`changeLog.Buffer.Flush`, t.store.changelog.buffer.flushInterval.String()},
		//		[]string{`changeLog.MinInSycReplicas`, fmt.Sprint(t.store.changelog.topic.minInSycReplicas)},
		//		[]string{`changeLog.ReplicationFactor`, fmt.Sprint(t.store.changelog.topic.replicationFactor)},
		//		[]string{`changeLog.Replicated`, fmt.Sprint(t.store.changelog.replicated)},
		//		[]string{`changeLog.Suffix`, fmt.Sprint(t.store.changelog.topic.suffix)},
		//	)
		//}

		for _, v := range tableData {
			flowTable.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT})
			flowTable.Append(v)
		}
		flowTable.Render()
		data = append(data, []string{topic, b.String()})
	}

	out := new(bytes.Buffer)
	table := tablewriter.NewWriter(out)
	table.SetHeader([]string{"Config", "Value"})

	for _, v := range data {
		table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT})
		table.Append(v)
	}
	table.Render()

	return out.String()
}
