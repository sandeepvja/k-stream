/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/k-stream/store_backend"
	"github.com/pickme-go/k-stream/task_pool"
	"github.com/pickme-go/metrics"
	"time"
)

type StreamBuilderConfig struct {
	ApplicationId    string
	AsyncProcessing  bool
	BootstrapServers []string // kafka Brokers
	WorkerPool       task_pool.PoolConfig
	Store            struct {
		BackendBuilder store_backend.Builder
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
		Type             dlq.DqlType // G, T
		Topic            string      // if global
	}
	Host      string
	ChangeLog struct {
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
	Consumer        *consumer.Config
	ConsumerCount   int
	Producer        *producer.Options
	MetricsReporter metrics.Reporter
}

func NewStreamBuilderConfig() *StreamBuilderConfig {
	config := &StreamBuilderConfig{}
	config.Producer = new(producer.Options)
	config.Consumer = consumer.NewConsumerConfig()
	config.ConsumerCount = 1

	config.ChangeLog.Suffix = `_changelog`
	config.ChangeLog.Replicated = false
	config.ChangeLog.MinInSycReplicas = 2
	config.ChangeLog.ReplicationFactor = 3
	config.ChangeLog.Buffer.Enabled = true
	config.ChangeLog.Buffer.Size = 100
	config.ChangeLog.Buffer.FlushInterval = 100 * time.Millisecond

	config.Producer.NumOfWorkers = 1
	config.Producer.RetryBackOff = time.Millisecond * 30
	config.Producer.Retry = 5
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = producer.WaitForAll
	config.Producer.BatchNumMessages = 1
	config.Producer.QueueBufferingMax = 1

	//set default task execution order
	config.WorkerPool.Order = task_pool.OrderByKey
	config.WorkerPool.NumOfWorkers = 100
	config.WorkerPool.WorkerBufferSize = 10

	// default metrics reporter
	config.MetricsReporter = metrics.NoopReporter()

	return config
}

func (c *StreamBuilderConfig) validate() {
	if c.ApplicationId == `` {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[ApplicationId] cannot be empty`)
	}

	if c.Host == `` {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[Host] cannot be empty`)
	}

	if len(c.BootstrapServers) < 1 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[BootstrapServers] cannot be empty`)
	}

	if c.ChangeLog.MinInSycReplicas < 1 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[ChangeLog.MinInSycReplicas] cannot be zero`)
	}

	if c.ChangeLog.ReplicationFactor < 1 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[ChangeLog.ReplicationFactor] cannot be zero`)
	}

	if c.ChangeLog.Buffer.FlushInterval < 1 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[ChangeLog.Buffer.FlushInterval] cannot be zero`)
	}

	if c.ChangeLog.Buffer.Size < 1 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[ChangeLog.Buffer.Size] cannot be zero`)
	}

	// producer configurations
	if c.Producer.QueueBufferingMax < 1 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[Producer.QueueBufferingMax] should be greater than zero`)
	}

	if c.Producer.BatchNumMessages < 1 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[Producer.BatchNumMessages] should be greater than zero`)
	}

	if c.Producer.Retry < 1 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[Producer.Retry] should be greater than zero`)
	}

	if c.Producer.RetryBackOff < 1*time.Millisecond {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[Producer.RetryBackOff] should be equal or greater than 1ms`)
	}

	//DLQ configurations
	if c.DLQ.Enabled {
		if len(c.DLQ.BootstrapServers) < 1 {
			logger.DefaultLogger.Fatal(`k-stream.stream-builder`, `[DLQ.BootstrapServers] cannot be empty`)
		}

		if c.DLQ.Type == dlq.DqlGlobal && c.DLQ.TopicFormat == `` {
			logger.DefaultLogger.Fatal(`k-stream.stream-builder`,
				`[DLQ.BootstrapServers] global topic format cannot be empty when topic type is [dlq.DqlGlobal]`)
		}
	}

	//Worker Pool options
	if c.WorkerPool.Order > 2 || c.WorkerPool.Order < 0 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`,
			`Invalid WorkerPool Order`)
	}

	if c.WorkerPool.WorkerBufferSize < 1 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`,
			`WorkerPool WorkerBufferSize should be greater than 0`)
	}

	if c.WorkerPool.NumOfWorkers < 1 {
		logger.DefaultLogger.Fatal(`k-stream.stream-builder`,
			`WorkerPool NumOfWorkers should be greater than 0`)
	}
}
