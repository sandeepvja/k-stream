/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"context"
	"fmt"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/changelog"
	"github.com/pickme-go/k-stream/consumer"
	kContext "github.com/pickme-go/k-stream/context"
	kErr "github.com/pickme-go/k-stream/errors"
	"github.com/pickme-go/k-stream/internal/node"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/task_pool"
	"github.com/pickme-go/metrics"
	"github.com/pickme-go/traceable-context"
	"time"
)

type processor struct {
	id               string
	topicPartition   consumer.TopicPartition
	topologyBuilder  *node.TopologyBuilder
	changelogEnabled bool
	changelog        changelog.Changelog
	changelogBuilder changelog.Builder
	errorHandler     kErr.ErrorHandler
	records          chan *consumer.Record
	context          context.Context
	cancel           context.CancelFunc
	changelogMarks   chan *consumer.Record
	taskPoolConfig   *task_pool.PoolConfig
	taskPool         *task_pool.Pool
	logger           logger.Logger
	metricsReporter  metrics.Reporter
	metrics          struct {
		processedLatency metrics.Observer
	}
}

func newProcessor(id string, tp consumer.TopicPartition, changelog changelog.Builder, logger logger.Logger, metricsReporter metrics.Reporter) (*processor, error) {

	ctx, cancelFunc := context.WithCancel(context.Background())

	p := &processor{
		id:               id,
		topicPartition:   tp,
		context:          ctx,
		cancel:           cancelFunc,
		changelogMarks:   make(chan *consumer.Record),
		logger:           logger,
		changelogBuilder: changelog,
		metricsReporter:  metricsReporter,
	}

	p.metrics.processedLatency = metricsReporter.Observer(`k_stream_stream_processor_job_processed_latency_microseconds`, []string{`topic`, `partition`})

	return p, nil
}

func (p *processor) boot() error {

	defer p.logger.Info(`k-stream.streamProcessor`, fmt.Sprintf(`stream processor %s booted`, p.id))

	p.taskPool = task_pool.NewPool(
		p.topicPartition.String(),
		p.topologyBuilder,
		p.metricsReporter,
		p.taskPoolConfig)

	if p.changelogEnabled {

		stateChangelog, err := p.changelogBuilder(p.id, p.topicPartition.Topic, p.topicPartition.Partition)
		if err != nil {
			return errors.WithPrevious(err, `k-stream.streamProcessor`, `cannot init changelog`)
		}

		p.changelog = stateChangelog

		records, err := p.changelog.ReadAll(p.context)
		if err != nil {
			return errors.WithPrevious(err, `k-stream.streamProcessor`, `cannot recover`)
		}

		for _, record := range records {
			ctx := p.createContext(record)
			// these records are already marked in the changelog so START execution immediately
			p.execute(ctx, record.Timestamp, record)
		}
	}

	return nil
}

func (p *processor) start() {

	p.logger.Info(`k-stream.streamProcessor`,
		fmt.Sprintf("stream processor %s started", p.id))

	for record := range p.records {
		p.process(record)
	}

	// records chan is closed stop the processor
	p.Stop()

}

func (p *processor) process(record *consumer.Record) {

	ctx := p.createContext(record)

	// if message processing method is sync changelog marking is not necessary
	begin := time.Now()
	if p.taskPoolConfig.Order == task_pool.OrderPreserved {
		p.taskPool.Run(ctx, record.Key, record.Value, func() {
			p.metrics.processedLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{
				`topic`:     p.topicPartition.Topic,
				`partition`: fmt.Sprint(p.topicPartition.Partition),
			})
		})
		return
	}

	//first mark record in the changelog
	if p.changelogEnabled {
		if err := p.changelog.Put(ctx, record); err != nil {
			p.logger.ErrorContext(ctx, `k-stream.streamProcessor`,
				fmt.Sprintf(`cannot save to changelog - %+v`, err))
		} else {
			// send record to marked chen
			//p.changelogMarks <- record
			p.logger.TraceContext(ctx, `k-stream.streamProcessor`,
				fmt.Sprintf(`record mark on changelog %s[%d]`, record.Topic, record.Partition))
		}
	}

	p.execute(ctx, begin, record)

}

func (p *processor) execute(ctx context.Context, begin time.Time, record *consumer.Record) {

	p.taskPool.Run(ctx, record.Key, record.Value, func() {
		p.commit(ctx, begin, record)
	})

}

func (p *processor) commit(ctx context.Context, begin time.Time, record *consumer.Record) {

	// processing is done delete from changelog
	if p.changelogEnabled {
		if err := p.changelog.Delete(ctx, record); err != nil {
			p.logger.ErrorContext(ctx, `k-stream.streamProcessor`,
				fmt.Sprintf(`cannot delete from changelog %s[%d] due to %+v`, record.Topic, record.Partition, err))
			return
		}

		p.logger.TraceContext(ctx, `k-stream.streamProcessor`,
			fmt.Sprintf(`record deleted from changelog %s[%d]`, record.Topic, record.Partition))
	}

	p.metrics.processedLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{
		`topic`:     p.topicPartition.Topic,
		`partition`: fmt.Sprint(p.topicPartition.Partition),
	})
}

func (p *processor) createContext(record *consumer.Record) context.Context {
	return kContext.FromRecord(traceable_context.WithUUID(record.UUID), record)
}

func (p *processor) Stop() {

	p.logger.Info(`k-stream.streamProcessor`,
		fmt.Sprintf(`stream processor %s[%d] stopping...`, p.topicPartition.Topic, p.topicPartition.Partition))

	p.taskPool.Stop()

	defer p.logger.Info(`k-stream.streamProcessor`,
		fmt.Sprintf(`stream processor %s[%d] stopped`, p.topicPartition.Topic, p.topicPartition.Partition))

	p.cancel()

}
