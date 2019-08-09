/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package changelog

import (
	"context"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/metrics"
	"sync"
	"time"
)

// Buffer holds a temporary changelog Buffer
type Buffer struct {
	records       []*consumer.Record
	mu            *sync.Mutex
	shouldFlush   chan bool
	flushInterval time.Duration
	bufferSize    int
	producer      producer.Producer
	lastFlushed   time.Time
	metrics       struct {
		flushLatency metrics.Observer
	}
}

// NewBuffer creates a new Buffer object
func NewBuffer(p producer.Producer, size int, flushInterval time.Duration) *Buffer {
	flush := 1 * time.Second
	if flushInterval != 0 {
		flush = flushInterval
	}

	b := &Buffer{
		records:       make([]*consumer.Record, 0, size),
		mu:            new(sync.Mutex),
		producer:      p,
		bufferSize:    size,
		shouldFlush:   make(chan bool, 1),
		flushInterval: flush,
		lastFlushed:   time.Now(),
	}

	go b.runFlusher()

	return b
}

// Clear clears the Buffer
func (b *Buffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.flushAll(); err != nil {
		logger.DefaultLogger.ErrorContext(context.Background(), `k-stream.changelog.buffer`, err)
	}

}

func (b *Buffer) Records() []*consumer.Record {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.records
}

// Store stores the record in Buffer
func (b *Buffer) Store(record *consumer.Record) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.records = append(b.records, record)

	if len(b.records) >= b.bufferSize {
		b.flush()
	}
}

func (b *Buffer) runFlusher() {
	tic := time.NewTicker(b.flushInterval)
	defer tic.Stop()

	for range tic.C {

		if time.Since(b.lastFlushed) <= b.flushInterval {
			continue
		}

		b.mu.Lock()
		if len(b.records) > 0 {
			//b.flush()
		}
		b.mu.Unlock()

	}
}

func (b *Buffer) flush() {
	if err := b.flushAll(); err != nil {
		logger.DefaultLogger.ErrorContext(context.Background(), `k-stream.changelog.buffer`, err)
	}

	logger.DefaultLogger.Trace(`k-stream.changelog.buffer`, `buffer flushed`)
}

func (b *Buffer) flushAll() error {
	begin := time.Now()
	defer func(t time.Time) {
		b.metrics.flushLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), nil)
	}(begin)

	// publish buffer to kafka and clear on success
	//deDuplicated := deDuplicate(b.records)
	//if len(deDuplicated) > 0 {
	//	if err := b.producer.ProduceBatch(context.Background(), deDuplicated); err != nil {
	//		return err
	//	}
	//}

	if err := b.producer.ProduceBatch(context.Background(), b.records); err != nil {
		return err
	}

	b.reset()

	return nil
}

func (b *Buffer) Delete(record *consumer.Record) {
	record.Value = nil
	b.Store(record)
}

func (b *Buffer) reset() {
	b.records = make([]*consumer.Record, 0, b.bufferSize)
	b.lastFlushed = time.Now()
}

func (b *Buffer) Close() {
	// flush existing buffer
	logger.DefaultLogger.Info(`k-stream.changelog.buffer`, `flushing buffer...`)
	if err := b.flushAll(); err != nil {
		logger.DefaultLogger.ErrorContext(context.Background(), `k-stream.changelog.buffer`, err)
	}
}
