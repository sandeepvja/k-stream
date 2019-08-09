/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package changelog

import (
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/producer"
	"testing"
	"time"
)

func TestNewBuffer(t *testing.T) {
	b := NewBuffer(producer.NewMockProducer(t), 10, 10*time.Second)
	if b.records == nil {
		t.Fail()
	}

	if b.mu == nil {
		t.Fail()
	}
}

func TestBufferStore(t *testing.T) {
	b := NewBuffer(producer.NewMockProducer(t), 10, 10*time.Second)

	rec := new(consumer.Record)
	rec.Key = []byte(`key`)
	b.Store(rec)

	if string(b.records[0].Key) != string(rec.Key) {
		t.Fail()
	}
}

func TestBufferClear(t *testing.T) {
	b := NewBuffer(producer.NewMockProducer(t), 10, 10*time.Second)

	rec := new(consumer.Record)
	rec.Key = []byte(``)
	rec.Value = []byte(``)
	b.Store(rec)

	b.Clear()

	if len(b.records) > 0 {
		t.Fail()
	}
}

func TestBufferShouldClearOnceFull(t *testing.T) {
	size := 5

	b := NewBuffer(producer.NewMockProducer(t), size, 10*time.Millisecond)
	go b.runFlusher()

	time.Sleep(1 * time.Second)

	rec := new(consumer.Record)
	for i := 0; i < size*20+1; i++ {
		b.Store(rec)
	}

	if len(b.records) != size {
		t.Fail()
	}
}

func TestBufferFlushInterval(t *testing.T) {
	d := 100 * time.Millisecond
	b := NewBuffer(producer.NewMockProducer(t), 10, d)
	go b.runFlusher()

	time.Sleep(d)

	rec := new(consumer.Record)
	rec.Key = []byte(`100`)
	rec.Value = []byte(`200`)
	b.Store(rec)

	time.Sleep(d + 1*time.Second)

	if len(b.records) > 0 {
		t.Fail()
	}
}
