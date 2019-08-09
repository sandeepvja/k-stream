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

var testBuffer = NewBuffer(producer.NewMockProducer(nil), 10, 100*time.Millisecond)

func init() {
	go testBuffer.runFlusher()
	time.Sleep(1 * time.Second)
}

var rec = &consumer.Record{
	Key:   []byte(`100`),
	Value: []byte(`100`),
}

func BenchmarkBuffer_Store(b *testing.B) {

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			println(22)
			testBuffer.Store(rec)
		}
	})
}
