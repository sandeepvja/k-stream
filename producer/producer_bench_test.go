package producer

import (
	"github.com/pickme-go/k-stream/consumer"
	"testing"
)

func BenchmarkMockProducer_Produce(b *testing.B) {
	producer := NewMockProducer(b)

	msg := &consumer.Record{
		Key:       []byte(string(`100`)),
		Value:     []byte(string(`100`)),
		Partition: 1,
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := producer.Produce(msg)
			if err != nil {
				b.Error(err)
			}
		}
	})

}
