package producer

import (
	"github.com/pickme-go/k-stream/consumer"
	"testing"
)

func TestMockProducer_Produce(t *testing.T) {
	producer := NewMockProducer(t)

	msg := &consumer.Record{
		Key:       []byte(string(`100`)),
		Value:     []byte(string(`100`)),
		Partition: 1,
	}

	p, _, err := producer.Produce(msg)
	if err != nil {
		t.Error(err)
	}

	if p != 1 {
		t.Fail()
	}
}

func TestMockProducer_ProduceBatch(t *testing.T) {
	producer := NewMockProducer(t)

	msg1 := &consumer.Record{
		Key:       []byte(string(`100`)),
		Value:     []byte(string(`100`)),
		Partition: 1,
	}

	msg2 := *msg1
	msg2.Key = []byte(string(`100`))

	err := producer.ProduceBatch([]*consumer.Record{msg1, &msg2})
	if err != nil {
		t.Error(err)
	}
}
