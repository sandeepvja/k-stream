package consumer

import (
	"bytes"
	"github.com/pickme-go/k-stream/admin"
	"github.com/pickme-go/k-stream/data"
	"testing"
)

func TestConsumer_Consume(t *testing.T) {
	admin := admin.NewMockAdminWithTopics(map[string]*admin.Topic{
		`test`: {
			Name:          "test",
			NumPartitions: 2,
		},
	})
	// append some messages to the partitions
	topic, _ := admin.Topics.Topic(`test`)
	partition, _ := topic.Partition(0)
	partition.Append(&data.Record{
		Key:       []byte(`1024`),
		Value:     []byte(`1024`),
		Partition: 0,
	})
	partition.Append(&data.Record{
		Key:       []byte(`1025`),
		Value:     []byte(`1025`),
		Partition: 1,
	})

	c := NewMockConsumer(admin.Topics)
	partitions, err := c.Consume([]string{`test`}, nil)
	if err != nil {
		t.Error(err)
	}

	for tp := range partitions {
		go tp.Records()
	}
}

func consumeAndAssert(tp Partition, value []byte) {
	for r := range tp.Records() {
		if bytes.Equal(r.Value, value) {

		}
	}
}
