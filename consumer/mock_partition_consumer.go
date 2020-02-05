package consumer

import (
	"github.com/google/uuid"
	"github.com/pickme-go/k-stream/admin"
	"github.com/pickme-go/k-stream/data"
	"github.com/pickme-go/k-stream/k-stream/offsets"
	"log"
	"time"
)

type mockPartitionConsumer struct {
	topics         *admin.Topics
	offsets        offsets.Manager
	fetchInterval  time.Duration
	closing        bool
	closed         chan bool
	fetchBatchSize int
	events         chan Event
}

func NewMockPartitionConsumer(topics *admin.Topics, offsets offsets.Manager) *mockPartitionConsumer {
	return &mockPartitionConsumer{
		topics:         topics,
		fetchInterval:  100 * time.Millisecond,
		fetchBatchSize: 1000,
		closed:         make(chan bool, 1),
		offsets:        offsets,
		events:         make(chan Event, 100),
	}
}

func (m *mockPartitionConsumer) Consume(topic string, partition int32, offset Offset) (<-chan Event, error) {

	go m.consume(topic, partition, offset)
	return m.events, nil
}

func (m *mockPartitionConsumer) consume(topic string, partition int32, offset Offset) error {
	pt := m.topics.Topics()[topic].Partitions()[int(partition)]

	var currentOffset = int64(offset)

	for !m.closing {
		time.Sleep(m.fetchInterval)

		records, err := pt.Fetch(currentOffset, m.fetchBatchSize)
		if err != nil {
			log.Fatal(err)
		}

		if len(records) < 1 {
			m.events <- &PartitionEnd{}
			continue
		}

		partitionEnd, err := m.offsets.GetOffsetLatest(topic, partition)
		if err != nil {
			log.Fatal(err)
		}

		for _, msg := range records {
			m.events <- &data.Record{
				Key:       msg.Key,
				Value:     msg.Value,
				Offset:    msg.Offset,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Timestamp: msg.Timestamp,
				UUID:      uuid.New(),
				Headers:   msg.Headers,
			}

			//if highWatermark == 0 || highWatermark-1 == msg.Offset {
			if msg.Offset == partitionEnd {
				m.events <- &PartitionEnd{}
			}
		}

		currentOffset = records[len(records)-1].Offset + 1

	}

	m.closed <- true
	return nil
}

func (m *mockPartitionConsumer) Errors() <-chan *Error {
	return make(chan *Error)
}

func (m *mockPartitionConsumer) Close() error {
	m.closing = true
	<-m.closed
	close(m.events)
	return nil
}

func (m *mockPartitionConsumer) Id() string {
	panic("implement me")
}
