package producer

import (
	"context"
	"github.com/pickme-go/k-stream/admin"
	"github.com/pickme-go/k-stream/data"
	"hash"
	"hash/fnv"
	"sync"
)

type MockStreamProducer struct {
	hasher hash.Hash32
	topics *admin.Topics
	mu     sync.Mutex
}

func NewMockProducer(topics *admin.Topics) *MockStreamProducer {
	return &MockStreamProducer{
		hasher: fnv.New32a(),
		topics: topics,
	}
}

func (msp *MockStreamProducer) Produce(ctx context.Context, message *data.Record) (partition int32, offset int64, err error) {
	msp.mu.Lock()
	msp.hasher.Reset()
	_, err = msp.hasher.Write(message.Key)
	msp.mu.Unlock()
	if err != nil {
		return partition, offset, err
	}

	topic, err := msp.topics.Topic(message.Topic)
	if err != nil {
		return partition, offset, err
	}

	p := int64(msp.hasher.Sum32()) % int64(len(topic.Partitions()))
	pt, err := topic.Partition(int(p))
	if err != nil {
		return
	}

	message.Partition = int32(p)
	if err = pt.Append(message); err != nil {
		return
	}

	return int32(p), message.Offset, nil
}

func (msp *MockStreamProducer) ProduceBatch(ctx context.Context, messages []*data.Record) error {
	for _, msg := range messages {
		if _, _, err := msp.Produce(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

func (msp *MockStreamProducer) Close() error {
	return nil
}
