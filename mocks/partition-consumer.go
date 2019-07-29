package mocks

import "github.com/pickme-go/k-stream/consumer"

type MockPartitionConsumer struct {
	records      consumer.Event
	recordsArray []consumer.Event
}

func (*MockPartitionConsumer) Consume(topic string, partition int32, offset consumer.Offset) (<-chan consumer.Event, error) {
	panic("implement me")
}

func (*MockPartitionConsumer) Errors() <-chan *consumer.Error {
	panic("implement me")
}

func (*MockPartitionConsumer) GetOldestOffset(topic string, partition int32) (int64, error) {
	panic("implement me")
}

func (*MockPartitionConsumer) GetLatestOffset(topic string, partition int32) (int64, error) {
	panic("implement me")
}

func (*MockPartitionConsumer) Close() error {
	panic("implement me")
}

func (*MockPartitionConsumer) Id() string {
	panic("implement me")
}
