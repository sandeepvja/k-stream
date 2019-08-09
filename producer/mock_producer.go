package producer

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/logger"
	"time"
)

type MockProducer struct {
	mockSarama *mocks.SyncProducer
	closed     bool
}

func NewMockProducer(logger logger.TestingLogger) Producer {

	config := sarama.NewConfig()

	mockSarama := mocks.NewSyncProducer(logger, config)
	mockSarama.ExpectSendMessageAndSucceed()

	return &MockProducer{
		mockSarama: mockSarama,
	}
}

func (p *MockProducer) Produce(ctx context.Context, message *consumer.Record) (partition int32, offset int64, err error) {
	p.mockSarama.ExpectSendMessageAndSucceed()
	msg := &sarama.ProducerMessage{
		Topic:     message.Topic,
		Key:       sarama.ByteEncoder(message.Key),
		Value:     sarama.ByteEncoder(message.Value),
		Timestamp: time.Now(),
	}

	if !message.Timestamp.IsZero() {
		msg.Timestamp = message.Timestamp
	}

	if message.Partition > 0 {
		msg.Partition = message.Partition
	}

	_, o, err := p.mockSarama.SendMessage(msg)
	if err != nil {
		return 0, 0, errors.WithPrevious(err, `k-stream.saramaProducer`, `cannot send message :`)
	}

	return msg.Partition, o, nil

}

func (p *MockProducer) ProduceBatch(ctx context.Context, messages []*consumer.Record) error {
	p.mockSarama.ExpectSendMessageAndSucceed()
	msgs := make([]*sarama.ProducerMessage, 0)
	for _, message := range messages {

		m := &sarama.ProducerMessage{
			Topic:     message.Topic,
			Key:       sarama.ByteEncoder(message.Key),
			Value:     sarama.ByteEncoder(message.Value),
			Timestamp: time.Now(),
		}

		if message.Partition > 0 {
			m.Partition = message.Partition
		}

		msgs = append(msgs, m)
	}

	err := p.mockSarama.SendMessages(msgs)
	if err != nil {
		return errors.WithPrevious(err, `k-stream.saramaProducer`, `cannot produce batch`)
	}

	return nil
}

func (p *MockProducer) Close() error {
	p.closed = true
	return p.mockSarama.Close()
}

func (p *MockProducer) Closed() bool {
	return p.closed
}
