package kstream

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/data"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"github.com/pickme-go/k-stream/k-stream/internal/node"
	"github.com/pickme-go/k-stream/producer"
	"time"
)

type KSink struct {
	Id                int32
	KeyEncoder        encoding.Encoder
	ValEncoder        encoding.Encoder
	Producer          producer.Producer
	ProducerBuilder   producer.Builder
	name              string
	TopicPrefix       string
	topic             topic
	Repartitioned     bool
	info              map[string]string
	KeyEncoderBuilder encoding.Builder
	ValEncoderBuilder encoding.Builder
	headerConstructor func(ctx context.Context, key, val interface{}) (records []*sarama.RecordHeader)
}

func (s *KSink) Childs() []node.Node {
	return []node.Node{}
}

func (s *KSink) ChildBuilders() []node.NodeBuilder {
	return []node.NodeBuilder{}
}

func (s *KSink) Build() (node.Node, error) {
	p, err := s.ProducerBuilder(&producer.Config{
		//id: producer.NewProducerId(s.topic(s.topic(s.TopicPrefix))),
	})
	if err != nil {
		return nil, errors.WithPrevious(err, `cannot Build producer`)
	}

	return &KSink{
		KeyEncoder:  s.KeyEncoderBuilder(),
		ValEncoder:  s.ValEncoderBuilder(),
		Producer:    p,
		TopicPrefix: s.TopicPrefix,
		name:        s.name,
		topic:       s.topic,
		info:        s.info,
	}, nil
}

func (s *KSink) AddChildBuilder(builder node.NodeBuilder) {
	panic("implement me")
}

func (s *KSink) AddChild(node node.Node) {
	panic("implement me")
}

//type kSinkBuilder struct {
//	keyEncoderBuilder encoding.Builder
//	valEncoderBuilder encoding.Builder
//	producerBuilder   producer.Builder
//	name              string
//	info              map[string]string
//	topic             string
//}

//func (b *kSinkBuilder) AddChildBuilder(builder node.NodeBuilder) {
//	panic("implement me")
//}
//
//func (b *kSinkBuilder) Build() (node.Node, error) {
//
//	p, err := b.producerBuilder(&producer.Options{
//		id: producer.NewProducerId(b.topic),
//	})
//	if err != nil {
//		return nil, errors.WithPrevious(err,  `cannot Build producer`)
//	}
//
//	return &kSink{
//		keyEncoder: b.keyEncoderBuilder(),
//		valEncoder: b.valEncoderBuilder(),
//		producer:   p,
//		name:       b.name,
//		topic:      b.topic,
//	}, nil
//}

type SinkOption func(sink *KSink)

func (s *KSink) applyOptions(options ...SinkOption) {
	for _, option := range options {
		option(s)
	}
}

func (s *KSink) Name() string {
	return `sink_` + s.topic(s.TopicPrefix)
}

func (*KSink) Next() bool {
	return false
}

func (s *KSink) SinkType() string {
	return `kafka`
}

func (*KSink) Type() node.Type {
	return node.TypeSink
}

func (s *KSink) Info() map[string]string {
	return map[string]string{
		`topic`: s.topic(s.TopicPrefix),
	}
}

func WithProducer(p producer.Builder) SinkOption {
	return func(sink *KSink) {
		sink.ProducerBuilder = func(conf *producer.Config) (producer.Producer, error) {
			return p(nil)
		}
	}
}

func withPrefixTopic(topic topic) SinkOption {
	return func(sink *KSink) {
		sink.topic = topic
	}
}

func WithHeaders(f func(ctx context.Context, key, val interface{}) (records []*sarama.RecordHeader)) SinkOption {
	return func(sink *KSink) {
		sink.headerConstructor = f
	}
}

func NewKSinkBuilder(name string, id int32, topic topic, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...SinkOption) *KSink {

	builder := &KSink{
		ValEncoderBuilder: valEncoder,
		KeyEncoderBuilder: keyEncoder,
		topic:             topic,
		name:              name,
		Id:                id,
	}

	builder.applyOptions(options...)
	return builder
}

func (s *KSink) Close() error {
	return nil
}

func (s *KSink) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {

	record := new(data.Record)
	record.Timestamp = time.Now()
	record.Topic = s.topic(s.TopicPrefix)

	keyByt, err := s.KeyEncoder.Encode(kIn)
	if err != nil {
		return nil, nil, false, err
	}
	record.Key = keyByt
	if vIn == nil {
		record.Value = nil
	} else {
		valByt, err := s.ValEncoder.Encode(vIn)
		if err != nil {
			return nil, nil, false, err
		}
		record.Value = valByt
	}

	if s.headerConstructor != nil {
		headers := s.headerConstructor(ctx, kIn, vIn)
		if len(headers) > 0 {
			record.Headers = make([]*sarama.RecordHeader, 0)
			record.Headers = append(record.Headers, headers...)
		}
	}

	if _, _, err := s.Producer.Produce(ctx, record); err != nil {
		return nil, nil, false, err
	}

	return nil, nil, true, nil
}

func (s *KSink) ID() int32 {
	return s.Id
}
