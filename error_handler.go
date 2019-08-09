package kstream

//import (
//	"context"
//	"encoding/json"
//	"fmt"
//	"github.com/pickme-go/log"
//	"github.com/pickme-go/k-stream/consumer"
//	kstreamContext "github.com/pickme-go/k-stream/context"
//	"github.com/pickme-go/k-stream/errors"
//	"github.com/pickme-go/k-stream/producer"
//)
//
//type errorHandlerType int
//
//const ErrorTopicGlobal errorHandlerType = 0
//const ErrorTopicMultiple errorHandlerType = 0
//
//type errorMessage struct {
//	Topic     string `json:"topic"`
//	Partition int32  `json:"partition"`
//	Key       []byte `json:"key"`
//	Val       []byte `json:"val"`
//	Meta      struct {
//		Err string `json:"err"`
//	} `json:"meta"`
//}
//
//type defaultErrorHandler struct {
//	errorTopic string
//	topicType  errorHandlerType
//	producer   producer.Producer
//}
//
//func NewDefaultErrorHandler() (errors.ErrorHandler, error) {
//	p, err := producer.DefaultBuilder(&producer.Options{
//		Partitioner: producer.Random,
//	})
//	if err != nil {
//		return nil, errors.New(`k-stream.ErrorHandler`, `cannot init producer`, err)
//	}
//	return &defaultErrorHandler{
//		producer: p,
//	}, nil
//}
//
//func (h *defaultErrorHandler) Handle(ctx context.Context, streamError *errors.StreamError) {
//	r, err := h.prepareMessage(ctx, streamError)
//	if err != nil {
//		log.Error(log.WithPrefix(`k-stream.ErrorHandler`, fmt.Sprintf(`cannot decode error message - %+v`, err)))
//		return
//	}
//
//	if _, _, err = h.producer.Produce(r); err != nil {
//		log.Error(log.WithPrefix(`k-stream.ErrorHandler`, fmt.Sprintf(`cannot publish error message : %+v`, err)))
//		return
//	}
//}
//
//func (h *defaultErrorHandler) prepareMessage(ctx context.Context, e *errors.StreamError) (*consumer.Record, error) {
//	ct, ok := ctx.(*kstreamContext.Context)
//	if !ok {
//		return nil, errors.New(
//			`k-stream.ErrorHandler`,
//			`context should be the type of [kstream.Context]`,
//			nil)
//	}
//
//	msg := errorMessage{
//		Topic:     ct.Meta().Topic,
//		Partition: ct.Meta().Partition,
//		Key:       e.Key,
//		Val:       e.Value,
//	}
//
//	msg.Meta.Err = e.Err.Error()
//
//	byt, err := json.Marshal(msg)
//	if err != nil {
//		return nil, err
//	}
//
//	return &consumer.Record{
//		Key:   e.Key,
//		Value: byt,
//		Topic: h.topic(ct.Meta().Topic),
//	}, nil
//}
//
//func (h *defaultErrorHandler) topic(recordTopic string) string {
//	if h.topicType == ErrorTopicMultiple {
//		return recordTopic
//	}
//
//	return h.errorTopic
//}
