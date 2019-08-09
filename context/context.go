package context

import (
	"context"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/traceable-context"
	"time"
)

var recordMeta = `rc_meta`

type RecordMeta struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

type Context struct {
	context.Context
}

func FromRecord(parent context.Context, record *consumer.Record) context.Context {

	return traceable_context.WithValue(parent, &recordMeta, &RecordMeta{
		Topic:     record.Topic,
		Offset:    record.Offset,
		Partition: record.Partition,
		Timestamp: record.Timestamp,
	})
}

func RecordFromContext(ctx context.Context, key []byte, val []byte) (*consumer.Record, error) {

	if c, ok := ctx.(*Context); ok {

		meta := Meta(c)

		return &consumer.Record{
			Topic:     meta.Topic,
			Partition: meta.Partition,
			Offset:    meta.Offset,
			Timestamp: meta.Timestamp,
			Key:       key,
			Value:     val,
		}, nil
	}

	return nil, errors.New(`k-stream.context`, `invalid context expected [k-stream.context]`)
}

func Meta(ctx context.Context) *RecordMeta {
	if meta, ok := ctx.Value(&recordMeta).(*RecordMeta); ok {
		return meta
	}

	panic(`k-stream.context meta not available`)
}
