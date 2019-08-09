/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package changelog

import (
	"context"
	"github.com/pickme-go/k-stream/consumer"
)

type Builder func(id string, topic string, partition int32, opts ...Options) (Changelog, error)

type Changelog interface {
	ReadAll(ctx context.Context) ([]*consumer.Record, error)
	Put(ctx context.Context, record *consumer.Record) error
	PutAll(ctx context.Context, record []*consumer.Record) error
	Delete(ctx context.Context, record *consumer.Record) error
	DeleteAll(ctx context.Context, record []*consumer.Record) error
	Close()
}
