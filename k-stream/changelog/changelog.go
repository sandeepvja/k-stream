/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package changelog

import (
	"context"
	"github.com/pickme-go/k-stream/data"
)

type Builder func(id string, topic string, partition int32, opts ...Options) (Changelog, error)

type Changelog interface {
	ReadAll(ctx context.Context) ([]*data.Record, error)
	Put(ctx context.Context, record *data.Record) error
	PutAll(ctx context.Context, record []*data.Record) error
	Delete(ctx context.Context, record *data.Record) error
	DeleteAll(ctx context.Context, record []*data.Record) error
	Close()
}
