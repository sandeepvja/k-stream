/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"github.com/pickme-go/k-stream/data"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"github.com/pickme-go/k-stream/k-stream/internal/join"
	"github.com/pickme-go/k-stream/k-stream/processors"
	"github.com/pickme-go/k-stream/k-stream/store"
	"github.com/pickme-go/log/v2"
)

type GlobalTableOffset int

// table will start syncing from locally stored offset or topic oldest offset
const GlobalTableOffsetDefault GlobalTableOffset = 0

var globalTableStoreWriter = func(r *data.Record, store store.Store) error {
	if r.Value == nil {
		if err := store.Backend().Delete(r.Key); err != nil {
			return err
		}
	}

	return store.Backend().Set(r.Key, r.Value, 0)
}

// table will start syncing from topic latest offset (suitable for stream topics since the topic can contains historical
// data )
const GlobalTableOffsetLatest GlobalTableOffset = -1

type globalTableOptions struct {
	initialOffset GlobalTableOffset
	logger        log.Logger
	storeWriter   StoreWriter
}

type globalTableOption func(options *globalTableOptions)

func GlobalTableWithOffset(offset GlobalTableOffset) globalTableOption {
	return func(options *globalTableOptions) {
		options.initialOffset = offset
	}
}

func GlobalTableWithLogger(logger log.Logger) globalTableOption {
	return func(options *globalTableOptions) {
		options.logger = logger
	}
}

func GlobalTableWithBackendWriter(writer StoreWriter) globalTableOption {
	return func(options *globalTableOptions) {
		options.storeWriter = writer
	}
}

type GlobalTable interface {
	Stream
}

type globalKTable struct {
	*kStream
	storeName string
	logger    log.Logger
	store     store.Store
	options   *globalTableOptions
}

func (t *globalKTable) To(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...SinkOption) {
	t.logger.Fatal(`k-stream.globalKTable`, `global table dose not support stream processing`)
}

func (t *globalKTable) Transform(transformer processors.TransFunc) Stream {
	t.logger.Fatal(`k-stream.globalKTable`, `global table dose not support stream transforming`)
	return nil
}

func (t *globalKTable) Filter(filter processors.FilterFunc) Stream {
	t.logger.Fatal(`k-stream.globalKTable`, `global table dose not support stream processing`)
	return nil
}

func (t *globalKTable) Process(processor processors.ProcessFunc) Stream {
	t.logger.Fatal(`k-stream.globalKTable`, `global table dose not support stream processing`)
	return nil
}

func (t *globalKTable) Join(stream Stream, keyMapper join.KeyMapper, valMapper join.ValueMapper) Stream {
	t.logger.Fatal(`k-stream.globalKTable`, `global table to global table joins are not supported yet`)
	return nil
}
