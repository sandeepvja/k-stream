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

// Starting offset for the global table partition.
type GlobalTableOffset int

// GlobalTableOffsetDefault defines the starting offset for the GlobalTable when GlobalTable stream syncing started.
const GlobalTableOffsetDefault GlobalTableOffset = 0

// GlobalTableOffsetLatest defines the beginning of the partition.
// suitable for stream topics since the topic can contains historical data.
const GlobalTableOffsetLatest GlobalTableOffset = -1

// globalTableStoreWriter overrides the persisting logic for GlobalTables.
var globalTableStoreWriter = func(r *data.Record, store store.Store) error {
	// tombstone handling
	if r.Value == nil {
		if err := store.Backend().Delete(r.Key); err != nil {
			return err
		}
	}

	return store.Backend().Set(r.Key, r.Value, 0)
}

type globalTableOptions struct {
	initialOffset GlobalTableOffset
	logger        log.Logger
	backendWriter StoreWriter
}

type GlobalTableOption func(options *globalTableOptions)

// GlobalTableWithOffset overrides the default starting offset when GlobalTable syncing started.
func GlobalTableWithOffset(offset GlobalTableOffset) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.initialOffset = offset
	}
}

// GlobalTableWithLogger overrides the default logger for the GlobalTable (default is NoopLogger).
func GlobalTableWithLogger(logger log.Logger) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.logger = logger
	}
}

// GlobalTableWithBackendWriter overrides the persisting behavior of the GlobalTable.
// eg :
//	func(r *data.Record, store store.Store) error {
//		// tombstone handling
//		if r.Value == nil {
//			if err := store.Backend().Delete(r.Key); err != nil {
//				return err
//			}
//		}
//
//		return store.Backend().Set(r.Key, r.Value, 0)
//	}
func GlobalTableWithBackendWriter(writer StoreWriter) GlobalTableOption {
	return func(options *globalTableOptions) {
		options.backendWriter = writer
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
