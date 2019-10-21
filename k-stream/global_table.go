/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"github.com/pickme-go/k-stream/k-stream/internal/join"
	"github.com/pickme-go/k-stream/k-stream/processors"
	"github.com/pickme-go/k-stream/k-stream/store"
	"github.com/pickme-go/log/v2"
)

type GlobalTableOffset int

// table will start syncing from locally stored offset or topic oldest offset
const GlobalTableOffsetDefault GlobalTableOffset = 0

// table will start syncing from topic latest offset (suitable for stream topics since the topic can contains historical
// data )
const GlobalTableOffsetLatest GlobalTableOffset = -1

type globalTableOptions struct {
	initialOffset GlobalTableOffset
	logger        log.Logger
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

//func NewGlobalKTable(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, store string, options ...globalTableOption) GlobalTable {
//
//	//apply options
//	opts := new(globalTableOptions)
//	opts.initialOffset = GlobalTableOffsetDefault
//	for _, o := range options {
//		o(opts)
//	}
//
//	if keyEncoder == nil {
//		opts.logger.Fatal(`k-stream.globalKTable`, `keyEncoder cannot be null`)
//	}
//
//	if valEncoder == nil {
//		opts.logger.Fatal(`k-stream.globalKTable`, `valEncode cannot be null`)
//	}
//
//
//
//	s := NewKStream(topic, keyEncoder, valEncoder)
//	kStream, _ := s.(*kStream)
//	stream := &globalKTable{
//		kStream:   kStream,
//		storeName: store,
//		options:   opts,
//		logger:opts.logger,
//	}
//
//	return stream
//}

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
