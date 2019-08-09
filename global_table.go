/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"github.com/pickme-go/k-stream/encoding"
	"github.com/pickme-go/k-stream/internal/join"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/processors"
	"github.com/pickme-go/k-stream/store"
)

type GlobalTableOffset int

// table will start syncing from locally stored offset or topic oldest offset
const GlobalTableOffsetDefault GlobalTableOffset = 0

// table will start syncing from topic latest offset (suitable for stream topics since the topic can contains historical
// data )
const GlobalTableOffsetLatest GlobalTableOffset = -1

type globalTableOptions struct {
	initialOffset GlobalTableOffset
}

type globalTableOption func(options *globalTableOptions)

func GlobalTableWithOffset(offset GlobalTableOffset) globalTableOption {
	return func(options *globalTableOptions) {
		options.initialOffset = offset
	}
}

type GlobalTable interface {
	Stream
}

type globalKTable struct {
	*kStream
	storeName string
	store     store.Store
	options   *globalTableOptions
}

func NewGlobalKTable(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, store string, options ...globalTableOption) GlobalTable {

	if keyEncoder == nil {
		logger.DefaultLogger.Fatal(`k-stream.globalKTable`, `keyEncoder cannot be null`)
	}

	if valEncoder == nil {
		logger.DefaultLogger.Fatal(`k-stream.globalKTable`, `valEncode cannot be null`)
	}

	//apply options
	opts := new(globalTableOptions)
	opts.initialOffset = GlobalTableOffsetDefault
	for _, o := range options {
		o(opts)
	}

	s := NewKStream(topic, keyEncoder, valEncoder)
	kStream, _ := s.(*kStream)
	stream := &globalKTable{
		kStream:   kStream,
		storeName: store,
		options:   opts,
	}

	return stream
}

func (t *globalKTable) To(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...SinkOption) {
	logger.DefaultLogger.Fatal(`k-stream.globalKTable`, `global table dose not support stream processing`)
}

func (t *globalKTable) Transform(transformer processors.TransFunc) Stream {
	logger.DefaultLogger.Fatal(`k-stream.globalKTable`, `global table dose not support stream transforming`)
	return nil
}

func (t *globalKTable) Filter(filter processors.FilterFunc) Stream {
	logger.DefaultLogger.Fatal(`k-stream.globalKTable`, `global table dose not support stream processing`)
	return nil
}

func (t *globalKTable) Process(processor processors.ProcessFunc) Stream {
	logger.DefaultLogger.Fatal(`k-stream.globalKTable`, `global table dose not support stream processing`)
	return nil
}

func (t *globalKTable) Join(stream Stream, keyMapper join.KeyMapper, valMapper join.ValueMapper) Stream {
	logger.DefaultLogger.Fatal(`k-stream.globalKTable`, `global table to global table joins are not supported yet`)
	return nil
}
