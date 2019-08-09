/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"github.com/pickme-go/k-stream/encoding"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/store"
)

type KTable struct {
	kStream
	store store.Store
}

func NewKTable(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Option) Stream {
	if keyEncoder == nil {
		logger.DefaultLogger.Fatal(`k-stream.kStream`, `keyEncoder cannot be null`)
	}

	if valEncoder == nil {
		logger.DefaultLogger.Fatal(`k-stream.kStream`, `valEncoder cannot be null`)
	}

	return newKStream(func(s string) string { return topic }, keyEncoder, valEncoder, nil, options...)
}
