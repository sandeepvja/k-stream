/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package backend

import (
	"time"
)

type Builder func(name string) (Backend, error)

type Backend interface {
	Name() string
	Set(key []byte, value []byte, expiry time.Duration) error
	Get(key []byte) ([]byte, error)
	RangeIterator(fromKy []byte, toKey []byte) Iterator
	Iterator() Iterator
	Delete(key []byte) error
	SetExpiry(time time.Duration)
	String() string
	Persistent() bool
	Close() error
	Destroy() error
}
