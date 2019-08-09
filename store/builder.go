package store

import "github.com/pickme-go/k-stream/encoding"

type Builder func(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Options) (Store, error)

type StateStoreBuilder func(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Options) StateStore
