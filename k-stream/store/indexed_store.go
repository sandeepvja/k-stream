package store

import (
	"context"
	"fmt"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"sync"
	"time"
)

type Index interface {
	Name() string
	Write(key, value interface{}) error
	Delete(val, value interface{}) error
	Read(index string) ([]interface{}, error)
}

type IndexedStore interface {
	Store
	GetIndex(ctx context.Context, name string) (Index, error)
	GetIndexedRecords(ctx context.Context, index, key string) ([]interface{}, error)
}

type indexedStore struct {
	Store
	indexes map[string]Index
	mu      *sync.Mutex
}

func NewIndexedStore(name string, keyEncoder, valEncoder encoding.Encoder, indexes []Index, options ...Options) (IndexedStore, error) {
	store, err := NewStore(name, keyEncoder, valEncoder, options...)
	if err != nil {
		return nil, err
	}

	idxs := make(map[string]Index)
	for _, association := range indexes {
		idxs[association.Name()] = association
	}

	return &indexedStore{
		Store:   store,
		indexes: idxs,
		mu:      new(sync.Mutex),
	}, nil
}

func (i *indexedStore) Set(ctx context.Context, key, val interface{}, expiry time.Duration) error {
	// set indexes
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, index := range i.indexes {
		if err := index.Write(key.(string), val); err != nil {
			return err
		}
	}
	return i.Store.Set(ctx, key, val, expiry)
}

func (i *indexedStore) Delete(ctx context.Context, key interface{}) error {
	// delete indexes
	val, err := i.Store.Get(ctx, key)
	if err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	for _, index := range i.indexes {
		if err := index.Delete(key.(string), val); err != nil {
			return err
		}
	}
	return i.Store.Delete(ctx, key)
}

func (i *indexedStore) GetIndex(_ context.Context, name string) (Index, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	index, ok := i.indexes[name]
	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, name)
	}
	return index, nil
}

func (i *indexedStore) GetIndexedRecords(ctx context.Context, index, key string) ([]interface{}, error) {
	i.mu.Lock()
	idx, ok := i.indexes[index]
	i.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, index)
	}

	indexes, err := idx.Read(key)
	if err != nil {
		return nil, err
	}

	var records []interface{}
	for _, index := range indexes {
		record, err := i.Get(ctx, index)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	return records, nil
}
