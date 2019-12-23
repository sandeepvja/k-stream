package store

import (
	"context"
	"fmt"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"sync"
	"time"
)

type AssociationStore interface {
	Store
	GetAssociate(ctx context.Context, name string) (Association, error)
	GetAssociateRecords(ctx context.Context, name, key string) ([]interface{}, error)
}

type associationStore struct {
	Store
	associations map[string]Association
	mu           *sync.Mutex
}

func NewStoreWithAssociations(name string, keyEncoder, valEncoder encoding.Encoder, associations []Association, options ...Options) (AssociationStore, error) {
	store, err := NewStore(name, keyEncoder, valEncoder, options...)
	if err != nil {
		return nil, err
	}

	assocs := make(map[string]Association)
	for _, association := range associations {
		assocs[association.Name()] = association
	}

	return &associationStore{
		Store:        store,
		associations: assocs,
		mu:           new(sync.Mutex),
	}, nil
}

func (i *associationStore) Set(ctx context.Context, key, val interface{}, expiry time.Duration) error {
	// set associations
	i.mu.Lock()
	for _, assoc := range i.associations {
		if err := assoc.Write(key.(string), val); err != nil {
			return err
		}
	}
	i.mu.Unlock()
	return i.Store.Set(ctx, key, val, expiry)
}

func (i *associationStore) Delete(ctx context.Context, key interface{}) error {
	// delete associations
	val, err := i.Store.Get(ctx, key)
	if err != nil {
		return err
	}

	i.mu.Lock()
	for _, assoc := range i.associations {
		if err := assoc.Delete(key.(string), val.(string)); err != nil {
			return err
		}
	}
	i.mu.Unlock()

	return i.Store.Delete(ctx, key)
}

func (i *associationStore) GetAssociate(ctx context.Context, name string) (Association, error) {
	i.mu.Lock()
	association, ok := i.associations[name]
	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, name)
	}
	i.mu.Unlock()

	return association, nil
}

func (i *associationStore) GetAssociateRecords(ctx context.Context, name, key string) ([]interface{}, error) {
	i.mu.Lock()
	association, ok := i.associations[name]
	if !ok {
		return nil, fmt.Errorf(`associate [%s] does not exist`, name)
	}
	i.mu.Unlock()

	indexes, err := association.Read(key)
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
