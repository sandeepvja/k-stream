package store

import (
	"fmt"
	"sync"
)

type KeyMapper func(key, val interface{}) (idx string)

type Association interface {
	Store() Store
	Name() string
	KeyMapper() KeyMapper
	Write(associatedKey, value string) error
	Delete(associatedKey, value string) error
	Read(associatedKey string) ([]string, error)
}

type association struct {
	indexes map[string]map[string]bool // indexKey:recordKey:bool
	mapper  KeyMapper
	store   Store
	mu      *sync.Mutex
}

func NewAssociation(store Store, mapper KeyMapper) Association {
	return &association{
		indexes: make(map[string]map[string]bool),
		store:   store,
		mapper:  mapper,
		mu:      new(sync.Mutex),
	}
}

func (s *association) Store() Store {
	return s.store
}

func (s *association) Name() string {
	return s.store.Name()
}

func (s *association) KeyMapper() KeyMapper {
	return s.mapper
}

func (s *association) Write(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	associatedKey := s.KeyMapper()(key, value)
	_, ok := s.indexes[associatedKey]
	if !ok {
		s.indexes[associatedKey] = make(map[string]bool)
	}
	s.indexes[associatedKey][key] = true
	return nil
}

func (s *association) Delete(key, value string) error {
	associatedKey := s.KeyMapper()(key, value)
	if _, ok := s.indexes[associatedKey]; !ok {
		return fmt.Errorf(`assosiation %s does not exist for %s`, associatedKey, s.store.Name())
	}

	delete(s.indexes[associatedKey], key)
	return nil
}

func (s *association) Read(associatedKey string) ([]string, error) {
	var indexes []string
	index, ok := s.indexes[associatedKey]
	if !ok {
		return nil, fmt.Errorf(`association %s does not exist`, associatedKey)
	}
	for k := range index {
		indexes = append(indexes, k)
	}

	return indexes, nil
}
