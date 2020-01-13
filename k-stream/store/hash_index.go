package store

import (
	"errors"
	"fmt"
	"sync"
)

type KeyMapper func(key, val interface{}) (idx string)

var UnknownIndex = errors.New(`index does not exist`)

type stringHashIndex struct {
	indexes map[string]map[string]bool // indexKey:recordKey:bool
	mapper  KeyMapper
	mu      *sync.Mutex
	name    string
}

func NewStringHashIndex(name string, mapper KeyMapper) Index {
	return &stringHashIndex{
		indexes: make(map[string]map[string]bool),
		mapper:  mapper,
		mu:      new(sync.Mutex),
		name:    name,
	}
}

func (s *stringHashIndex) Name() string {
	return s.name
}

func (s *stringHashIndex) Write(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashKey := s.mapper(key, value)
	_, ok := s.indexes[hashKey]
	if !ok {
		s.indexes[hashKey] = make(map[string]bool)
	}
	s.indexes[hashKey][key.(string)] = true
	return nil
}

func (s *stringHashIndex) Delete(key, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashKey := s.mapper(key, value)
	if _, ok := s.indexes[hashKey]; !ok {
		return fmt.Errorf(`hashKey %s does not exist for %s`, hashKey, s.name)
	}

	delete(s.indexes[hashKey], key.(string))
	return nil
}

func (s *stringHashIndex) Read(key string) ([]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var indexes []interface{}
	index, ok := s.indexes[key]
	if !ok {
		return nil, UnknownIndex
	}
	for k := range index {
		indexes = append(indexes, k)
	}

	return indexes, nil
}
