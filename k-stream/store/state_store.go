package store

import (
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/data"
	"github.com/pickme-go/k-stream/k-stream/encoding"
)

type stateStore struct {
	name       string
	options    *storeOptions
	backend    backend.Backend
	keyEncoder encoding.Encoder
	valEncoder encoding.Encoder
}

func NewStateStore(name string, keyEncoder encoding.Encoder, valEncoder encoding.Encoder, options ...Options) StateStore {

	configs := storeOptions{}
	configs.apply(options...)

	return &stateStore{
		name:       name,
		keyEncoder: keyEncoder,
		valEncoder: valEncoder,
	}
}

func (s *stateStore) Name() string {
	return s.name
}

func (s *stateStore) Set(key interface{}, value interface{}) error {
	k, err := s.keyEncoder.Encode(key)
	if err != nil {
		return errors.WithPrevious(err, `key encode err `)
	}

	v, err := s.valEncoder.Encode(value)
	if err != nil {
		return errors.WithPrevious(err, `key encode err `)
	}

	return s.backend.Set(k, v, 0)
}

func (s *stateStore) Get(key interface{}) (value interface{}, err error) {
	k, err := s.keyEncoder.Encode(key)
	if err != nil {
		return nil, errors.WithPrevious(err, `key encode err `)
	}

	byts, err := s.options.backend.Get(k)
	if err != nil {
		return nil, errors.WithPrevious(err, `key encode err `)
	}

	v, err := s.valEncoder.Decode(byts)
	if err != nil {
		return nil, errors.WithPrevious(err, `value decode err `)
	}

	return v, nil
}

func (s *stateStore) GetAll() ([]*data.Record, error) {
	panic("implement me")
}
