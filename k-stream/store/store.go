package store

import (
	"context"
	"fmt"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/data"
	"github.com/pickme-go/k-stream/k-stream/changelog"
	kContext "github.com/pickme-go/k-stream/k-stream/context"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"github.com/pickme-go/log"
	"time"
)

type Builder func(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Options) (Store, error)
type StateStoreBuilder func(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Options) StateStore

type Store interface {
	Name() string
	Backend() backend.Backend
	KeyEncoder() encoding.Encoder
	ValEncoder() encoding.Encoder
	Set(ctx context.Context, key interface{}, value interface{}, expiry time.Duration) error
	Get(ctx context.Context, key interface{}) (value interface{}, err error)
	GetRange(ctx context.Context, fromKey interface{}, toKey interface{}) (map[interface{}]interface{}, error)
	GetAll(ctx context.Context) (Iterator, error)
	Delete(ctx context.Context, key interface{}) error
	String() string
}

type StateStore interface {
	Name() string
	Set(key interface{}, value interface{}) error
	Get(key interface{}) (value interface{}, err error)
	GetAll() ([]*data.Record, error)
}

type store struct {
	backend    backend.Backend
	name       string
	logger     log.Logger
	keyEncoder encoding.Encoder
	valEncoder encoding.Encoder
	changelog  changelog.Changelog
}

func NewStore(name string, keyEncoder encoding.Encoder, valEncoder encoding.Encoder, logger log.Logger, options ...Options) (Store, error) {

	opts := new(storeOptions)
	opts.apply(options...)

	if opts.backend == nil {
		backend, err := opts.backendBuilder(name)
		if err != nil {
			logger.Fatal(`k-stream.Store.Registry`, fmt.Sprintf(`backend builder error - %+v`, err))
		}
		opts.backend = backend
	}

	store := &store{
		name:       name,
		keyEncoder: keyEncoder,
		logger:     logger,
		valEncoder: valEncoder,
		backend:    opts.backend,
	}

	store.backend.SetExpiry(opts.expiry)

	if opts.changelogEnable {
		panic(`not yest implemented`)
		/*p, err := producer.DefaultBuilder(&producer.Option{
			Partitioner: producer.Random,
		})
		if err != nil {
			return nil, err
		}

		topic := name + `_store`
		cLog, err := changelog.DefaultBuilder(name, topic, -1, changelog.Producer(p))
		opts.changelog = cLog

		return &recoverableStore{
			Store:     store,
			changelog: cLog,
		}, nil

		store.backend.SetExpiry(opts.expiry)*/
	}

	logger.Info(
		fmt.Sprintf(`default store [%s] inited`, name))

	return store, nil
}

func (s *store) Name() string {
	return s.name
}

func (s *store) String() string {
	return fmt.Sprintf(`Backend: %s\nChangelogInfo: %s`, s.Backend().Name(), s.changelog)
}

func (s *store) KeyEncoder() encoding.Encoder {
	return s.keyEncoder
}

func (s *store) ValEncoder() encoding.Encoder {
	return s.valEncoder
}

func (s *store) Backend() backend.Backend {
	return s.backend
}

func (s *store) Set(ctx context.Context, key interface{}, value interface{}, expiry time.Duration) error {

	k, err := s.keyEncoder.Encode(key)
	if err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`store [%s] key encode error`, s.name))
	}

	// if value is null remove from store (tombstone)
	if value == nil {
		return s.backend.Delete(k)
	}

	v, err := s.valEncoder.Encode(value)
	if err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`store [%s] key encode err `, s.name))
	}

	// if changelog enable write record to the changelog
	if s.changelog != nil {
		record, err := kContext.RecordFromContext(ctx, k, v)
		if err != nil {
			return err
		}

		if err := s.changelog.Put(ctx, record); err != nil {
			return err
		}
	}

	return s.backend.Set(k, v, expiry)
}

func (s *store) Get(ctx context.Context, key interface{}) (value interface{}, err error) {

	k, err := s.keyEncoder.Encode(key)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`store [%s] key encode err `, s.name))
	}

	byt, err := s.backend.Get(k)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`store [%s] value dose not exist `, s.name))
	}

	if len(byt) < 1 {
		return nil, nil
	}

	v, err := s.valEncoder.Decode(byt)
	if err != nil {
		return nil, errors.WithPrevious(err, fmt.Sprintf(`store [%s] value decode err `, s.name))
	}

	return v, nil
}

func (s *store) GetRange(ctx context.Context, fromKey interface{}, toKey interface{}) (map[interface{}]interface{}, error) {
	i := s.backend.Iterator()
	i.SeekToFirst()

	vals := make(map[interface{}]interface{})

	for i.Valid() {
		if i.Error() != nil {
			return nil, errors.WithPrevious(i.Error(), fmt.Sprintf(`store [%s] backend key iterator error `, s.name))
		}

		k, err := s.keyEncoder.Decode(i.Key())
		if err != nil {
			return nil, errors.WithPrevious(err, fmt.Sprintf(`store [%s] value decode err `, s.name))
		}

		if len(i.Value()) < 1 {
			vals[k] = nil
			i.Next()
		}

		v, err := s.valEncoder.Decode(i.Value())
		if err != nil {
			return nil, errors.WithPrevious(err, fmt.Sprintf(`store [%s] value decode err `, s.name))
		}

		vals[k] = v
		i.Next()
	}

	return vals, nil
}

func (s *store) GetAll(ctx context.Context) (Iterator, error) {

	i := s.backend.Iterator()
	i.SeekToFirst()

	return &iterator{
		iterator:   i,
		keyEncoder: s.keyEncoder,
		valEncoder: s.valEncoder,
	}, nil

	/*i := s.backend.Iterator()
	i.SeekToFirst()

	values := make(map[interface{}]interface{}, 0)

	for i.Valid() {
		if i.Error() != nil {
			return nil, errors.New( fmt.Sprintf(`store [%s] backend key iterator error `, s.name), i.Error())
		}

		k, err := s.keyEncoder.Decode(i.Key())
		if err != nil && len(i.Key()) > 0 {
			log.Error(log.WithPrefix( fmt.Sprintf(`store [%s] key decode err due to %+v`, s.name, err)))
			i.Next()
			continue
			//return nil, errors.New( `value decode err `, err)
		}

		if len(i.Value()) < 1 {
			i.Next()
			continue
		}

		v, err := s.valEncoder.Decode(i.Value())
		if err != nil {
			return nil, errors.New( fmt.Sprintf(`store [%s] value decode err `, s.name), err)
		}

		values[k] = v

		i.Next()
	}

	return values, nil*/
}

func (s *store) Delete(ctx context.Context, key interface{}) (err error) {
	k, err := s.keyEncoder.Encode(key)
	if err != nil {
		return errors.WithPrevious(err, fmt.Sprintf(`store [%s] key encode err `, s.name))
	}

	// if changelog enable delete record from changelog
	if s.changelog != nil {
		record, err := kContext.RecordFromContext(ctx, k, nil)
		if err != nil {
			return err
		}

		if err := s.changelog.Delete(ctx, record); err != nil {
			return err
		}
	}

	return s.backend.Delete(k)
}
