package changelog

import (
	"encoding/binary"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/store_backend"
	"sync"
)

type cacheManager struct {
	caches             map[string]*cache
	mu                 *sync.Mutex
	backendBuilder     store_backend.Builder
	cacheOffsetStorage store_backend.Backend
}

func newCacheManager(backendBuilder store_backend.Builder) (*cacheManager, error) {
	m := &cacheManager{
		caches:         make(map[string]*cache),
		mu:             new(sync.Mutex),
		backendBuilder: backendBuilder,
	}

	offsetBackend, err := backendBuilder(`__changelog_cache_offsets`)
	if err != nil {
		return nil, err
	}

	if !offsetBackend.Persistent() {
		return nil, errors.New(`k-stream.changelog.replicaManager`, `only persistent backend are supported`)
	}

	m.cacheOffsetStorage = offsetBackend
	return m, nil
}

func (m *cacheManager) getCache(tp consumer.TopicPartition) (*cache, error) {
	if c, ok := m.caches[tp.String()]; ok {
		return c, nil
	}

	b, err := m.backendBuilder(`__changelog_cache_` + tp.String())
	if err != nil {
		return nil, err
	}

	cache := new(cache)
	cache.tp = tp
	cache.backend = b
	cache.offsetBackend = m.cacheOffsetStorage

	m.mu.Lock()
	m.caches[tp.String()] = cache
	m.mu.Unlock()

	return cache, nil
}

type cache struct {
	backend       store_backend.Backend
	offsetBackend store_backend.Backend
	tp            consumer.TopicPartition
}

func (c *cache) Flush() error {
	itr := c.backend.Iterator()
	for itr.Valid() {
		if err := c.backend.Delete(itr.Key()); err != nil {
			return errors.WithPrevious(err, `k-stream.changelog.cache`, `cache flush failed`)
		}
		itr.Next()
	}
	return nil
}

func (c *cache) Put(record *consumer.Record) error {

	if len(record.Value) < 1 {
		if err := c.backend.Delete(record.Key); err != nil {
			return err
		}
	} else {
		if err := c.backend.Set(record.Key, record.Value, 0); err != nil {
			return err
		}
	}
	// update current offset on backend
	return c.offsetBackend.Set([]byte(c.offsetKeyPrefix()), c.encodeOffset(record.Offset), 0)

}

func (c *cache) offsetKeyPrefix() string {
	return `__changelog_offset_cache_last_synced_` + c.tp.String()
}

func (c *cache) ReadAll() []*consumer.Record {
	var records []*consumer.Record

	i := c.backend.Iterator()
	i.SeekToFirst()
	for i.Valid() {
		record := &consumer.Record{
			Key:       i.Key(),
			Value:     i.Value(),
			Topic:     c.tp.Topic,
			Partition: c.tp.Partition,
		}
		records = append(records, record)
		i.Next()
	}

	return records
}

func (c *cache) Delete(record *consumer.Record) error {
	return c.backend.Delete(record.Key)
}

func (c *cache) decodeOffset(offset []byte) int64 {
	return int64(binary.LittleEndian.Uint64(offset))
}

func (c *cache) encodeOffset(offset int64) []byte {
	byt := make([]byte, 8)
	binary.LittleEndian.PutUint64(byt, uint64(offset))

	return byt
}

func (c *cache) LastSynced() (int64, error) {
	byt, err := c.offsetBackend.Get([]byte(c.offsetKeyPrefix()))
	if err != nil {
		return 0, err
	}

	if len(byt) < 1 {
		return 0, nil
	}

	return c.decodeOffset(byt), nil
}

func (c *cache) stop() error {
	//return c.backend.Close()
	return nil
}
