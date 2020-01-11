package store

import (
	"context"
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/backend/memory"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"github.com/pickme-go/log/v2"
	"github.com/pickme-go/metrics/v2"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func BenchmarkIndexedStore_Set(b *testing.B) {
	assoc := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	i := &indexedStore{
		Store:   NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)),
		indexes: map[string]Index{`foo`: assoc},
		mu:      new(sync.Mutex),
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := i.Set(context.Background(), strconv.Itoa(rand.Intn(99999)+1), `111,222`, 0); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkIndexedStore_GetIndexedRecords(b *testing.B) {
	assocStore := NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0))
	for i := 1; i < 99909; i++ {
		compKey := strconv.Itoa(rand.Intn(4)+1) + `:` + strconv.Itoa(i)
		if err := assocStore.Set(context.Background(), strconv.Itoa(i), compKey, 0); err != nil {
			b.Error(err)
		}
	}

	assoc := NewStringHashIndex(`foo`, func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `:`)[0]
	})

	st, err := NewIndexedStore(
		`foo`,
		encoding.StringEncoder{},
		encoding.StringEncoder{},
		[]Index{assoc},
		WithBackend(memory.NewMemoryBackend(log.NewNoopLogger(), metrics.NoopReporter())))
	if err != nil {
		b.Error(err)
	}

	for i := 1; i < 99909; i++ {
		compKey := strconv.Itoa(rand.Intn(4)+1) + `:` + strconv.Itoa(i)
		if err := st.Set(context.Background(), strconv.Itoa(i), compKey, 0); err != nil {
			b.Error(err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := st.GetIndexedRecords(context.Background(), `foo`, strconv.Itoa(rand.Intn(4)+1)); err != nil {
				b.Error(err)
			}
		}
	})
}
