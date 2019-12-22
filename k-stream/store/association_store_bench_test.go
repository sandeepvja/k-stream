package store

import (
	"context"
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkIndexedStore_Set(b *testing.B) {
	assoc := NewAssociation(NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)), func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	i := &associationStore{
		Store:        assoc.Store(),
		associations: map[string]Association{`foo`: assoc},
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
