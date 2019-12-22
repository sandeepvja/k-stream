package store

import (
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkAssociation_Write(b *testing.B) {
	assoc := NewAssociation(NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)), func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := assoc.Write(strconv.Itoa(rand.Intn(100000)+1), `111,222`); err != nil {
				b.Error(err)
			}
		}

	})
}

func BenchmarkAssociation_Read(b *testing.B) {
	assoc := NewAssociation(NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)), func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	for i := 1; i < 1000; i++ {
		if err := assoc.Write(strconv.Itoa(i), `111,222`); err != nil {
			b.Error(err)
		}
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := assoc.Read(`111`); err != nil {
				b.Error(err)
			}
		}

	})
}
