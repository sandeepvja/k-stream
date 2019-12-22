package store

import (
	"context"
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"reflect"
	"strings"
	"testing"
)

func Test_indexedStore_Delete(t *testing.T) {
	assoc := NewAssociation(NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)), func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	i := &associationStore{
		Store:        assoc.Store(),
		associations: map[string]Association{`foo`: assoc},
	}

	if err := i.Set(context.Background(), `200`, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := i.Set(context.Background(), `300`, `111,333`, 0); err != nil {
		t.Error(err)
	}

	if err := i.Delete(context.Background(), `200`); err != nil {
		t.Error(err)
	}

	data, err := assoc.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, []string{`300`}) {
		t.Errorf(`want []string{300}, have %#v`, data)
	}
}

func Test_indexedStore_Set(t *testing.T) {
	assoc := NewAssociation(NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)), func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	i := &associationStore{
		Store:        assoc.Store(),
		associations: map[string]Association{`foo`: assoc},
	}

	if err := i.Set(context.Background(), `200`, `111,222`, 0); err != nil {
		t.Error(err)
	}

	if err := i.Set(context.Background(), `300`, `111,333`, 0); err != nil {
		t.Error(err)
	}

	data, err := assoc.Read(`111`)
	if err != nil {
		t.Error(data)
	}

	if !reflect.DeepEqual(data, []string{`200`, `300`}) {
		t.Errorf("want []string{`200`, `300`}, have %#v", data)
	}
}
