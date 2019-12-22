package store

import (
	"github.com/pickme-go/k-stream/backend"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"reflect"
	"strings"
	"testing"
)

func TestNewAssociation(t *testing.T) {
	store := NewMockStore(`test`, nil, nil, backend.NewMockBackend(`test`, 0))
	var mapper func(key, val interface{}) (idx string)
	assc := NewAssociation(store, mapper)
	type args struct {
		store  Store
		mapper KeyMapper
	}
	tests := []struct {
		name string
		args args
		want Association
	}{
		{name: `new`, args: struct {
			store  Store
			mapper KeyMapper
		}{store: store, mapper: mapper}, want: assc},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewAssociation(tt.args.store, tt.args.mapper); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewAssociation() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_association_Delete(t *testing.T) {
	assoc := NewAssociation(NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)), func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	if err := assoc.Write(`100`, `111,222`); err != nil {
		t.Error(err)
	}

	if err := assoc.Delete(`100`, `111,222`); err != nil {
		t.Error(err)
	}

	data, err := assoc.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if len(data) > 0 {
		t.Fail()
	}
}

func Test_association_Name(t *testing.T) {
	tests := []struct {
		name  string
		assoc Association
		want  string
	}{
		{
			name: `name`,
			assoc: NewAssociation(
				NewMockStore(`test`, nil, nil, backend.NewMockBackend(`test`, 0)), nil),
			want: `test`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.assoc.Name(); got != tt.want {
				t.Errorf("Name() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_association_Read(t *testing.T) {
	assoc := NewAssociation(NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)), func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	if err := assoc.Write(`100`, `111,222`); err != nil {
		t.Error(err)
	}

	data, err := assoc.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, []string{`100`}) {
		t.Errorf("expect []interface{}{`111,222`} have %#v", data)
	}
}

func Test_association_Write(t *testing.T) {
	assoc := NewAssociation(NewMockStore(`foo`, encoding.StringEncoder{}, encoding.StringEncoder{}, backend.NewMockBackend(`foo`, 0)), func(key, val interface{}) (idx string) {
		return strings.Split(val.(string), `,`)[0]
	})

	if err := assoc.Write(`100`, `111,222`); err != nil {
		t.Error(err)
	}

	data, err := assoc.Read(`111`)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, []string{`100`}) {
		t.Errorf("expect []interface{}{`111,222`} have %#v", data)
	}
}
