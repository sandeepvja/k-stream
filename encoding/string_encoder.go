package encoding

import (
	"fmt"
	"github.com/pickme-go/errors"
	"reflect"
)

type StringEncoder struct{}

func (s StringEncoder) Encode(v interface{}) ([]byte, error) {
	str, ok := v.(string)
	if !ok {
		return nil, errors.New(`encoding`, fmt.Sprintf(`invalid type [%+v] expected string`, reflect.TypeOf(v)))
	}

	return []byte(str), nil
}

func (s StringEncoder) Decode(data []byte) (interface{}, error) {
	return string(data), nil
}
