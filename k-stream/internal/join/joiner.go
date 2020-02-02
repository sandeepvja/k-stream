package join

import (
	"context"
	"github.com/pickme-go/k-stream/k-stream/internal/node"
)

type Type int

const (
	LeftJoin Type = iota
	InnerJoin
)

type Joiner interface {
	node.Node
	Join(ctx context.Context, key, val interface{}) (joinedVal interface{}, err error)
}

type KeyMapper func(key, value interface{}) (mappedKey interface{}, err error)

type ValueMapper func(left, right interface{}) (joined interface{}, err error)
