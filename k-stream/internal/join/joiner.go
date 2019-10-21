package join

import (
	"context"
	"github.com/pickme-go/k-stream/k-stream/internal/node"
)

type JoinType int

const (
	LeftJoin JoinType = iota
	InnerJoin
)

type Joiner interface {
	node.Node
	Join(ctx context.Context, key interface{}, val interface{}) (joinedVal interface{}, err error)
}

type KeyMapper func(key interface{}, value interface{}) (mappedKey interface{}, err error)

type ValueMapper func(left interface{}, right interface{}) (joined interface{}, err error)
