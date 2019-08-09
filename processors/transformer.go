package processors

import (
	"context"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/internal/node"
)

type TransFunc func(ctx context.Context, key, value interface{}) (kOut, vOut interface{}, err error)

type Transformer struct {
	Id            int32
	TransFunc     TransFunc
	childBuilders []node.NodeBuilder
	childs        []node.Node
}

func (t *Transformer) Childs() []node.Node {
	return t.childs
}

func (t *Transformer) ChildBuilders() []node.NodeBuilder {
	return t.childBuilders
}

func (t *Transformer) Build() (node.Node, error) {
	var childs []node.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range t.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &Transformer{
		TransFunc: t.TransFunc,
		childs:    childs,
		Id:        t.Id,
	}, nil
}

func (t *Transformer) Next() bool {
	return true
}

func (t *Transformer) ID() int32 {
	return t.Id
}

func (t *Transformer) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	k, v, err := t.TransFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `k-stream.processors.Transformer`, `transformer error`)
	}

	for _, child := range t.childs {
		_, _, next, err := child.Run(ctx, k, v)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}

	return k, v, true, err
}

func (t *Transformer) Type() node.Type {
	return node.Type(`transformer`)
}

func (t *Transformer) Name() string {
	return `transformer`
}

func (t *Transformer) AddChildBuilder(builder node.NodeBuilder) {
	t.childBuilders = append(t.childBuilders, builder)
}

func (t *Transformer) AddChild(node node.Node) {
	t.childs = append(t.childs, node)
}
