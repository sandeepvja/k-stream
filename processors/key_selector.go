package processors

import (
	"context"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/internal/node"
)

type SelectKeyFunc func(ctx context.Context, key, value interface{}) (kOut interface{}, err error)

type KeySelector struct {
	Id            int32
	SelectKeyFunc SelectKeyFunc
	childBuilders []node.NodeBuilder
	childs        []node.Node
}

func (ks *KeySelector) Build() (node.Node, error) {
	var childs []node.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range ks.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &KeySelector{
		SelectKeyFunc: ks.SelectKeyFunc,
		childs:        childs,
		Id:            ks.Id,
	}, nil
}

func (ks *KeySelector) ChildBuilders() []node.NodeBuilder {
	return ks.childBuilders
}

func (ks *KeySelector) AddChildBuilder(builder node.NodeBuilder) {
	ks.childBuilders = append(ks.childBuilders, builder)
}

func (ks *KeySelector) Next() bool {
	return true
}

func (ks *KeySelector) ID() int32 {
	return ks.Id
}

func (ks *KeySelector) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	k, err := ks.SelectKeyFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `k-stream.processors.key_selector`, `error in select key function`)
	}

	for _, child := range ks.childs {
		_, _, next, err := child.Run(ctx, k, vIn)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}

	return k, vOut, true, err
}

func (ks *KeySelector) Type() node.Type {
	return node.Type(`key_selector`)
}

func (ks *KeySelector) Childs() []node.Node {
	return ks.childs
}

func (ks *KeySelector) AddChild(node node.Node) {
	ks.childs = append(ks.childs, node)
}
