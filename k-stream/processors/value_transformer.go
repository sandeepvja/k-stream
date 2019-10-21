package processors

import (
	"context"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/k-stream/internal/node"
)

type ValueTransformFunc func(ctx context.Context, key, value interface{}) (vOut interface{}, err error)

type ValueTransformer struct {
	Id                 int32
	ValueTransformFunc ValueTransformFunc
	childBuilders      []node.NodeBuilder
	childs             []node.Node
}

func (vt *ValueTransformer) Build() (node.Node, error) {
	var childs []node.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range vt.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &ValueTransformer{
		ValueTransformFunc: vt.ValueTransformFunc,
		childs:             childs,
		Id:                 vt.Id,
	}, nil
}

func (vt *ValueTransformer) ChildBuilders() []node.NodeBuilder {
	return vt.childBuilders
}

func (vt *ValueTransformer) AddChildBuilder(builder node.NodeBuilder) {
	vt.childBuilders = append(vt.childBuilders, builder)
}

func (vt *ValueTransformer) Next() bool {
	return true
}

func (vt *ValueTransformer) ID() int32 {
	return vt.Id
}

func (vt *ValueTransformer) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	v, err := vt.ValueTransformFunc(ctx, kIn, vIn)
	if err != nil {
		return nil, nil, false, errors.WithPrevious(err, `error in value transform function`)
	}

	for _, child := range vt.childs {
		_, _, next, err := child.Run(ctx, kIn, v)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}

	return kIn, v, true, err
}

func (vt *ValueTransformer) Type() node.Type {
	return node.Type(`value_transformer`)
}

func (vt *ValueTransformer) Childs() []node.Node {
	return vt.childs
}

func (vt *ValueTransformer) AddChild(node node.Node) {
	vt.childs = append(vt.childs, node)
}
