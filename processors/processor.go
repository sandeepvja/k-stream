/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package processors

import (
	"context"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/internal/node"
)

type ProcessFunc func(ctx context.Context, key, value interface{}) error

type Processor struct {
	Id            int32
	ProcessFunc   ProcessFunc
	childBuilders []node.NodeBuilder
	childs        []node.Node
}

func (p *Processor) Childs() []node.Node {
	return p.childs
}

func (p *Processor) ChildBuilders() []node.NodeBuilder {
	return p.childBuilders
}

func (p *Processor) AddChildBuilder(builder node.NodeBuilder) {
	p.childBuilders = append(p.childBuilders, builder)
}

func (p *Processor) AddChild(node node.Node) {
	p.childs = append(p.childs, node)
}

func (p *Processor) Run(ctx context.Context, kIn, vIn interface{}) (interface{}, interface{}, bool, error) {
	err := p.ProcessFunc(ctx, kIn, vIn)
	if err != nil {
		return kIn, vIn, false, errors.WithPrevious(err, `k-stream.processors.Processor`, `process error`)
	}

	for _, child := range p.childs {
		_, _, next, err := child.Run(ctx, kIn, vIn)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}

	return kIn, vIn, true, nil
}

func (p *Processor) Build() (node.Node, error) {
	var childs []node.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range p.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &Processor{
		ProcessFunc: p.ProcessFunc,
		childs:      childs,
		Id:          p.Id,
	}, nil
}

func (p *Processor) Next() bool {
	return true
}

func (p *Processor) Name() string {
	return `processor`
}

func (p *Processor) Type() node.Type {
	return node.Type(`processor`)
}

func (p *Processor) ID() int32 {
	return p.Id
}
