package node

import (
	"context"
	"log"
)

type mockNode struct {
	fn   func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error)
	next bool
}

func (n *mockNode) Childs() []Node {
	panic("implement me")
}

func (n *mockNode) AddChild(node Node) {
	panic("implement me")
}

func (n *mockNode) Next() bool {
	return n.next
}

func (n *mockNode) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	return n.fn(ctx, kIn, vIn)
}

func (n *mockNode) Type() Type {
	return Type(`mockNode`)
}

type mockNodeBuilder struct {
	node *mockNode
}

func (b *mockNodeBuilder) ChildBuilders() []NodeBuilder {
	panic("implement me")
}

func (b *mockNodeBuilder) AddChildBuilder(builder NodeBuilder) {
	panic("implement me")
}

func newMockBuilder(criteria func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error), next bool) NodeBuilder {
	return &mockNodeBuilder{
		node: &mockNode{fn: criteria, next: next},
	}
	return nil
}

func (b *mockNodeBuilder) Build() (Node, error) {
	return b.node, nil
}

func (*mockNodeBuilder) Type() Type {
	return Type(`mockNode`)
}

func (*mockNodeBuilder) Next() bool {
	return true
}

func NewMockTopologyBuilder(branche int, node int) *TopologyBuilder {
	return nil

}

func NewMockTopology(branche int, node int) Topology {

	b := NewMockTopologyBuilder(branche, node)
	tp, err := b.Build()
	if err != nil {
		log.Fatal(err)
	}

	return tp

}
