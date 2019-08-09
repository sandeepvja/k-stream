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

	//var branches []BranchNodeBuilder
	//
	//for i := 1; i <= branche; i++ {
	//
	//	value := fmt.Sprintf(`br_%d`, i)
	//
	//	br := BranchNodeBuilder{
	//		Name: value,
	//	}
	//
	//	predicate := &Predicate{
	//		name: value,
	//		Func: func(ctx context.Context, key interface{}, val interface{}) (bl bool, e error) {
	//			_, ok := key.(int)
	//			isTrue := ok && val.(string) == value
	//
	//			return isTrue, nil
	//		},
	//	}
	//
	//	br.Nodes = append(br.Nodes, predicate)
	//
	//	for j := 1; j <= node; j++ {
	//		n := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	//
	//			if vIn.(string) != value {
	//				log.Fatal(`wrong type`, value, vIn.(string))
	//			}
	//
	//			return kIn.(int) * 2, vIn, true, nil
	//		}, false)
	//
	//		br.Nodes = append(br.Nodes, n)
	//	}
	//
	//	branches = append(branches, br)
	//}
	//
	//brBuilder := BranchBuilder{
	//	name:     `bb1`,
	//	Branches: branches,
	//}
	//
	//source := &mockSourceBuilder{&mockSource{
	//	func(ctx context.Context, kIn, vIn []byte) (kOut, vOut interface{}, err error) {
	//		k, e := strconv.Atoi(string(kIn))
	//		if e != nil {
	//			return nil, nil, e
	//		}
	//
	//		return k, string(vIn), nil
	//	},
	//}}
	//
	//b := new(TopologyBuilder)
	//b.Source = source
	//b.Builders = []NodeBuilder{brBuilder}
	//
	//return b

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
