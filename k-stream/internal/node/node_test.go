package node

import (
	"context"
	"strconv"
	"testing"
)

var intEnc = &mockSourceBuilder{&mockSource{
	func(ctx context.Context, kIn, vIn []byte) (kOut, vOut interface{}, err error) {
		k, e := strconv.Atoi(string(kIn))
		if e != nil {
			return nil, nil, e
		}

		return k, string(vIn), nil
	},
}}

var stringEnc = mockSourceBuilder{mockSource{
	func(ctx context.Context, kIn, vIn []byte) (kOut, vOut interface{}, err error) {
		return string(kIn), string(vIn), nil
	},
}}

func build(source SourceBuilder, nodes []NodeBuilder) (Topology, error) {
	b := new(TopologyBuilder)
	b.Source = source
	b.SourceNodeBuilder = nodes
	topology, err := b.Build()
	if err != nil {
		return Topology{}, err
	}

	return topology, nil
}

func TestTopologyBuilder_Build(t *testing.T) {

	node1 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		return kIn.(int) * 2, vIn.(string), true, nil
	}, true)
	node2 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		return kIn.(int) * 2, vIn.(string), true, nil
	}, true)

	tp, err := build(intEnc, []NodeBuilder{node1, node2})
	if err != nil {
		t.Error(err)
	}

	k, v, err := tp.Run(context.Background(), []byte(`100`), []byte(`test`))
	if err != nil {
		t.Error(err)
	}

	if k.(int) != 400 || v.(string) != `test` {
		t.Fail()
	}

}

func TestTopologyBuilder_ContinueToNext(t *testing.T) {

	node1 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		return kIn.(int) * 2, vIn.(int) * 2, true, nil
	}, false)
	node2 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		return kIn.(int) * 2, vIn.(int) * 2, true, nil
	}, true)

	tp, err := build(&mockSourceBuilder{}, []NodeBuilder{node1, node2})
	if err != nil {
		t.Error(err)
	}

	k, v, err := tp.Run(context.Background(), []byte(`100`), []byte(`100`))
	if err != nil {
		t.Error(err)
	}

	if k.(int) != 200 || v.(int) != 200 {
		t.Fail()
	}

}

func TestTopologyBuilder_Branch(t *testing.T) {

	nodeB1_1 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {

		if vIn.(string) != `br1` {
			t.FailNow()
		}

		return kIn.(int) * 2, vIn, true, nil
	}, true)

	predicate1 := &Predicate{
		Func: func(ctx context.Context, key interface{}, val interface{}) (b bool, e error) {
			_, kOk := key.(int)
			return kOk && val.(string) == `br1`, nil
		},
	}

	predicate2 := &Predicate{
		Func: func(ctx context.Context, key interface{}, val interface{}) (b bool, e error) {
			_, kOk := key.(int)
			return kOk && val.(string) == `br2`, nil
		},
	}

	nodeB1_2 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		if vIn.(string) != `br1` {
			t.FailNow()
		}

		return kIn.(int) * 2, vIn, true, nil
	}, true)

	nodeB2_1 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		if vIn.(string) != `br2` {
			t.FailNow()
		}
		return kIn.(int) * 2, vIn, true, nil
	}, true)
	nodeB2_2 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		if vIn.(string) != `br2` {
			t.FailNow()
		}
		return kIn.(int) * 2, vIn, true, nil
	}, true)

	branch1 := BranchNodeBuilder{
		Name:  `branch1`,
		Nodes: []NodeBuilder{predicate1, nodeB1_1, nodeB1_2},
	}

	branch2 := BranchNodeBuilder{
		Name:  `branch2`,
		Nodes: []NodeBuilder{predicate2, nodeB2_1, nodeB2_2},
	}

	brBuilder := &BranchBuilder{
		name:     `bb1`,
		Branches: []BranchNodeBuilder{branch1, branch2},
	}

	tp, err := build(intEnc, []NodeBuilder{brBuilder})
	if err != nil {
		t.Error(err)
	}

	_, _, err = tp.Run(context.Background(), []byte(`200`), []byte(`br1`))
	if err != nil {
		t.Error(err)
	}

}
