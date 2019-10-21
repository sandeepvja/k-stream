package node

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkBranchBuilder_Node(b *testing.B) {

	node1 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		return kIn.(int) * 2, vIn.(string), true, nil
	}, true)
	node2 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		return kIn.(int) * 2, vIn.(string), true, nil
	}, true)

	tp, err := build(intEnc, []NodeBuilder{node1, node2})
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()

	_, _, err = tp.Run(context.Background(), []byte(`100`), []byte(`test`))
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkBranchBuilder_Plain(b *testing.B) {

	node1 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		return kIn.(int) * 2, vIn.(string), true, nil
	}, true)
	node2 := newMockBuilder(func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
		return kIn.(int) * 2, vIn.(string), true, nil
	}, true)

	_, err := build(intEnc, []NodeBuilder{node1, node2})
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()

	_, _, err = func(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, err error) {
		return kIn.(int) * 2, vIn.(string), nil
	}(context.Background(), 100, `test`)

	if err != nil {

	}
}

var tp Topology

func init() {
	tp = NewMockTopology(20, 10)
}

func BenchmarkTestTopologyBuilder_Branch(b *testing.B) {

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			//k := rand.Intn(8)+1
			k := 3
			//k = 1
			_, _, err := tp.Run(context.Background(), []byte(`200`), []byte(fmt.Sprintf(`br_%d`, k)))
			if err != nil {
				b.Error(err)
			}
		}
	})

}
