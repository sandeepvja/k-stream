package join

import (
	"context"
	"fmt"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/k-stream/internal/node"
	"github.com/pickme-go/k-stream/k-stream/store"
)

type GlobalTableJoiner struct {
	//Topic string
	Id            int32
	Typ           JoinType
	Store         string
	KeyMapper     KeyMapper
	ValueMapper   ValueMapper
	store         store.Store
	Registry      store.Registry
	childBuilders []node.NodeBuilder
	childs        []node.Node
}

func (j *GlobalTableJoiner) ChildBuilders() []node.NodeBuilder {
	return j.childBuilders
}

func (j *GlobalTableJoiner) Childs() []node.Node {
	return j.childs
}

func (j *GlobalTableJoiner) AddChildBuilder(builder node.NodeBuilder) {
	j.childBuilders = append(j.childBuilders, builder)
}

func (j *GlobalTableJoiner) AddChild(node node.Node) {
	j.childs = append(j.childs, node)
}

func (j *GlobalTableJoiner) Next() bool {
	return true
}

func (j *GlobalTableJoiner) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, next bool, err error) {
	v, err := j.Join(ctx, kIn, vIn)

	for _, child := range j.childs {
		_, _, next, err := child.Run(ctx, kIn, v)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}
	return kIn, v, true, err
}

func (j *GlobalTableJoiner) Type() node.Type {
	return node.TypeJoiner
}

func (j *GlobalTableJoiner) Build() (node.Node, error) { //TODO: write new build
	j.store = j.Registry.Store(j.Store)
	if j.store == nil {
		return nil, errors.New(`store [` + j.Store + `] dose not exist`)
	}

	var childs []node.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range j.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &GlobalTableJoiner{
		Id:            j.Id,
		Typ:           j.Typ,
		Store:         j.Store,
		KeyMapper:     j.KeyMapper,
		ValueMapper:  j.ValueMapper,
		store:         j.store,
		Registry:      j.Registry,
		childs:        childs,
	}, nil
}

func (j *GlobalTableJoiner) Join(ctx context.Context, key interface{}, leftVal interface{}) (joinedVal interface{}, err error) {

	// get key from key mapper
	k, err := j.KeyMapper(key, leftVal)
	if err != nil {
		return nil, errors.WithPrevious(err, `KeyMapper error`)
	}

	// get value from store
	rightValue, err := j.store.Get(ctx, k)
	if err != nil {
		return nil, errors.WithPrevious(err,
			fmt.Sprintf(`cannot get value from [%s] store`, j.Store))
	}

	// for InnerJoin joins if right side lookup nil ignore the join
	if j.Typ == InnerJoin && rightValue == nil {
		return nil, errors.New(
			fmt.Sprintf(`right value lookup failed due to [key [%+v] dose not exist in %s store]`, k, j.store.Name()))
	}

	// send LeftJoin value and right value to ValueJoiner and get the joined value
	valJoined, err := j.ValueMapper(leftVal, rightValue)
	if err != nil {
		return nil, errors.WithPrevious(err,
			`value mapper failed`)
	}

	return valJoined, nil

}

//func (j *GlobalTableJoiner) Process(ctx context.Context, key interface{}, value interface{}) (interface{}, interface{}, error) {
//	v, err := j.Join(ctx, key, value)
//	return key, v, err
//}

func (j *GlobalTableJoiner) Name() string {
	return j.Store
}

func (j *GlobalTableJoiner) ID() int32 {
	return j.Id
}

//func (j *GlobalTableJoiner) JoinType() string {
//	return `GlobalTableJoiner`
//}
