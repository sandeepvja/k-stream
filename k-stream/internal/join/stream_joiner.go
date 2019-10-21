package join

import (
	"context"
	"github.com/pickme-go/k-stream/k-stream/internal/node"
)

type StreamJoiner struct {
	Id            int32
	childs        []node.Node
	childBuilders []node.NodeBuilder
}

func (j *StreamJoiner) Build() (node.Node, error) {
	var childs []node.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range j.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &StreamJoiner{
		childs: childs,
		Id:     j.Id,
	}, nil
}

func (j *StreamJoiner) ChildBuilders() []node.NodeBuilder {
	return j.childBuilders
}

func (j *StreamJoiner) AddChildBuilder(builder node.NodeBuilder) {
	j.childBuilders = append(j.childBuilders, builder)
}

func (j *StreamJoiner) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	for _, child := range j.childs {
		_, _, _, err := child.Run(ctx, kIn, vIn)
		if err != nil {
			return nil, nil, false, err
		}
	}
	return kIn, vIn, true, nil
}

func (j *StreamJoiner) Childs() []node.Node {
	return j.childs
}

func (j *StreamJoiner) AddChild(node node.Node) {
	j.childs = append(j.childs, node)
}

func (j *StreamJoiner) Next() bool {
	return true
}

func (j *StreamJoiner) Type() node.Type {
	return node.Type(`stream_joiner`)
}

func (j *StreamJoiner) Name() string {
	return `stream_joiner`
}

func (j *StreamJoiner) ID() int32 {
	return j.Id
}

//type StreamJoinEncoder struct {
//	typ reflect.Type
//}
//
//func (s *StreamJoinEncoder) Encode(data interface{}) ([]byte, error) {
//	s.typ =  reflect.TypeOf(data)
//	var buf bytes.Buffer
//	enc := gob.NewEncoder(&buf)
//	err := enc.Encode(data)
//	if err != nil {
//		return nil, err
//	}
//	return buf.Bytes(), nil
//}
//
//func (s *StreamJoinEncoder) Decode(data []byte) (interface{}, error) {
//	decoded := reflect.New(s.typ)
//	buf := bytes.NewBuffer(data)
//	dec := gob.NewDecoder(buf)
//	err := dec.Decode(decoded)
//	if err != nil {
//		return decoded.Interface(),err
//	}
//	return decoded.Interface(), nil
//}
