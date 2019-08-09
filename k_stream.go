/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package kstream

import (
	"context"
	"fmt"
	"github.com/pickme-go/k-stream/branch"
	"github.com/pickme-go/k-stream/encoding"
	"github.com/pickme-go/k-stream/internal/join"
	"github.com/pickme-go/k-stream/internal/node"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/processors"
	"sync/atomic"

	//"sync/atomic"
	"time"
)

var nodeCounter int32

type topic func(string) string

type Stream interface {
	Branch(branches []branch.BranchDetails, opts ...Option) []Stream
	//ParallelBranch(name string, predicate branch.BranchPredicate, opts ...Option) Stream
	SelectKey(selectKeyFunc processors.SelectKeyFunc) Stream
	TransformValue(valueTransformFunc processors.ValueTransformFunc) Stream
	Transform(transformer processors.TransFunc) Stream
	Filter(filter processors.FilterFunc) Stream
	Process(processor processors.ProcessFunc) Stream
	JoinGlobalTable(stream Stream, keyMapper join.KeyMapper, valMapper join.ValueMapper, typ join.JoinType) Stream
	JoinKTable(stream Stream, keyMapper join.KeyMapper, valMapper join.ValueMapper) Stream
	JoinStream(stream Stream, valMapper join.ValueMapper, opts ...RepartitionOption) Stream
	//LeftJoin(stream Stream, keyMapper join.KeyMapper, valMapper join.ValueMapper) Stream
	Through(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...SinkOption) Stream
	To(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...SinkOption)
}

type StreamConfigs map[string]interface{}

type kStreamConfig struct {
	processorRetryCount    int
	processorRetryInterval time.Duration
	dlq                    bool
	changelog              struct {
		enabled           bool
		replicated        bool // min number of in-sync replications in other nodes
		minInSycReplicas  int  // min number of in-sync replications in other nodes
		replicationFactor int
		suffix            string
		buffer            struct {
			enabled       bool
			size          int
			flushInterval time.Duration
		}
	}
	repartition RepartitionOptions
}

//type kStreamBranch struct {
//	name      string
//	predicate node.BranchPredicate
//	stream    *kStream
//	parallel  bool
//}

type kStream struct {
	rootStream *kStream
	source     *kSourceBuilder
	//nodes    []node.NodeBuilder
	topology *node.TopologyBuilder
	//branches []*kStreamBranch
	root        bool
	config      *kStreamConfig
	streams     []*kStream
	NodeBuilder node.NodeBuilder
	Node        node.Node
	keySelected bool
	topic       topic
}

func (c *kStreamConfig) apply(options ...Option) {

	// apply defaults
	c.processorRetryCount = 1
	c.processorRetryInterval = 0
	c.dlq = false
	c.changelog.enabled = false
	c.changelog.buffer.size = 10
	c.changelog.buffer.flushInterval = 1 * time.Second
	c.changelog.minInSycReplicas = 2
	c.changelog.replicationFactor = 3

	for _, opt := range options {
		opt(c)
	}

}

type Option func(*kStreamConfig)

func WithConfig(configs StreamConfigs) Option {
	return func(stream *kStreamConfig) {

		for p, value := range configs {

			switch p {
			case `stream.processor.retry`:
				if v, ok := value.(int); ok {
					stream.processorRetryCount = v
					continue
				}
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupported config type for [%s]`, p))

			case `stream.processor.retry.interval`:
				if v, ok := value.(int); ok {
					stream.processorRetryInterval = time.Duration(v) * time.Millisecond
					continue
				}
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupported config type for [%s]`, p))

			case `stream.processor.changelog.enabled`:
				if v, ok := value.(bool); ok {
					stream.changelog.enabled = v
					continue
				}
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupported config type for [%s]`, p))

			case `stream.processor.changelog.minInSyncReplicas`:
				if v, ok := value.(int); ok {
					stream.changelog.minInSycReplicas = v
					continue
				}
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupported config type for [%s]`, p))

			case `stream.processor.changelog.buffer.flushInterval`:
				if v, ok := value.(time.Duration); ok {
					stream.changelog.buffer.flushInterval = v
					continue
				}
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupported config type for [%s]`, p))

			case `stream.processor.changelog.buffer.enabled`:
				if v, ok := value.(bool); ok {
					stream.changelog.buffer.enabled = v
					continue
				}
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupported config type for [%s]`, p))

			case `stream.processor.changelog.buffer.size`:
				if v, ok := value.(int); ok {
					stream.changelog.buffer.size = v
					continue
				}
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupported config type for [%s]`, p))

			case `stream.processor.changelog.replicated`:
				if v, ok := value.(bool); ok {
					stream.changelog.replicated = v
					continue
				}
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupportxed config type for [%s]`, p))

			case `stream.processor.changelog.replicationFactor`:
				if v, ok := value.(int); ok {
					stream.changelog.replicationFactor = v
					continue
				}
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupported config type for [%s]`, p))

			case `stream.processor.dlq.enabled`:
				if v, ok := value.(bool); ok {
					stream.dlq = v
					continue
				}
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupported config type for [%s]`, p))

			default:
				logger.DefaultLogger.Fatal(`k-stream.kStream`,
					fmt.Sprintf(`unsupported config [%s]`, p))
			}
		}
	}
}

func NewKStream(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...Option) Stream {
	if keyEncoder == nil {
		logger.DefaultLogger.Fatal(`k-stream.kStream`, `keyEncoder cannot be null`)
	}

	if valEncoder == nil {
		logger.DefaultLogger.Fatal(`k-stream.kStream`, `valEncoder cannot be null`)
	}

	return newKStream(func(s string) string { return topic }, keyEncoder, valEncoder, nil, options...)
}

func newKStream(topic topic, keyEncoder encoding.Builder, valEncoder encoding.Builder, parent *kStream, options ...Option) *kStream {

	config := new(kStreamConfig)
	config.apply(options...)

	stream := &kStream{
		config: config,
	}

	if parent == nil {
		stream.root = true
		stream.rootStream = stream
		//setup source Node
		sourceNode := new(SourceNode)
		stream.Node = sourceNode
		stream.NodeBuilder = sourceNode
		//setup source builder
		kSource := &kSourceBuilder{
			topic:             topic(``),
			name:              topic(``),
			keyEncoderBuilder: keyEncoder,
			valEncoderBuilder: valEncoder,
			info: map[string]string{
				`topic`:     topic(``),
				`changelog`: fmt.Sprint(config.changelog.enabled),
			},
		}
		stream.topic = topic

		stream.source = kSource
	}

	return stream
}

func (s *kStream) Branch(branches []branch.BranchDetails, opts ...Option) []Stream {
	return s.branch(branches, false, opts...)
}

func (s *kStream) branch(branches []branch.BranchDetails, parallel bool, opts ...Option) []Stream {

	bs := &branch.BranchSplitter{
		Id: atomic.AddInt32(&nodeCounter, 1),
	}

	//id := atomic.AddUint32(&nodeCounter,1)

	var streams = make([]Stream, len(branches), len(branches))

	for i, br := range branches {
		b := &branch.Branch{
			Name:      br.Name,
			Predicate: br.Predicate,
			Id:        atomic.AddInt32(&nodeCounter, 1),
		}
		bs.AddChild(b)
		bs.AddChildBuilder(b)

		stream := newKStream(nil, nil, nil, s, opts...)
		stream.Node = b
		stream.NodeBuilder = b
		stream.rootStream = s.rootStream
		stream.keySelected = s.keySelected

		streams[i] = stream
	}

	s.Node.AddChild(bs)
	s.NodeBuilder.AddChildBuilder(bs)

	return streams
}

func (s *kStream) SelectKey(selectKeyFunc processors.SelectKeyFunc) Stream {
	sk := &processors.KeySelector{
		SelectKeyFunc: selectKeyFunc,
		Id:            atomic.AddInt32(&nodeCounter, 1),
	}

	s.Node.AddChild(sk)
	s.NodeBuilder.AddChildBuilder(sk)

	keySelected := newKStream(nil, nil, nil, s)
	keySelected.Node = sk
	keySelected.NodeBuilder = sk
	keySelected.keySelected = true
	keySelected.rootStream = s.rootStream

	return keySelected
}

func (s *kStream) TransformValue(valueTransformFunc processors.ValueTransformFunc) Stream {
	tv := &processors.ValueTransformer{
		ValueTransformFunc: valueTransformFunc,
		Id:                 atomic.AddInt32(&nodeCounter, 1),
	}

	s.Node.AddChild(tv)
	s.NodeBuilder.AddChildBuilder(tv)

	valueTransformed := newKStream(nil, nil, nil, s)
	valueTransformed.Node = tv
	valueTransformed.NodeBuilder = tv
	valueTransformed.rootStream = s.rootStream
	valueTransformed.keySelected = s.keySelected

	return valueTransformed
}

func (s *kStream) Transform(transformer processors.TransFunc) Stream {
	t := &processors.Transformer{
		TransFunc: transformer,
		Id:        atomic.AddInt32(&nodeCounter, 1),
	}

	s.Node.AddChild(t)
	s.NodeBuilder.AddChildBuilder(t)

	transformed := newKStream(nil, nil, nil, s)
	transformed.Node = t
	transformed.NodeBuilder = t
	transformed.keySelected = true
	transformed.rootStream = s.rootStream

	return transformed
}

func (s *kStream) Filter(filter processors.FilterFunc) Stream {
	//s.nodes = append(s.nodes, &processors.Filter{
	//	FilterFunk: filter,
	//})

	f := &processors.Filter{
		FilterFunc: filter,
		Id:         atomic.AddInt32(&nodeCounter, 1),
	}

	s.Node.AddChild(f)
	s.NodeBuilder.AddChildBuilder(f)

	filtered := newKStream(nil, nil, nil, s)
	filtered.Node = f
	filtered.NodeBuilder = f
	filtered.rootStream = s.rootStream
	filtered.keySelected = s.keySelected

	return filtered
}

func (s *kStream) JoinGlobalTable(stream Stream, keyMapper join.KeyMapper, valMapper join.ValueMapper, typ join.JoinType) Stream {
	joinStream, ok := stream.(*globalKTable)
	if !ok {
		logger.DefaultLogger.Fatal(`k-stream.kStream`,
			`unsupported join type for global table joiner, only global tables are supported`)
	}
	joiner := &join.GlobalTableJoiner{
		Typ:         typ,
		Store:       joinStream.storeName,
		KeyMapper:   keyMapper,
		ValueMapper: valMapper,
		Id:          atomic.AddInt32(&nodeCounter, 1),
	}

	s.Node.AddChild(joiner)
	s.NodeBuilder.AddChildBuilder(joiner)

	joined := newKStream(nil, nil, nil, s)
	joined.Node = joiner
	joined.NodeBuilder = joiner
	joined.rootStream = s.rootStream
	joined.keySelected = s.keySelected

	return joined
}

func (s *kStream) JoinStream(stream Stream, valMapper join.ValueMapper, opts ...RepartitionOption) Stream {

	rightStream, ok := stream.(*kStream)
	if !ok {
		logger.DefaultLogger.Fatal(`k-stream.kStream`,
			`unsupported join type for stream joiner, only k-streams are supported`)
	}

	var repartition = &RepartitionOptions{
		LeftTopic:  s.rootStream.topic,
		RightTopic: rightStream.rootStream.topic,
	}
	repartition.Apply(opts...)

	leftWindow := join.NewWindow()
	rightWindow := join.NewWindow()
	joinedNode := &join.StreamJoiner{
		Id: atomic.AddInt32(&nodeCounter, 1),
	}

	left := &join.SideJoiner{
		Side:        `left`,
		LeftWindow:  leftWindow,
		RightWindow: rightWindow,
		ValueMapper: valMapper,
		Id:          atomic.AddInt32(&nodeCounter, 1),
	}
	left.AddChild(joinedNode)
	left.AddChildBuilder(joinedNode)

	right := &join.SideJoiner{
		Side:        `right`,
		LeftWindow:  leftWindow,
		RightWindow: rightWindow,
		ValueMapper: valMapper,
		Id:          atomic.AddInt32(&nodeCounter, 1),
	}
	right.AddChild(joinedNode)
	right.AddChildBuilder(joinedNode)

	var setNewRightStream = func() *kStream {
		err := repartition.RightRepartition.Validate(RightSide)
		if err != nil {
			logger.DefaultLogger.Fatal(`k-stream.kStream`, err)
		}
		rightStream.To(repartition.RightRepartition.Topic.Name, repartition.RightRepartition.KeyEncoder,
			repartition.RightRepartition.ValueEncoder, withPrefixTopic(func(prefix string) string { return prefix + `_right_stream_joiner` }))

		newRightStream := newKStream(func(prefix string) string { return prefix + `_right_stream_joiner` },
			repartition.RightRepartition.KeyEncoder, repartition.RightRepartition.ValueEncoder, nil)

		return newRightStream
	}

	var setNewLeftStream = func() *kStream {
		err := repartition.LeftRepartition.Validate(LeftSide)
		if err != nil {
			logger.DefaultLogger.Fatal(`k-stream.kStream`, err)
		}
		s.To(repartition.LeftRepartition.Topic.Name, repartition.LeftRepartition.KeyEncoder,
			repartition.LeftRepartition.ValueEncoder, withPrefixTopic(func(prefix string) string { return prefix + `_left_stream_joiner` }))

		newLeftStream := newKStream(func(prefix string) string { return prefix + `_left_stream_joiner` },
			repartition.RightRepartition.KeyEncoder, repartition.RightRepartition.ValueEncoder, nil)

		return newLeftStream
	}

	var setJoinedStream = func(stm *kStream) *kStream {
		joined := newKStream(nil, nil, nil, stm)
		joined.Node = joinedNode
		joined.NodeBuilder = joinedNode
		joined.rootStream = stm.rootStream
		joined.keySelected = stm.keySelected

		return joined
	}

	if !s.keySelected && !rightStream.keySelected {

		s.Node.AddChild(left)
		s.NodeBuilder.AddChildBuilder(left)

		rightStream.Node.AddChild(right)
		rightStream.NodeBuilder.AddChildBuilder(right)

		return setJoinedStream(s)

	}

	if rightStream.keySelected && s.keySelected {

		newRightStream := setNewRightStream()
		newRightStream.Node = right
		newRightStream.NodeBuilder = right
		s.rootStream.streams = append(s.rootStream.streams, newRightStream)

		newLeftStream := setNewLeftStream()
		newLeftStream.Node = left
		newLeftStream.NodeBuilder = left
		s.rootStream.streams = append(s.rootStream.streams, newLeftStream)

		return setJoinedStream(newLeftStream)
	}

	if rightStream.keySelected && !s.keySelected {

		newRightStream := setNewRightStream()
		newRightStream.Node = right
		newRightStream.NodeBuilder = right
		s.rootStream.streams = append(s.rootStream.streams, newRightStream)

		s.Node.AddChild(left)
		s.NodeBuilder.AddChildBuilder(left)

		return setJoinedStream(s)
	}

	if s.keySelected && !rightStream.keySelected {

		newLeftStream := setNewLeftStream()
		newLeftStream.Node = left
		newLeftStream.NodeBuilder = left
		s.rootStream.streams = append(s.rootStream.streams, newLeftStream)

		rightStream.Node.AddChild(right)
		rightStream.NodeBuilder.AddChildBuilder(right)

		return setJoinedStream(newLeftStream)
	}

	logger.DefaultLogger.Fatal(`k-stream.kStream.stream_joiner`, `can not reach here`)

	return nil
}

func (s *kStream) JoinKTable(stream Stream, keyMapper join.KeyMapper, valMapper join.ValueMapper) Stream {
	panic("implement me")
}

func (s *kStream) Process(processor processors.ProcessFunc) Stream {
	p := &processors.Processor{
		ProcessFunc: processor,
		Id:          atomic.AddInt32(&nodeCounter, 1),
	}

	s.Node.AddChild(p)
	s.NodeBuilder.AddChildBuilder(p)

	processed := newKStream(nil, nil, nil, s)
	processed.Node = p
	processed.NodeBuilder = p
	processed.rootStream = s.rootStream
	processed.keySelected = s.keySelected

	return processed
}

func (s *kStream) Through(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...SinkOption) Stream {

	if keyEncoder == nil {
		logger.DefaultLogger.Fatal(`k-stream.kStream`, fmt.Sprintf(`keyEncoder cannot be null for sink [%s]`, topic))
	}

	if valEncoder == nil {
		logger.DefaultLogger.Fatal(`k-stream.kStream`, fmt.Sprintf(`valEncoder cannot be null for sink [%s]`, topic))
	}

	s.To(topic, keyEncoder, valEncoder, options...)

	stream := newKStream(func(prefix string) string { return topic }, keyEncoder, valEncoder, nil)

	s.streams = append(s.streams, stream)

	return stream
}

func (s *kStream) To(topic string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...SinkOption) {

	if keyEncoder == nil {
		logger.DefaultLogger.Fatal(`k-stream.kStream`, fmt.Sprintf(`keyEncoder cannot be null for sink [%s]`, topic))
	}

	if valEncoder == nil {
		logger.DefaultLogger.Fatal(`k-stream.kStream`, fmt.Sprintf(`valEncoder cannot be null for sink [%s]`, topic))
	}

	sink := NewKSinkBuilder(
		`sink_`+topic,
		atomic.AddInt32(&nodeCounter, 1),
		func(prefix string) string { return topic },
		keyEncoder,
		valEncoder,
		options...)

	sink.info = map[string]string{
		`topic`: topic,
	}

	//sink := &KSink{
	//	name:              `sink_` + topic,
	//	topic:             func(prefix string)(string){return topic},
	//	KeyEncoderBuilder: keyEncoder,
	//	ValEncoderBuilder: valEncoder,
	//	info: map[string]string{
	//		`topic`: topic,
	//	},
	//}
	//sink.applyOptions(options...)

	s.Node.AddChild(sink)
	s.NodeBuilder.AddChildBuilder(sink)

}

func (s *kStream) Build(builder *StreamBuilder) ([]*kStream, error) {
	var streams []*kStream
	t := new(node.TopologyBuilder)
	t.Source = s.source
	err := s.build(builder, s.NodeBuilder)
	if err != nil {
		return nil, err
	}
	t.SourceNodeBuilder = s.NodeBuilder

	s.topology = t

	streams = append(streams, s)

	// Build other topologies
	for _, tOther := range s.streams {
		tps, err := tOther.Build(builder)
		if err != nil {
			return nil, err
		}

		streams = append(streams, tps...)
	}

	return streams, nil
}

func (s *kStream) build(builder *StreamBuilder, node node.NodeBuilder) error {

	switch nd := node.(type) {
	case *join.GlobalTableJoiner:
		nd.Registry = builder.storeRegistry

	case *KSink:
		nd.ProducerBuilder = builder.defaultBuilders.producer
		nd.TopicPrefix = builder.config.ApplicationId + `_`
	}

	for _, nodeBuilder := range node.ChildBuilders() {
		err := s.build(builder, nodeBuilder)
		if err != nil {
			return err
		}
	}

	return nil
}

type SourceNode struct {
	Id            int32
	childs        []node.Node
	childBuilders []node.NodeBuilder
}

func (sn *SourceNode) Childs() []node.Node {
	return sn.childs
}

func (sn *SourceNode) ChildBuilders() []node.NodeBuilder {
	return sn.childBuilders
}

func (sn *SourceNode) AddChildBuilder(builder node.NodeBuilder) {
	sn.childBuilders = append(sn.childBuilders, builder)
}

func (sn *SourceNode) AddChild(node node.Node) {
	sn.childs = append(sn.childs, node)
}

func (sn *SourceNode) Build() (node.Node, error) {
	var childs []node.Node
	//var childBuilders []node.NodeBuilder

	for _, childBuilder := range sn.childBuilders {
		child, err := childBuilder.Build()
		if err != nil {
			return nil, err
		}

		childs = append(childs, child)
	}

	return &SourceNode{
		childs: childs,
	}, nil
}

func (sn *SourceNode) Next() bool {
	panic("implement me")
}

func (sn *SourceNode) Run(ctx context.Context, kIn, vIn interface{}) (kOut, vOut interface{}, cont bool, err error) {
	for _, child := range sn.childs {
		_, _, next, err := child.Run(ctx, kIn, vIn)
		if err != nil || !next {
			return nil, nil, false, err
		}
	}
	return kIn, vIn, true, nil
}

func (sn *SourceNode) Type() node.Type {
	panic("implement me")
}
