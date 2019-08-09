package kstream

import (
	"context"
	"errors"
	"fmt"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/encoding"
	"github.com/pickme-go/k-stream/graph"
	"github.com/pickme-go/k-stream/internal/join"
	"github.com/pickme-go/k-stream/internal/node"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/k-stream/store"
	"github.com/pickme-go/k-stream/store_backend"
	"hash"
	"hash/fnv"
	"reflect"
	"sync"
)

const MockApplicationId = `mock_k_stream`

var topics *topicData

//to create mock topic to publish data to process in streams
type MockTopic struct {
	name       string
	partitions []chan Message
	producer   producer.Producer
}

func (mt *MockTopic) Produce(key, value []byte) error {

	message := &consumer.Record{
		Topic: mt.name,
		Key:   key,
		Value: value,
	}

	_, _, err := mt.producer.Produce(context.Background(), message)
	return err
}

type Message struct {
	key   []byte
	value []byte
}

func NewMockTopic(name string, partition int, partitionBuffer int) (*MockTopic, error) {
	topic := &MockTopic{
		name:       name,
		partitions: make([]chan Message, partition, partition),
		producer:   newMockProducer(),
	}
	for p, _ := range topic.partitions {
		topic.partitions[p] = make(chan Message, partitionBuffer)
	}

	if topics == nil {
		topics = &topicData{
			topics: make(map[string]*MockTopic),
			Mutex:  new(sync.Mutex),
		}
	}

	err := topics.addTopic(name, topic)
	if err != nil {
		return nil, err
	}

	return topic, nil
}

//mock streams to simulate all kstream features without any kafka connections
type MockStream struct {
	registry     store.Registry
	streams      []Stream
	builtStreams []Stream
	graph *graph.Graph
}

func NewMockKStream(registry store.Registry, streams ...Stream) (*MockStream, error) {

	mockStream := &MockStream{}

	mockStream.registry = registry
	mockStream.graph = graph.NewGraph()

	for _, stream := range streams {
		_, ok := stream.(*kStream)
		if !ok {
			panic(fmt.Sprintf(`mock kstream is not implemented for %v stream type`, reflect.TypeOf(stream)))
		}
		mockStream.streams = append(mockStream.streams, stream)
	}

	return mockStream, nil
}

func (ms *MockStream) Run() {
	wg := new(sync.WaitGroup)
	for _, stream := range ms.builtStreams {
		ks, ok := stream.(*kStream)
		if !ok {
			panic(fmt.Sprintf(`mock kstream is not implemented for %v stream type`, reflect.TypeOf(stream)))
		}

		topicName := ks.topic(MockApplicationId + `_`)
		topic, err := topics.topic(topicName)
		if err != nil {
			panic(err)
		}
		for _, partition := range topic.partitions {
			wg.Add(1)
			go runPartition(wg, ks.NodeBuilder, partition, ks.source.keyEncoderBuilder(), ks.source.valEncoderBuilder())
		}
	}
	wg.Wait()
}

func runPartition(wg *sync.WaitGroup, builder node.NodeBuilder, partition chan Message, keyEncoder, valEncoder encoding.Encoder) {
	node, err := builder.Build()
	if err != nil {
		panic(err)
	}

	for {
		select {
		case message, closed := <-partition:
			if !closed {
				wg.Done()
			}

			k, err := keyEncoder.Decode(message.key)
			if err != nil {
				//return nil, nil, errors.WithPrevious(err, `k-stream.streamProcessor`, `key decode error`)
				//panic(err)
				logger.DefaultLogger.Fatal(`k-stream.mock_stream.runPartition`, fmt.Sprintf(`key decoder error %v`, err))
			}

			v, err := valEncoder.Decode(message.value)
			if err != nil {
				//return nil, nil, errors.WithPrevious(err, `k-stream.streamProcessor`, `value decode error`)
				//panic(err)
				logger.DefaultLogger.Fatal(`k-stream.mock_stream.runPartition`, fmt.Sprintf(`value decoder error %v`, err))
			}
			_, _, _, _ = node.Run(context.Background(), k, v)
		}
	}

}

func (ms *MockStream) Build() {
	for _, stream := range ms.streams {

		built := ms.buildStream(stream)
		ms.builtStreams = append(ms.builtStreams, built...)
	}

	for _, stream := range ms.builtStreams {
		st, _ := stream.(*kStream)
		ms.graph.RenderTopology(st.topology)
	}
	fmt.Println(`k-stream.stream-builder `, ms.graph.Build())
}

func (ms *MockStream) buildStream(s Stream) []Stream {
	var streams []Stream

	ks, _ := s.(*kStream)
	t := new(node.TopologyBuilder)
	t.Source = ks.source

	ms.buildNode(ks.NodeBuilder)

	t.SourceNodeBuilder = ks.NodeBuilder
	ks.topology =t
	streams = append(streams, ks)

	for _, strms := range ks.streams {
		bstrms := ms.buildStream(strms)
		streams = append(streams, bstrms...)
	}

	return streams
}

func (ms *MockStream) buildNode(node node.NodeBuilder) {
	switch nd := node.(type) {
	//case *join.GlobalTableJoiner:
	//	nd.Registry = builder.storeRegistry

	case *KSink:
		nd.ProducerBuilder = func(options *producer.Options) (i producer.Producer, e error) {
			return &MockStreamProducer{hasher: fnv.New32a()}, nil
		}
		nd.TopicPrefix = MockApplicationId + `_`
	case *join.GlobalTableJoiner:
		nd.Registry = ms.registry
	}

	for _, nodeBuilder := range node.ChildBuilders() {
		ms.buildNode(nodeBuilder)
	}
}

//topic data is to make sure uniqueness of topics and handle topics
type topicData struct {
	*sync.Mutex
	topics map[string]*MockTopic
}

func (td *topicData) addTopic(name string, topic *MockTopic) error {
	td.Lock()
	defer td.Unlock()
	_, ok := td.topics[name]
	if ok {
		return errors.New(`topic already exists`)
	}
	td.topics[name] = topic
	return nil
}

func (td *topicData) topic(name string) (*MockTopic, error) {
	td.Lock()
	defer td.Unlock()

	t, ok := td.topics[name]
	if !ok {
		return t, errors.New(fmt.Sprintf(`topic does not exists "%v"`, name))
	}

	return t, nil
}

//to produce data to mock topics
type MockStreamProducer struct {
	hasher hash.Hash32
}

func newMockProducer() *MockStreamProducer {
	return &MockStreamProducer{
		hasher: fnv.New32a(),
	}
}

func (msp *MockStreamProducer) Produce(ctx context.Context, message *consumer.Record) (partition int32, offset int64, err error) {
	msp.hasher.Reset()
	_, err = msp.hasher.Write(message.Key)
	if err != nil {
		return partition, offset, err
	}

	topic, err := topics.topic(message.Topic)
	if err != nil {
		return partition, offset, err
	}

	p := int64(msp.hasher.Sum32()) % int64(len(topic.partitions))

	topic.partitions[p] <- Message{key: message.Key, value: message.Value}

	return int32(p), offset, nil
}

func (msp *MockStreamProducer) ProduceBatch(ctx context.Context, messages []*consumer.Record) error {
	panic("implement me")
}

func (msp *MockStreamProducer) Close() error {
	panic("implement me")
}

//mockStoreRegistry is featured for store.Registry for mock store processes
type mockStoreRegistry struct {
	Stores map[string]store.Store
	mu     *sync.Mutex
}

func NewMockStoreRegistry() store.Registry {
	return &mockStoreRegistry{}
}

func (msr *mockStoreRegistry) Register(store store.Store) {
	msr.mu.Lock()
	defer msr.mu.Unlock()

	name := store.Name()
	if _, ok := msr.Stores[name]; ok {
		logger.DefaultLogger.Fatal(`k-stream.Store.Registry`,
			fmt.Sprintf(`store [%s] already exist`, name))
	}

	msr.Stores[name] = store
}

func (msr *mockStoreRegistry) New(name string, keyEncoder encoding.Builder, valEncoder encoding.Builder, options ...store.Options) store.Store {
	msr.mu.Lock()
	defer msr.mu.Unlock()

	if _, ok := msr.Stores[name]; ok {
		logger.DefaultLogger.Fatal(`k-stream.mock_stream.mockStoreRegistry`, fmt.Sprintf(`store [%s] already exist`, name))
	}

	s := store.NewMockStore(name, keyEncoder(), valEncoder(), store_backend.NewMockBackend(name+`backend`, 100))
	msr.Stores[name] = s

	return msr.Stores[name]
}

func (msr *mockStoreRegistry) Store(name string) store.Store {
	msr.mu.Lock()
	defer msr.mu.Unlock()

	store, ok := msr.Stores[name]
	if !ok {
		logger.DefaultLogger.Error(`k-stream.mock_stream.mockStoreRegistry`, fmt.Sprintf(`unknown store [%s]`, name))
	}

	return store
}

func (msr *mockStoreRegistry) List() []string {
	msr.mu.Lock()
	defer msr.mu.Unlock()

	var list []string

	for name := range msr.Stores {
		list = append(list, name)
	}

	return list
}
