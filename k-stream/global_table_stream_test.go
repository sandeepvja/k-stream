package kstream

import (
	"fmt"
	"github.com/pickme-go/k-stream/admin"
	"github.com/pickme-go/k-stream/backend/memory"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/data"
	"github.com/pickme-go/k-stream/k-stream/encoding"
	"github.com/pickme-go/k-stream/k-stream/offsets"
	"github.com/pickme-go/k-stream/k-stream/store"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/log/v2"
	"github.com/pickme-go/metrics/v2"
	"sync"
	"testing"
	"time"
)

func TestGlobalTableStream_StartStreams_After(t *testing.T) {
	mocksTopics := admin.NewMockTopics()
	kafkaAdmin := &admin.MockKafkaAdmin{
		Topics: mocksTopics,
	}
	offsetManager := &offsets.MockManager{Topics: mocksTopics}
	if err := kafkaAdmin.CreateTopics(map[string]*admin.Topic{
		`tp1`: {
			Name:              "tp1",
			NumPartitions:     9,
			ReplicationFactor: 1,
		},
		`tp2`: {
			Name:              "tp2",
			NumPartitions:     9,
			ReplicationFactor: 1,
		},
	}); err != nil {
		t.Error(err)
	}

	str1, _ := store.NewStore(`store tp1`, encoding.StringEncoder{}, encoding.StringEncoder{}, store.WithBackend(memory.NewMemoryBackend(log.NewNoopLogger(), metrics.NoopReporter())))
	str2, _ := store.NewStore(`store tp2`, encoding.StringEncoder{}, encoding.StringEncoder{}, store.WithBackend(memory.NewMemoryBackend(log.NewNoopLogger(), metrics.NoopReporter())))

	tables := make(map[string]*globalKTable)
	opts := new(globalTableOptions)
	opts.backendWriter = globalTableStoreWriter
	tables[`tp1`] = &globalKTable{store:str1, storeName: str1.Name(), options:opts}
	tables[`tp2`] = &globalKTable{store:str2, storeName: str2.Name(), options:opts}

	gTableStream, err := newGlobalTableStream(tables, &GlobalTableStreamConfig{
		ConsumerBuilder: consumer.NewMockPartitionConsumerBuilder(mocksTopics, offsetManager),
		BackendBuilder:  memory.Builder(memory.NewConfig()),
		OffsetManager:   offsetManager,
		KafkaAdmin:      kafkaAdmin,
		Metrics:         metrics.NoopReporter(),
		Logger:          log.NewNoopLogger(),
	})
	if err != nil {
		t.Error(err)
	}

	// produce 3333 records to each topic
	p := producer.NewMockProducer(mocksTopics)
	for i:=1; i <= 3333; i++{
		_, _, _ = p.Produce(nil, &data.Record{
			Key:            []byte(fmt.Sprint(i)),
			Value:          []byte(`v`),
			Topic:          "tp1",
		})
		_, _, _ = p.Produce(nil, &data.Record{
			Key:            []byte(fmt.Sprint(i)),
			Value:          []byte(`v`),
			Topic:          "tp2",
		})
	}
	wg := &sync.WaitGroup{}
	gTableStream.StartStreams(wg)

	go func() {
		time.Sleep(2 * time.Second)
		gTableStream.stop()
	}()

	wg.Wait()
	count := 0
	i, _ := str1.GetAll(nil)
	for i.Valid(){
		count ++
		i.Next()
	}

	i, _ = str2.GetAll(nil)
	for i.Valid(){
		count ++
		i.Next()
	}

	if count != 6666 {
		t.Fail()
	}
}

func TestGlobalTableStream_StartStreams_OldestOffset(t *testing.T) {
	mocksTopics := admin.NewMockTopics()
	kafkaAdmin := &admin.MockKafkaAdmin{
		Topics: mocksTopics,
	}
	offsetManager := &offsets.MockManager{Topics: mocksTopics}
	if err := kafkaAdmin.CreateTopics(map[string]*admin.Topic{
		`tp1`: {
			Name:              "tp1",
			NumPartitions:     9,
			ReplicationFactor: 1,
		},
	}); err != nil {
		t.Error(err)
	}

	str1, _ := store.NewStore(`store tp1`, encoding.StringEncoder{}, encoding.StringEncoder{}, store.WithBackend(memory.NewMemoryBackend(log.NewNoopLogger(), metrics.NoopReporter())))
	tables := make(map[string]*globalKTable)
	opts := new(globalTableOptions)
	opts.backendWriter = globalTableStoreWriter
	//opts.initialOffset = GlobalTableOffsetDefault
	tables[`tp1`] = &globalKTable{store:str1, storeName: str1.Name(), options:opts}

	gTableStream, err := newGlobalTableStream(tables, &GlobalTableStreamConfig{
		ConsumerBuilder: consumer.NewMockPartitionConsumerBuilder(mocksTopics, offsetManager),
		BackendBuilder:  memory.Builder(memory.NewConfig()),
		OffsetManager:   offsetManager,
		KafkaAdmin:      kafkaAdmin,
		Metrics:         metrics.NoopReporter(),
		Logger:          log.NewNoopLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// produce 3333 records to each topic
	p := producer.NewMockProducer(mocksTopics)
	for i:=1; i <= 3333; i++{
		_, _, _ = p.Produce(nil, &data.Record{
			Key:            []byte(fmt.Sprint(i)),
			Value:          []byte(`v`),
			Topic:          "tp1",
		})
	}
	wg := &sync.WaitGroup{}
	gTableStream.StartStreams(wg)

	// syncing is done add another batch
	for i:=3333; i <= 6666; i++{
		_, _, _ = p.Produce(nil, &data.Record{
			Key:            []byte(fmt.Sprint(i)),
			Value:          []byte(`v`),
			Topic:          "tp1",
		})
	}

	go func() {
		time.Sleep(2 * time.Second)
		gTableStream.stop()
	}()

	wg.Wait()
	count := 0
	i, _ := str1.GetAll(nil)
	for i.Valid(){
		count ++
		i.Next()
	}
	if count != 6666 {
		t.Error(`count is invalid `, count)
		t.Fail()
	}
}

func TestGlobalTableStream_StartStreams_LatestOffset(t *testing.T) {
	mocksTopics := admin.NewMockTopics()
	kafkaAdmin := &admin.MockKafkaAdmin{
		Topics: mocksTopics,
	}
	offsetManager := &offsets.MockManager{Topics: mocksTopics}
	if err := kafkaAdmin.CreateTopics(map[string]*admin.Topic{
		`tp1`: {
			Name:              "tp1",
			NumPartitions:     9,
			ReplicationFactor: 1,
		},
	}); err != nil {
		t.Error(err)
	}

	str1, _ := store.NewStore(`store tp1`, encoding.StringEncoder{}, encoding.StringEncoder{}, store.WithBackend(memory.NewMemoryBackend(log.NewNoopLogger(), metrics.NoopReporter())))
	tables := make(map[string]*globalKTable)
	opts := new(globalTableOptions)
	opts.backendWriter = globalTableStoreWriter
	opts.initialOffset = GlobalTableOffsetLatest
	tables[`tp1`] = &globalKTable{store:str1, storeName: str1.Name(), options:opts}

	gTableStream, err := newGlobalTableStream(tables, &GlobalTableStreamConfig{
		ConsumerBuilder: consumer.NewMockPartitionConsumerBuilder(mocksTopics, offsetManager),
		BackendBuilder:  memory.Builder(memory.NewConfig()),
		OffsetManager:   offsetManager,
		KafkaAdmin:      kafkaAdmin,
		Metrics:         metrics.NoopReporter(),
		Logger:          log.NewNoopLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// produce 3333 records to each topic
	p := producer.NewMockProducer(mocksTopics)
	//for i:=1; i <= 3333; i++{
	//	_, _, _ = p.Produce(nil, &data.Record{
	//		Key:            []byte(fmt.Sprint(i)),
	//		Value:          []byte(`v`),
	//		Topic:          "tp1",
	//	})
	//}
	wg := &sync.WaitGroup{}
	gTableStream.StartStreams(wg)

	// syncing is done add another batch
	for i:=1; i <= 3333; i++{
		_, _, _ = p.Produce(nil, &data.Record{
			Key:            []byte(fmt.Sprint(i)),
			Value:          []byte(`v`),
			Topic:          "tp1",
		})
	}

	go func() {
		time.Sleep(4 * time.Second)
		gTableStream.stop()
	}()

	wg.Wait()
	count := 0
	i, _ := str1.GetAll(nil)
	for i.Valid(){
		count ++
		i.Next()
	}
	if count != 3333 {
		t.Error(`count is invalid `, count)
		t.Fail()
	}
}