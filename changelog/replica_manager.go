package changelog

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/k-stream/offset"
	"github.com/pickme-go/k-stream/store_backend"
	"sync"
)

type ReplicaManager struct {
	replicas      map[string]*replicaSyncer // map[tp]syncer
	mu            *sync.RWMutex
	offsetManager *offset.Manager
	conf          ReplicaManagerConf
	cacheManager  *cacheManager
}

type ReplicaManagerConf struct {
	Consumer consumer.PartitionConsumerBuilder
	Backend  store_backend.Builder
	Tps      []consumer.TopicPartition
	Client   sarama.Client
}

func NewReplicaManager(conf ReplicaManagerConf) (*ReplicaManager, error) {

	rm := &ReplicaManager{
		replicas: make(map[string]*replicaSyncer),
		mu:       new(sync.RWMutex),
		offsetManager: &offset.Manager{
			Client: conf.Client,
		},
		conf: conf,
	}

	cacheManager, err := newCacheManager(conf.Backend)
	if err != nil {
		return nil, err
	}

	rm.cacheManager = cacheManager

	return rm, nil
}

func (m *ReplicaManager) StartReplicas(tps []consumer.TopicPartition) error {
	logger.DefaultLogger.Info(`k-stream.changelog.replicaManager`, `starting replica syncers...`)
	wg := new(sync.WaitGroup)
	for _, tp := range tps {
		wg.Add(1)
		if err := m.startReplica(tp, wg); err != nil {
			return err
		}
	}
	wg.Wait()
	return nil
}

func (m *ReplicaManager) GetCache(tp consumer.TopicPartition) (*cache, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cache, err := m.cacheManager.getCache(tp)
	if err != nil {
		return nil, err
	}

	return cache, nil
}

func (m *ReplicaManager) startReplica(tp consumer.TopicPartition, wg *sync.WaitGroup) error {
	logger.DefaultLogger.Info(`k-stream.changelog.replicaManager`, fmt.Sprintf(`starting replica syncer [%s]...`, tp))

	replica, err := m.buildReplica(tp)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.replicas[tp.String()] = replica
	m.mu.Unlock()

	localCached, err := replica.cache.LastSynced()
	if err != nil {
		return err
	}

	valid, brokerOffset, err := m.offsetManager.OffsetValid(tp, localCached)
	if err != nil {
		return err
	}

	startingOffset := brokerOffset
	if valid {
		startingOffset = localCached
		logger.DefaultLogger.Info(`k-stream.changelog.replicaManager`,
			fmt.Sprintf(`local cache [%d] found for [%s]`, localCached, tp))
	} else {
		logger.DefaultLogger.Info(`k-stream.changelog.replicaManager`,
			fmt.Sprintf(`local cache invalid for [%s] flushing...`, tp))
		if err := replica.cache.Flush(); err != nil {
			return err
		}
		logger.DefaultLogger.Info(`k-stream.changelog.replicaManager`,
			fmt.Sprintf(`local cache flushed for [%s]`, tp))
	}

	started, errs := replica.Sync(startingOffset)
	go func() {
		for err := range errs {
			logger.DefaultLogger.Fatal(`k-stream.changelog.replicaManager`, err)
		}
	}()

	<-started
	wg.Done()
	logger.DefaultLogger.Info(`k-stream.changelog.replicaManager`, fmt.Sprintf(`started replica syncer [%s]`, tp))

	return nil
}

func (m *ReplicaManager) buildReplica(tp consumer.TopicPartition) (*replicaSyncer, error) {
	cnf := consumer.NewSimpleConsumerConfig()
	cnf.Id = fmt.Sprintf(`changelog_%s_replica_consumer`, tp)
	c, err := m.conf.Consumer(cnf)
	if err != nil {
		return nil, err
	}

	if rep, ok := m.replicas[tp.String()]; ok {
		rep.consumer = c
		return rep, nil
	}

	cache, err := m.cacheManager.getCache(tp)
	if err != nil {
		return nil, err
	}

	return &replicaSyncer{
		consumer: c,
		cache:    cache,
		tp:       tp,
	}, nil
}

func (m *ReplicaManager) StopReplicas(tps []consumer.TopicPartition) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.DefaultLogger.Info(`k-stream.changelog.replicaManager`, fmt.Sprintf(`stopping replica syncers...`))
	for _, tp := range tps {
		r := m.replicas[tp.String()]
		if err := r.Stop(); err != nil {
			logger.DefaultLogger.Error(`k-stream.changelog.replicaManager`, err)
		} else {
			logger.DefaultLogger.Info(`k-stream.changelog.replicaManager`, fmt.Sprintf(`replica syncer for [%s] stopped`, tp))
		}
	}
	logger.DefaultLogger.Info(`k-stream.changelog.replicaManager`, fmt.Sprintf(`replica syncers stopped`))
	return nil
}
