package kstream

import (
	"fmt"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/metrics"
	"sync"
)

type StreamInstance struct {
	id                     string
	streams                map[string]*streamConfig // topic:topology
	topics                 []string
	processorPool          *processorPool
	numOfParallelConsumers int
	stopping               chan bool
	stopped                chan bool
	synced                 chan bool
	logger                 logger.Logger
	consumer               consumer.Builder
	allocationNotify       chan consumer.Allocation
	builder                *StreamBuilder
}

type instancesOptions struct {
	syncNotify chan bool
	rebalanced chan consumer.Allocation
}

func NotifyOnStart(c chan bool) InstancesOptions {
	return func(config *instancesOptions) {
		config.syncNotify = c
	}
}

func NotifyOnRebalanced(c chan consumer.Allocation) InstancesOptions {
	return func(config *instancesOptions) {
		config.rebalanced = c
	}
}

type InstancesOptions func(config *instancesOptions)

func (iOpts *instancesOptions) apply(options ...InstancesOptions) {
	for _, o := range options {
		o(iOpts)
	}
}

type Instances struct {
	streams           map[int]*StreamInstance
	globalTables      map[string]*globalTableConfig
	globalTableStream *globalTableStream
	options           *instancesOptions
	logger            logger.Logger
	builder           *StreamBuilder
	metricsReporter   metrics.Reporter
}

func NewStreams(builder *StreamBuilder, options ...InstancesOptions) *Instances {

	opts := new(instancesOptions)
	opts.apply(options...)

	instances := &Instances{
		streams:         make(map[int]*StreamInstance, 0),
		globalTables:    builder.globalTables,
		options:         opts,
		builder:         builder,
		metricsReporter: builder.metricsReporter,
		logger:          builder.logger,
	}

	if len(builder.streams) < 1 {
		return instances
	}

	for i := 1; i <= builder.config.ConsumerCount; i++ {
		id := fmt.Sprintf(`instance_%d`, i)
		instance := &StreamInstance{
			id:                     id,
			streams:                builder.streams,
			processorPool:          newProcessorPool(id, builder.streams, builder.defaultBuilders.changelog, builder.logger, builder.metricsReporter),
			numOfParallelConsumers: builder.config.ConsumerCount,
			stopping:               make(chan bool, 1),
			stopped:                make(chan bool, 1),
			logger:                 builder.logger,
			consumer:               builder.defaultBuilders.consumer,
			builder:                builder,
		}

		if opts.rebalanced != nil {
			instance.allocationNotify = opts.rebalanced
		}

		for t := range builder.streams {
			instance.topics = append(instance.topics, t)
		}

		instances.streams[i] = instance
	}

	return instances
}

func (ins *Instances) Start() (err error) {

	defer logger.DefaultLogger.Info(`k-stream.streams`, `k-stream shutdown completed`)

	var syncableReplicas []consumer.TopicPartition

	// start changelog replica syncing
	for _, stream := range ins.builder.streams {
		if stream.changelog.enabled && stream.changelog.replicated {
			for i := 0; i <= stream.changelog.topic.numOfPartitions-1; i++ {
				// already allocated partition doesnt need a replica syncer
				syncableReplicas = append(syncableReplicas, consumer.TopicPartition{
					Topic: stream.changelog.topic.name, Partition: int32(i),
				})
			}
		}
	}

	if len(syncableReplicas) > 0 {
		if err := ins.builder.changelogReplicaManager.StartReplicas(syncableReplicas); err != nil {
			logger.DefaultLogger.Fatal(`k-stream.streams`, err)
		}
	}

	wg := new(sync.WaitGroup)
	// start global table streams
	if len(ins.globalTables) > 0 {
		//wg.Add(1)
		ins.globalTableStream = newGlobalTableStream(ins.globalTables, &GlobalTableStreamConfig{
			ConsumerBuilder: ins.builder.defaultBuilders.partitionConsumer,
			Logger:          ins.logger,
			Metrics:         ins.metricsReporter,
			BackendBuilder:  ins.builder.defaultBuilders.backend,
		})
		ins.globalTableStream.startStreams(wg)
		if len(ins.streams) < 1 {
			if ins.options.syncNotify != nil {
				ins.options.syncNotify <- true
			}

			wg.Wait()
			return nil
		}
	}

	for _, instance := range ins.streams {
		// start stream consumer
		wg.Add(1)
		go func(wg *sync.WaitGroup, i *StreamInstance) {
			if err := i.Start(wg); err != nil {
				logger.DefaultLogger.Fatal(`k-stream.streams`, err)
			}
		}(wg, instance)
	}

	if ins.options.syncNotify != nil {
		ins.options.syncNotify <- true
	}

	wg.Wait()
	return nil
}

func (ins *Instances) Stop() {

	if len(ins.streams) > 0 {
		wg := &sync.WaitGroup{}
		for _, instance := range ins.streams {
			wg.Add(1)
			go func(w *sync.WaitGroup, i *StreamInstance) {
				i.Stop()
				w.Done()
			}(wg, instance)
		}
		wg.Wait()
	}

	if len(ins.globalTables) > 0 {
		ins.globalTableStream.stop()
	}

}

// starts the high level consumer for all streams
func (s *StreamInstance) Start(wg *sync.WaitGroup) error {

	defer wg.Done()
	config := consumer.NewConsumerConfig()
	config.OnRebalanced = func(allocation consumer.Allocation) {
		// consumer booting logic
		if len(allocation.Assigned) > 0 {
			logger.DefaultLogger.Warn(`k-stream.streams`, `partitions added`, allocation.Assigned)

			for _, tp := range allocation.Assigned {

				// stop started replicas
				if s.streams[tp.Topic].changelog.replicated {
					if err := s.builder.changelogReplicaManager.StopReplicas([]consumer.TopicPartition{
						{Topic: s.streams[tp.Topic].changelog.topic.name, Partition: tp.Partition},
					}); err != nil {
						logger.DefaultLogger.Error(`k-stream.streams`, err)
					}
				}

				if err := s.processorPool.addProcessor(tp); err != nil {
					logger.DefaultLogger.Fatal(`k-stream.streams`, `allocation failed due to `, err)
				}

				pro := s.processorPool.Processor(tp)
				if err := pro.boot(); err != nil {
					logger.DefaultLogger.Fatal(`k-stream.consumer`,
						fmt.Sprintf("cannot boot processor due to : %+v", err))
				}
			}
		}

		if len(allocation.Removed) > 0 {
			logger.DefaultLogger.Warn(`k-stream.streams`, `partitions removed`, allocation.Removed)
			// start changelog replica syncers
			var syncableReplicas []consumer.TopicPartition
			for _, tp := range allocation.Removed {
				if s.streams[tp.Topic].changelog.replicated {
					syncableReplicas = append(syncableReplicas, consumer.TopicPartition{
						Topic: s.streams[tp.Topic].changelog.topic.name, Partition: tp.Partition,
					})
				}
			}

			if len(syncableReplicas) > 0 {
				if err := s.builder.changelogReplicaManager.StartReplicas(syncableReplicas); err != nil {
					logger.DefaultLogger.Error(`k-stream.streams`, fmt.Sprintf(`cannot start replicas due to %s`, err))
				}
			}
		}

		go func() {
			if s.allocationNotify != nil {
				s.allocationNotify <- allocation
			}
		}()

	}
	config.Id = fmt.Sprintf(`group_consumer_%s`, s.id)
	c, err := s.consumer(config)
	if err != nil {
		return errors.WithPrevious(err, `k-stream.StreamInstance`, `cannot initiate consumer`)
	}

	partitionConsumers, err := c.Partitions(s.topics)
	if err != nil {
		return errors.WithPrevious(err, `k-stream.StreamInstance`, `cannot start consumer`)
	}

	consumerWg := new(sync.WaitGroup)
	consumerWg.Add(1)
	go func(consumerWg *sync.WaitGroup) {
		<-s.stopping
		if err := c.Close(); err != nil {
			logger.DefaultLogger.Fatal(`k-stream.streams`, err)
		}
		consumerWg.Done()
		s.stopped <- true
	}(consumerWg)

	go func() {
		for err := range c.Errors() {
			logger.DefaultLogger.Info(`k-stream.streams`, `k-stream.streams`, err)
		}
	}()

	wgx := new(sync.WaitGroup)
	for partition := range partitionConsumers {
		wgx.Add(1)
		go func(wg *sync.WaitGroup, p *consumer.Partition) {
			pro := s.processorPool.Processor(p.TopicPartition)

			go func(processor *processor) {
				for record := range processor.changelogMarks {
					if err := c.CommitPartition(record.Topic, record.Partition, record.Offset); err != nil {
						logger.DefaultLogger.Error(`k-stream.consumer`,
							fmt.Sprintf("cannot commit partition offset due to : %+v", err))
					}
				}
			}(pro)

			fmt.Print(pro, p)

			pro.records = p.Records
			pro.start()
			wg.Done()
		}(wgx, partition)
	}

	wgx.Wait()
	consumerWg.Wait()
	return nil
}

func (s *StreamInstance) Stop() {
	s.stopping <- true
	<-s.stopped
	logger.DefaultLogger.Info(`k-stream.streams`, fmt.Sprintf(`stream instance [%s] stopped`, s.id))
}
