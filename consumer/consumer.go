package consumer

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/metrics"
	"sync"
	"time"
)

func init() {
	//sarama.Logger = log.New(os.Stdout, ``, log.Lmicroseconds)
}

type Builder func(config *Config) (Consumer, error)

type PartitionConsumerBuilder func(config *PartitionConsumerConfig) (PartitionConsumer, error)

type TopicPartition struct {
	Topic     string
	Partition int32
}

func (tp TopicPartition) String() string {
	return fmt.Sprintf(`%s_%d`, tp.Topic, tp.Partition)
}

type Consumer interface {
	Consume(tps []string, handler ReBalanceHandler) (<-chan *Record, error)
	Rebalanced() <-chan Allocation
	Partitions(tps []string, handler ReBalanceHandler) (chan *Partition, error)
	Errors() <-chan *Error
	//PartitionConsumer() (PartitionConsumer, error)
	Commit() error
	Mark(record *Record)
	CommitPartition(topic string, partition int32, offset int64) error
	GetLatestOffset(topic string, partition int32) (int64, error)
	Close() error
}

type Offset int64

const (
	Earliest Offset = -2
	Latest   Offset = -1
)

func (o Offset) String() string {
	switch o {
	case -2:
		return `Earliest`
	case -1:
		return `Latest`
	default:
		return `unknown`
	}
}

type consumer struct {
	config  *Config
	context struct {
		ctx    context.Context
		cancel context.CancelFunc
	}
	saramaConsumer    *cluster.Consumer
	consumerErrors    chan *Error
	reBalanced        chan Allocation
	reBalancing       bool
	reBalanceNotified *sync.Cond
	reBalanceHandler  ReBalanceHandler
	stopping          chan bool
	booted            bool
	stopped           chan bool
	allocator         *PartitionAllocator
	partitionMap      *PartitionMap
	groupOffsetMeta   *groupOffsetMeta
	metrics           struct {
		reBalancing      metrics.Gauge
		commitLatency    metrics.Observer
		reBalanceLatency metrics.Observer
	}
}

func NewConsumer(config *Config) (Consumer, error) {

	if err := config.validate(); err != nil {
		return nil, err
	}

	c := &consumer{
		config:            config,
		allocator:         newPartitionAllocator(),
		consumerErrors:    make(chan *Error, 1),
		reBalanced:        make(chan Allocation, 1),
		stopping:          make(chan bool, 1),
		stopped:           make(chan bool, 1),
		reBalanceNotified: sync.NewCond(new(sync.Mutex)),
		partitionMap:      newPartitionMap(config.MetricsReporter, config.Logger),
	}

	pMeta, err := newPartitionMeta(config, config.Logger)
	if err != nil {
		return nil, err
	}

	c.groupOffsetMeta = pMeta

	// start the rpc server for partition metadata
	if err := c.startPartitionMetaRPC(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.context.ctx = ctx
	c.context.cancel = cancel

	c.metrics.commitLatency = config.MetricsReporter.Observer(`k_stream_consumer_commit_latency_microseconds`, nil)
	c.metrics.reBalanceLatency = config.MetricsReporter.Observer(`k_stream_consumer_re_balance_latency_microseconds`, nil)
	c.metrics.reBalancing = config.MetricsReporter.Gauge(`k_stream_consumer_rebalancing`, nil)

	return c, nil
}

func (c *consumer) Consume(tps []string, handler ReBalanceHandler) (<-chan *Record, error) {
	panic(`to be implemented`)
}

func (c *consumer) Partitions(tps []string, handler ReBalanceHandler) (chan *Partition, error) {
	c.reBalanceHandler = handler
	consumer, err := cluster.NewConsumer(c.config.BootstrapServers, c.config.GroupId, tps, c.config.Config)
	if err != nil {
		return nil, errors.WithPrevious(err, `k-stream.consumer`, "Failed to create consumer")
	}
	c.saramaConsumer = consumer

	// Subscribe for all InputTopics,
	c.config.Logger.Info(`k-stream.consumer`, fmt.Sprintf(`subscribing to topics %v`, tps))

	go func() {
		for err := range consumer.Errors() {
			c.config.Logger.Error(`k-stream.consumer`, fmt.Sprintf("Error: %+v", err))
			c.consumerErrors <- &Error{err}
		}
	}()

	go c.runNotifications(consumer)

	go func() {
		//create a waitgroup for running partitions
		for saramaPartition := range consumer.Partitions() {
			go c.runPartition(saramaPartition)
		}
	}()

	return c.partitionMap.partitionsBuffer, nil
}

func (c *consumer) Commit() error {
	return c.saramaConsumer.CommitOffsets()
}

func (c *consumer) Mark(record *Record) {
	c.saramaConsumer.MarkOffset(&sarama.ConsumerMessage{
		Topic:     record.Topic,
		Partition: record.Partition,
		Key:       record.Key,
		Value:     record.Value,
		Offset:    record.Offset,
		Timestamp: record.Timestamp,
	}, c.config.Host)
}

func (c *consumer) CommitPartition(topic string, partition int32, offset int64) error {
	defer func(begin time.Time) {
		c.metrics.commitLatency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{
			`topic`:     topic,
			`partition`: fmt.Sprint(partition),
		})
	}(time.Now())
	c.saramaConsumer.MarkPartitionOffset(topic, partition, offset, ``)
	return nil
}

func (c *consumer) GetLatestOffset(topic string, partition int32) (int64, error) {
	panic(`to be implemented`)
}

func (c *consumer) extractTps(kafkaTps map[string][]int32) []TopicPartition {
	tps := make([]TopicPartition, 0)
	for topic, partitions := range kafkaTps {
		for _, p := range partitions {
			tps = append(tps, TopicPartition{
				Topic:     topic,
				Partition: p,
			})
		}
	}
	return tps
}

func (c *consumer) refreshAllocations(tps []TopicPartition) {
	c.allocator.assign(tps)

	if len(c.allocator.allocations().Removed) > 0 {
		wg := new(sync.WaitGroup)
		wg.Add(len(c.allocator.allocations().Removed))
		for _, tp := range c.allocator.allocations().Removed {
			go func(tp TopicPartition) {
				c.partitionMap.closePartition(tp)
				wg.Done()
			}(tp)
		}
		wg.Wait()
		c.reBalanceHandler.OnPartitionRevoked(c.context.ctx, c.allocator.allocations().Removed)
	}

	if len(c.allocator.allocations().Assigned) > 0 {

		if !c.booted {
			// before assign check if other instances are still processing assigned partitions and if they are not done with
			// these partitions wait until they finish
			if err := c.waitForRemote(c.allocator.allocations().Assigned); err != nil {
				c.config.Logger.Error(`k-stream.consumer`, err)
			}
			c.booted = true
		}

		c.reBalanceHandler.OnPartitionAssigned(c.context.ctx, c.allocator.allocations().Assigned)
	}

	c.reBalanceNotified.Broadcast()
}

func (c *consumer) Errors() <-chan *Error {
	return c.consumerErrors
}

func (c *consumer) Rebalanced() <-chan Allocation {
	return c.reBalanced
}

func (c *consumer) Close() error {

	c.config.Logger.Info(`k-stream.consumer`, `upstream consumer is closing...`)
	defer c.config.Logger.Info(`k-stream.consumer`, `upstream consumer closed`)

	c.partitionMap.closeAll()

	// close sarama consumer so application will leave from the consumer group
	if err := c.saramaConsumer.Close(); err != nil {
		c.config.Logger.Error(`k-stream.consumer`,
			fmt.Sprintf(`cannot close consumer due to %+v`, err))
	}

	close(c.reBalanced)
	close(c.consumerErrors)

	return nil
}

func (c *consumer) runNotifications(consumer *cluster.Consumer) {

	var rebalanceLatency time.Time

	for notify := range consumer.Notifications() {
		switch notify.Type {
		case cluster.RebalanceStart:
			c.reBalancing = true
			rebalanceLatency = time.Now()
			c.metrics.reBalancing.Count(10, nil)
			c.config.Logger.Warn(`k-stream.consumer`, fmt.Sprintf(`consumer re-balanceing %#v`, c.allocator.currentAllocation))

		case cluster.RebalanceError:
			c.metrics.reBalancing.Count(30, nil)
			c.config.Logger.Error(`k-stream.consumer`, fmt.Sprintf(`consumer re-balance error %#v`, notify))

		case cluster.UnknownNotification:
			c.metrics.reBalancing.Count(0, nil)
			c.config.Logger.Error(`k-stream.consumer`, fmt.Sprintf(`consumer unknown notification %#v`, notify))

		case cluster.RebalanceOK:
			c.config.Logger.Warn(`k-stream.consumer`, fmt.Sprintf(`consumer re-balanced %#v`, notify))
			c.metrics.reBalanceLatency.Observe(float64(time.Since(rebalanceLatency).Nanoseconds()/1e3), nil)
			c.metrics.reBalancing.Count(0, nil)

			c.reBalancing = false
			c.refreshAllocations(c.extractTps(notify.Current))

			c.config.Logger.Info(`k-stream.consumer`,
				fmt.Sprintf(`rebalance completed in %s : partitions assigned - %v, current allocation - %v`,
					time.Since(rebalanceLatency).Round(time.Millisecond), notify.Claimed, notify.Current))
		default:
			c.config.Logger.Error(`k-stream.consumer`, `unknown notify type`)
		}
	}
}

func (c *consumer) waitForRemote(tps []TopicPartition) error {
	// get partition metadata for assigned partitions
	return nil
	return c.groupOffsetMeta.Wait(tps, c.config.Host)
}

func (c *consumer) startPartitionMetaRPC() error {
	return nil
	pd := newPartitionDiscovery(c.partitionMap, c.config.Host, c.config.Logger)
	if err := pd.startServer(); err != nil {
		return err
	}

	return nil
}

func (c *consumer) runPartition(sarama cluster.PartitionConsumer) {

	tp := TopicPartition{
		Topic:     sarama.Topic(),
		Partition: sarama.Partition(),
	}

	// if still re-balancing wait
	if c.reBalancing {
		c.reBalanceNotified.L.Lock()
		c.reBalanceNotified.Wait()
		c.reBalanceNotified.L.Unlock()
	}

	c.partitionMap.partition(tp, sarama)
	//println(`new partition added `, p.id)
}
