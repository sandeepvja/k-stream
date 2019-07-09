package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/pickme-go/k-stream/logger"
	"github.com/pickme-go/metrics"
	"github.com/pickme-go/traceable-context"
	"sync"
	"time"
)

type PartitionMap struct {
	id               string
	partitions       *sync.Map
	metricsReporter  metrics.Reporter
	logger           logger.Logger
	partitionsBuffer chan Partition
	wg               *sync.WaitGroup
}

func newPartitionMap(group string, metricsReporter metrics.Reporter, logger logger.Logger) *PartitionMap {
	return &PartitionMap{
		id:               group,
		partitions:       new(sync.Map),
		metricsReporter:  metricsReporter,
		logger:           logger,
		wg:               new(sync.WaitGroup),
		partitionsBuffer: make(chan Partition, 1000), // TODO get number of partitions and set the partition buffer
	}
}

func (m *PartitionMap) partition(tp TopicPartition, saramaPartition sarama.PartitionConsumer) *partition {

	var p *partition

	// update the partition map
	existing, ok := m.partitions.Load(tp.String())
	if !ok {
		//m.logger.Info(`kStream.consumer.partitionMap`, fmt.Sprintf(`partition [%s] does not exists`, tp))
		p = &partition{
			id:              uuid.New().String(),
			TopicPartition:  tp,
			log:             m.logger,
			SaramaPartition: saramaPartition,
			records:         make(chan *Record),
			stopping:        make(chan bool, 1),
			stopped:         make(chan bool, 1),
			done:            make(chan bool, 1),
		}
		p.metrics.consumerBuffer = m.metricsReporter.Gauge(metrics.MetricConf{
			Path:        `k_stream_consumer_buffer`,
			ConstLabels: map[string]string{`group`: m.id},
		})
		p.metrics.consumerBufferMax = m.metricsReporter.Gauge(metrics.MetricConf{
			Path:        `k_stream_consumer_buffer_max`,
			ConstLabels: map[string]string{`group`: m.id},
		})
		p.metrics.endToEndLatency = m.metricsReporter.Observer(metrics.MetricConf{
			Path:        `k_stream_consumer_end_to_end_latency_microseconds`,
			ConstLabels: map[string]string{`group`: m.id},
			Labels:      []string{`topic`, `partition`},
		})
		m.partitions.Store(tp.String(), p)
		m.partitionsBuffer <- p
	} else {
		p = existing.(*partition)
		m.logger.Info(`kStream.consumer.partitionMap`, fmt.Sprintf(`partition [%s] already exist`, tp))
		// wait until loop stopped
		p.stop()
		if err := p.closeUpStream(); err != nil {
			m.logger.Error(`kStream.consumer.partitionMap`, err)
		}

		p.SaramaPartition = saramaPartition
		m.wg.Add(1)
	}

	go p.run()

	return p
}

func (m *PartitionMap) closePartition(tp TopicPartition) {
	p, _ := m.partitions.Load(tp.String())
	p.(*partition).close()
	m.partitions.Delete(tp.String())
}

//func (m *PartitionMap) closeUpstreamPartitions(){
//	m.partitions.Range(func(key, value interface{}) bool {
//		if err := value.(*partition).closeUpStream(); err != nil {
//			m.logger.Error(`k-stream.consumer`, err)
//		}
//
//		return true
//	})
//}

//func (m *PartitionMap) closeDownstreamPartitions(){
//	m.partitions.Range(func(key, value interface{}) bool {
//		value.(*partition).stop()
//
//		if err := value.(*partition).closeDownstream(); err != nil {
//			m.logger.Error(`k-stream.consumer`, err)
//		}
//
//		return true
//	})
//
//	close(m.partitionsBuffer)
//}

func (m *PartitionMap) closeAll() {

	wg := &sync.WaitGroup{}
	wg.Add(m.count())
	m.partitions.Range(func(key, value interface{}) bool {
		p := value.(*partition)
		go func() {
			p.close()
			wg.Done()
		}()
		return true
	})

	wg.Wait()
	close(m.partitionsBuffer)

	m.partitions.Range(func(key, value interface{}) bool {
		m.partitions.Delete(key.(string))
		return true
	})

}

func (m *PartitionMap) count() int {
	var c int
	m.partitions.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	return c
}

type partition struct {
	id              string
	log             logger.Logger
	TopicPartition  TopicPartition
	records         chan *Record
	upstreamClosed  bool
	SaramaPartition sarama.PartitionConsumer
	stopping        chan bool
	stopped         chan bool
	done            chan bool
	metrics         struct {
		consumerBuffer    metrics.Gauge
		consumerBufferMax metrics.Gauge
		endToEndLatency   metrics.Observer
		ticker            *time.Ticker
	}
}

func (p *partition) runMetrics() {
	p.metrics.ticker = time.NewTicker(1 * time.Second)
	for range p.metrics.ticker.C {
		p.metrics.consumerBuffer.Count(float64(len(p.SaramaPartition.Messages())), nil)
		p.metrics.consumerBufferMax.Count(float64(cap(p.SaramaPartition.Messages())), nil)
	}
}

func (p *partition) closeUpStream() error {
	return p.SaramaPartition.Close()
}

//func (p *partition) notifyOnClose(c chan bool) {
//	p.closedNotifications = append(p.closedNotifications, c)
//}

// stop waits until all the buffers are closed and will make sure downstream processors are done before it exist
func (p *partition) stop() {
	p.metrics.ticker.Stop()
	p.stopping <- true
	<-p.stopped
}

func (p *partition) Wait() chan bool {
	return p.done
}

func (p *partition) Partition() TopicPartition {
	return p.TopicPartition
}

func (p *partition) Records() <-chan *Record {
	return p.records
}

func (p *partition) close() {
	p.log.Info(`kStream.consumer.partition`, fmt.Sprintf(`detaching downstream buffer from consumer buffer on [%s]`, p.TopicPartition))
	// stop message loop
	p.stop()

	p.log.Info(`kStream.consumer.partition`, fmt.Sprintf(`downstream partition buffer closed on [%s]`, p.TopicPartition))
	// close partition message buffer
	close(p.records)

	// wait until downstream consumers are done
	p.log.Info(`kStream.consumer.partition`, fmt.Sprintf(`waiting for downstream consumers for until the processing is finished on [%s]`, p.TopicPartition))
	<-p.done

	// notify all the closedNotify channels
	//for _, c := range p.closedNotifications{
	//	c <- true
	//}

	p.log.Info(`kStream.consumer.partition`, fmt.Sprintf(`partition closed on [%s]`, p.TopicPartition))
}

func (p *partition) run() {
	go p.runMetrics()

MessageLoop:
	for {
		select {
		case msg, ok := <-p.SaramaPartition.Messages():
			if !ok {
				p.log.Info(`kStream.consumer.partition`, fmt.Sprintf(`upstream partition buffer closed on [%s]`, p.TopicPartition))
				break MessageLoop
			}

			p.metrics.endToEndLatency.Observe(float64(time.Since(msg.Timestamp).Nanoseconds()/1e3), map[string]string{
				`topic`:     msg.Topic,
				`partition`: fmt.Sprint(msg.Partition),
			})

			record := &Record{
				Key:       msg.Key,
				Value:     msg.Value,
				Offset:    msg.Offset,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Timestamp: msg.Timestamp,
				UUID:      uuid.New(),
			}

			ctx := traceable_context.WithUUID(record.UUID)

			logger.DefaultLogger.TraceContext(ctx, `k-stream.consumer`,
				fmt.Sprintf(`Received after %d microseconds on %s[%d]`,
					time.Now().Sub(msg.Timestamp).Nanoseconds()/1000, msg.Topic, msg.Partition))

			p.records <- record

		case <-p.stopping:
			p.log.Info(`kStream.consumer.partition`, fmt.Sprintf(`stopping buffer loop on %s`, p.TopicPartition))
			break MessageLoop

		}
	}

	p.stopped <- true

}
