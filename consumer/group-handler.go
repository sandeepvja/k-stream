package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/pickme-go/metrics"
	"time"
)

type groupHandler struct {
	reBalanceHandler ReBalanceHandler
	partitionMap     map[string]*partition
	partitions       chan Partition
	metrics          struct {
		reporter         metrics.Reporter
		reBalancing      metrics.Gauge
		commitLatency    metrics.Observer
		reBalanceLatency metrics.Observer
		endToEndLatency  metrics.Observer
	}
}

func (h *groupHandler) Setup(session sarama.ConsumerGroupSession) error {

	tps := h.extractTps(session.Claims())

	for _, tp := range tps {
		p := newPartition(tp, session)
		h.partitionMap[tp.String()] = p
		h.partitions <- p
	}

	return h.reBalanceHandler.OnPartitionAssigned(session.Context(), tps)
}

func (h *groupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	tps := h.extractTps(session.Claims())

	for _, tp := range tps {
		h.partitionMap[tp.String()].close()
		delete(h.partitionMap, tp.String())
	}

	return h.reBalanceHandler.OnPartitionRevoked(session.Context(), tps)
}

func (h *groupHandler) ConsumeClaim(g sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	tp := TopicPartition{
		Topic:     c.Topic(),
		Partition: c.Partition(),
	}

	ch := h.partitionMap[tp.String()].records

	for msg := range c.Messages() {
		h.metrics.endToEndLatency.Observe(float64(time.Since(msg.Timestamp).Nanoseconds()/1e3), map[string]string{
			`topic`:     msg.Topic,
			`partition`: fmt.Sprint(msg.Partition),
		})
		ch <- &Record{
			Key:       msg.Key,
			Value:     msg.Value,
			Offset:    msg.Offset,
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Timestamp: msg.Timestamp,
			UUID:      uuid.New(),
		}
	}

	return nil
}

func (h *groupHandler) extractTps(kafkaTps map[string][]int32) []TopicPartition {
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
