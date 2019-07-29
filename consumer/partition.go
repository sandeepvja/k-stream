package consumer

import (
	"github.com/Shopify/sarama"
)

type Partition interface {
	Wait() chan<- bool
	Records() <-chan *Record
	Partition() TopicPartition
	MarkOffset(offset int64)
	CommitOffset(*Record) error
}

type partition struct {
	wait         chan bool
	records      chan *Record
	groupSession sarama.ConsumerGroupSession
	partition    TopicPartition
}

func newPartition(tp TopicPartition, session sarama.ConsumerGroupSession) *partition {
	return &partition{
		wait:         make(chan bool, 1),
		records:      make(chan *Record, 1),
		partition:    tp,
		groupSession: session,
	}
}

func (p *partition) Wait() chan<- bool {
	return p.wait
}

func (p *partition) Records() <-chan *Record {
	return p.records
}

func (p *partition) Partition() TopicPartition {
	return p.partition
}

func (p *partition) MarkOffset(offset int64) {
	p.groupSession.MarkOffset(p.partition.Topic, p.partition.Partition, offset+1, ``)
}

func (p *partition) CommitOffset(r *Record) error {
	p.groupSession.MarkOffset(r.Topic, r.Partition, r.Offset, ``)
	return nil
}

func (p *partition) close() {
	close(p.wait)
	close(p.records)
}
