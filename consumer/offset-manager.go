package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pickme-go/errors"
)

type OffsetManager struct {
	Client sarama.Client
}

func (m OffsetManager) OffsetValid(tp TopicPartition, offset int64) (isValid bool, valid int64, err error) {
	return m.validate(tp, offset)
}

func (m OffsetManager) offsetValid(local, bkStart, bkEnd int64) bool {
	return local >= bkStart && local < bkEnd
}

func (m OffsetManager) validate(tp TopicPartition, offset int64) (isValid bool, valid int64, err error) {

	startOffset, err := m.Client.GetOffset(tp.Topic, tp.Partition, sarama.OffsetOldest)
	if err != nil {
		return false, 0, errors.WithPrevious(err, `k-stream.changelog.replicaManager`,
			fmt.Sprintf(`cannot read startOffset for %s[%d]`, tp.Topic, tp.Partition))
	}

	endOffset, err := m.Client.GetOffset(tp.Topic, tp.Partition, sarama.OffsetNewest)
	if err != nil {
		return false, 0, errors.WithPrevious(err, `k-stream.changelog.replicaManager`,
			fmt.Sprintf(`cannot read endOffset for %s[%d]`, tp.Topic, tp.Partition))
	}

	return m.offsetValid(offset, startOffset, endOffset), startOffset, nil
}
