package offset

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/consumer"
)

type Manager struct {
	Client sarama.Client
}

func (m *Manager) OffsetValid(tp consumer.TopicPartition, offset int64) (isValid bool, valid int64, err error) {
	return m.validate(tp, offset)
}

func (m *Manager) offsetValid(local, bkStart, bkEnd int64) bool {
	return local >= bkStart && local < bkEnd
}

func (m *Manager) validate(tp consumer.TopicPartition, offset int64) (isValid bool, valid int64, err error) {

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
