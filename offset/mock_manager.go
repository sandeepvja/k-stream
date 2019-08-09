package offset

import (
	"github.com/pickme-go/k-stream/consumer"
)

type MockManager struct {
	tp                     consumer.TopicPartition
	offsetStart, offsetEnd int64
}

func MockManagerWithOffset(tp consumer.TopicPartition, offsetStart, offsetEnd int64) MockManager {
	return MockManager{
		tp:          tp,
		offsetStart: offsetStart,
		offsetEnd:   offsetEnd,
	}
}

func (m MockManager) OffsetValid(tp consumer.TopicPartition, offset int64) (isValid bool, valid int64, err error) {
	return m.validate(tp, offset)
}

func (m MockManager) offsetValid(local, bkStart, bkEnd int64) bool {
	return local >= bkStart && local < bkEnd
}

func (m MockManager) validate(tp consumer.TopicPartition, offset int64) (isValid bool, valid int64, err error) {
	return m.offsetValid(offset, m.offsetStart, m.offsetEnd), m.offsetStart, nil
}
