package offset

import (
	"github.com/pickme-go/k-stream/consumer"
	"testing"
)

func TestManager_OffsetValid(t *testing.T) {

	tp := consumer.TopicPartition{
		Topic:     `test`,
		Partition: 1,
	}
	manager := MockManagerWithOffset(tp, 100, 200)

	valid, brokerOffset, err := manager.OffsetValid(tp, 199)
	if err != nil {
		t.Error(err)
	}

	if !valid {
		t.Fail()
	}

	if brokerOffset != 100 {
		t.Fail()
	}
}
