package kstream

import (
	"github.com/pickme-go/k-stream/admin"
	"testing"
)

func TestGlobalTableStream_StartStreams(t *testing.T) {
	mocksTopics := admin.NewMockTopics()
	kafkaAdmin := &admin.MockKafkaAdmin{
		Topics: mocksTopics,
	}
	if err := kafkaAdmin.CreateTopics(map[string]*admin.Topic{
		`tp1`: {
			Name:              "tp1",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
		`tp2`: {
			Name:              "tp2",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
	}); err != nil {
		t.Error(err)
	}

	gTableStream := newGlobalTableStream()
}
