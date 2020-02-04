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
		`transaction`: {
			Name:              "transaction",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
		`customer_profile`: {
			Name:              "customer_profile",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
		`account_detail`: {
			Name:              "account_detail",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
		`message`: {
			Name:              "message",
			NumPartitions:     2,
			ReplicationFactor: 1,
		},
	}); err != nil {
		panic(err)
	}

	//gTableStream := newGlobalTableStream()
}
