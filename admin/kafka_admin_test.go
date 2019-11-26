package admin

import "testing"

func TestKafkaAdmin_FetchInfo(t *testing.T) {
	topic := `test`
	admin := NewMockAdminWithTopics(map[string]*Topic{
		topic: {
			Name:          "",
			Partitions:    nil,
			NumPartitions: 2,
		}})
	tps, err := admin.FetchInfo([]string{topic})
	if err != nil {
		t.Error(err)
	}

	if tps[topic].NumPartitions != 2 {
		t.Fail()
	}
}

func TestKafkaAdmin_CreateTopics(t *testing.T) {
	topic := `test`
	admin := &MockKafkaAdmin{NewMockTopics()}

	err := admin.CreateTopics(map[string]*Topic{
		topic: {
			NumPartitions:     5,
			ReplicationFactor: 2,
		},
	})
	if err != nil {
		t.Error(err)
	}

	tps, err := admin.FetchInfo([]string{topic})
	if err != nil {
		t.Error(err)
	}

	if tps[topic].NumPartitions != 5 {
		t.Fail()
	}

	if tps[topic].ReplicationFactor != 2 {
		t.Fail()
	}
}
