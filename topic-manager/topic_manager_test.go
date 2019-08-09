package topic_manager

import (
	"github.com/Shopify/sarama"
	"strconv"
	"testing"
)

func newManager() *TopicManager {
	return NewTopicManager(&TopicManagerConfig{
		BootstrapServers: []string{
			`localhost:9092`,
			`localhost:9093`,
			`localhost:9094`,
		},
	})
}

func TestTopicManager_FetchInfo(t *testing.T) {

	tpM := newManager()

	info, err := tpM.FetchInfo([]string{`trip`})
	if err != nil {
		t.Error(err)
	}

	if info.Topics[0].Name != `trip` {
		t.Fail()
	}

}

func TestTopicManager_CreateTopics(t *testing.T) {
	tpM := newManager()

	cleanupPolicy := `compact`
	deleteRetentionMs := `0`
	minCleanableDirtyRatio := `0.01`
	minInSyncReplicas := strconv.Itoa(2)
	segmentMs := `60000`

	topics := map[string]*sarama.TopicDetail{
		`trip_compact`: {
			ReplicationFactor: 2,
			NumPartitions:     5,
			ConfigEntries: map[string]*string{
				`cleanup.policy`:            &cleanupPolicy,
				`delete.retention.ms`:       &deleteRetentionMs,
				`min.cleanable.dirty.ratio`: &minCleanableDirtyRatio,
				`min.insync.replicas`:       &minInSyncReplicas,
				`segment.ms`:                &segmentMs,
			},
		},
	}

	err := tpM.CreateTopics(topics)
	if err != nil {
		t.Error(err)
	}
}

func TestTopicManager_DeleteTopics(t *testing.T) {
	tpM := newManager()
	_, err := tpM.DeleteTopics([]string{`trip_compact`})
	if err != nil {
		t.Error(err)
	}
}
