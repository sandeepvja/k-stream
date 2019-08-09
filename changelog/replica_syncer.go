package changelog

import (
	"fmt"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/logger"
)

type replicaSyncer struct {
	cache    *cache
	tp       consumer.TopicPartition
	consumer consumer.PartitionConsumer
	syncing  bool
}

func (rs *replicaSyncer) Sync(startOffset int64) (started chan bool, syncErrors chan error) {
	started = make(chan bool)
	syncErrors = make(chan error)

	go rs.initSync(startOffset, started, syncErrors)

	return started, syncErrors
}

func (rs *replicaSyncer) initSync(startOffset int64, started chan bool, syncErrors chan error) {
	if startOffset == 0 {
		startOffset = int64(consumer.Earliest)
	}

	events, err := rs.consumer.Consume(rs.tp.Topic, rs.tp.Partition, startOffset)
	if err != nil {
		syncErrors <- errors.WithPrevious(err, `k-stream.changelog.replicaSyncer`, fmt.Sprintf(`cannot read partition %s[%d]`,
			rs.tp.Topic, rs.tp.Partition))
		return
	}

	logger.DefaultLogger.Info(`k-stream.changelog.replicaSyncer`, fmt.Sprintf(`replica consumer for [%s] started at offset [%d]`, rs.tp, startOffset))

	started <- true
	rs.syncing = true

	for event := range events {
		switch ev := event.(type) {
		case *consumer.Record:
			if err := rs.cache.Put(ev); err != nil {
				syncErrors <- errors.WithPrevious(err, `k-stream.changelog.replicaSyncer`, `writing to cache failed`)
			}
		case *consumer.PartitionEnd:
			logger.DefaultLogger.Info(`k-stream.changelog.replicaSyncer`, fmt.Sprintf(`replica sync completed for [%s]`, rs.tp))
		case *consumer.Error:
			logger.DefaultLogger.Error(`k-stream.changelog.replicaSyncer`, err)
		}
	}

	close(started)
	close(syncErrors)
	rs.syncing = false
}

func (rs *replicaSyncer) Stop() error {
	return rs.consumer.Close()
}
