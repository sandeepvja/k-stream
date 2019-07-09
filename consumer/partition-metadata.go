package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/k-stream/logger"
)

type groupOffsetMeta struct {
	admin  sarama.ClusterAdmin
	logger logger.Logger
	group  string
}

func newPartitionMeta(conf *Config, logger logger.Logger) (*groupOffsetMeta, error) {
	admin, err := sarama.NewClusterAdmin(conf.BootstrapServers, &conf.Config.Config)
	if err != nil {
		return nil, errors.WithPrevious(err, `consumer.offsetMeta`, `admin client failed`)
	}

	return &groupOffsetMeta{
		admin:  admin,
		group:  conf.GroupId,
		logger: logger,
	}, nil
}

func (m *groupOffsetMeta) Wait(tps []TopicPartition, consumerHost string) error {
	// get host information for partitions
	meta, err := m.fetchMeta(tps, consumerHost)
	if err != nil {
		return errors.WithPrevious(err, `consumer.offsetMeta.Wait`, `fetch metadata failed`)
	}
	if len(meta) < 1 {
		return nil
	}

	m.logger.Info(`consumer.offsetMeta.fetchMeta`, `waiting for remote partitions until they finish processing`)
	defer m.logger.Info(`consumer.offsetMeta.fetchMeta`, `remote partitions waiting is done`)
	return requestPartitionMeta(meta, m.logger)
}

func (m *groupOffsetMeta) fetchMeta(tps []TopicPartition, consumerHost string) (map[string][]TopicPartition, error) {

	saramaTps := make(map[string][]int32)

	for _, tp := range tps {
		saramaTps[tp.Topic] = append(saramaTps[tp.Topic], tp.Partition)
	}

	m.logger.Info(`consumer.offsetMeta.fetchMeta`, fmt.Sprintf(`requesting offset meta for %+v`, tps))
	res, err := m.admin.ListConsumerGroupOffsets(m.group, saramaTps)
	if err != nil {
		return nil, errors.WithPrevious(err, `consumer.offsetMeta.fetchMeta`, `offset meta request failed`)
	}

	ownerships := make(map[string][]TopicPartition)

	for _, tp := range tps {
		meta := res.GetBlock(tp.Topic, tp.Partition)
		if meta == nil || meta.Metadata == `` {
			m.logger.Warn(`consumer.offsetMeta.fetchMeta`, fmt.Sprintf(`hostdata not available for %s`, tp))
			continue
		}
		host := meta.Metadata
		if host == consumerHost {
			continue
		}
		ownerships[host] = append(ownerships[host], tp)
	}
	m.logger.Info(`consumer.offsetMeta.fetchMeta`, fmt.Sprintf(`partition ownership details %+v`, ownerships))
	return ownerships, nil
}
