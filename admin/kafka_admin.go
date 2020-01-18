/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package admin

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/pickme-go/errors"
	"github.com/pickme-go/log/v2"
)

type Partition struct {
	Id    int32
	Error error
}

type Topic struct {
	Name              string
	Partitions        []Partition
	Error             error
	NumPartitions     int32
	ReplicationFactor int16
	ReplicaAssignment map[int32][]int32
	ConfigEntries     map[string]string
}

type KafkaAdmin interface {
	FetchInfo(topics []string) (map[string]*Topic, error)
	CreateTopics(topics map[string]*Topic) error
	DeleteTopics(topics []string) (map[string]error, error)
	Close()
}

type KafkaAdminConfig struct {
	BootstrapServers []string
	KafkaVersion     sarama.KafkaVersion
	Logger           log.Logger
}

type kafkaAdmin struct {
	admin  sarama.ClusterAdmin
	logger log.Logger
}

func NewKafkaAdmin(config *KafkaAdminConfig) *kafkaAdmin {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = config.KafkaVersion
	logger := config.Logger.NewLog(log.Prefixed(`kafka-admin`))
	admin, err := sarama.NewClusterAdmin(config.BootstrapServers, saramaConfig)
	if err != nil {
		logger.Fatal(fmt.Sprintf(`cannot get controller - %+v`, err))
	}

	return &kafkaAdmin{
		admin:  admin,
		logger: logger,
	}
}

func (c *kafkaAdmin) FetchInfo(topics []string) (map[string]*Topic, error) {
	topicInfo := make(map[string]*Topic)
	topicMeta, err := c.admin.DescribeTopics(topics)
	if err != nil {
		return nil, errors.WithPrevious(err, `cannot get metadata : `)
	}
	for _, tp := range topicMeta {

		var pts []Partition
		for _, pt := range tp.Partitions {
			pts = append(pts, Partition{
				Id:    pt.ID,
				Error: pt.Err,
			})
		}
		topicInfo[tp.Name] = &Topic{
			Name:          tp.Name,
			Partitions:    pts,
			NumPartitions: int32(len(pts)),
		}
		if tp.Err != sarama.ErrNoError {
			topicInfo[tp.Name].Error = tp.Err
		}
	}

	return topicInfo, nil
}

func (c *kafkaAdmin) CreateTopics(topics map[string]*Topic) error {
	for _, info := range topics {
		details := &sarama.TopicDetail{
			NumPartitions:     info.NumPartitions,
			ReplicationFactor: info.ReplicationFactor,
			ReplicaAssignment: info.ReplicaAssignment,
		}
		details.ConfigEntries = map[string]*string{}
		for name, config := range info.ConfigEntries {
			details.ConfigEntries[name] = &config
		}

		err := c.admin.CreateTopic(info.Name, details, false)
		if err != nil {
			if e, ok := err.(sarama.KError); ok && e == sarama.ErrTopicAlreadyExists || e == sarama.ErrNoError {
				c.logger.Warn(err)
				continue
			}
			return errors.WithPrevious(err, `could not create topic`)
		}
	}

	c.logger.Info(`k-stream.kafkaAdmin`,
		fmt.Sprintf(`kafkaAdmin topics created - %+v`, topics))

	return nil
}

func (c *kafkaAdmin) DeleteTopics(topics []string) (map[string]error, error) {
	for _, topic := range topics {
		err := c.admin.DeleteTopic(topic)
		if err != nil {
			return nil, errors.WithPrevious(err, `could not delete topic :`)
		}
	}

	return make(map[string]error), nil
}

func (c *kafkaAdmin) Close() {
	if err := c.admin.Close(); err != nil {
		c.logger.Warn(`k-stream.kafkaAdmin`,
			fmt.Sprintf(`kafkaAdmin cannot close broker : %+v`, err))
	}
}
