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
	"time"
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
	broker *sarama.Broker
	client sarama.Client
	logger log.Logger
}

func NewKafkaAdmin(config *KafkaAdminConfig) *kafkaAdmin {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = config.KafkaVersion
	logger := config.Logger.NewLog(log.Prefixed(`kafka-admin`))
	client, err := sarama.NewClient(config.BootstrapServers, saramaConfig)
	if err != nil {
		logger.Fatal(fmt.Sprintf(`cannot initiate builder deu to [%+v]`, err))
	}
	controller, err := client.Controller()
	if err != nil {
		logger.Fatal(fmt.Sprintf(`cannot get controller - %+v`, err))
	}

	return &kafkaAdmin{
		client: client,
		broker: controller,
		logger: logger,
	}
}

func (c *kafkaAdmin) FetchInfo(topics []string) (map[string]*Topic, error) {
	req := new(sarama.MetadataRequest)
	req.Topics = topics

	res, err := c.broker.GetMetadata(req)
	if err != nil {
		return nil, errors.WithPrevious(err, `cannot get metadata : `)
	}

	topicInfo := make(map[string]*Topic)
	for _, tp := range res.Topics {
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
	req := new(sarama.CreateTopicsRequest)
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

		req.TopicDetails = map[string]*sarama.TopicDetail{}
		req.TopicDetails[info.Name] = details
	}
	req.Timeout = 30 * time.Second
	res, err := c.broker.CreateTopics(req)
	if err != nil {
		return errors.WithPrevious(err, `could not create topic`)
	}

	for _, err := range res.TopicErrors {
		if err.Err == sarama.ErrTopicAlreadyExists {
			c.logger.Warn(err.Err)
			return nil
		}

		if err.Err == sarama.ErrNoError {
			c.logger.Warn(err.Err.Error())
			return nil
		}

		c.logger.Fatal(err.Err)
	}

	if len(res.TopicErrors) < 1 {
		c.logger.Info(`k-stream.kafkaAdmin`,
			fmt.Sprintf(`kafkaAdmin topics created - %+v`, topics))
	}

	return err
}

func (c *kafkaAdmin) DeleteTopics(topics []string) (map[string]error, error) {
	req := new(sarama.DeleteTopicsRequest)
	req.Topics = topics

	errs := make(map[string]error)
	res, err := c.broker.DeleteTopics(req)
	if err != nil {
		return nil, errors.WithPrevious(err, `could not delete topic :`)
	}

	for topic, err := range res.TopicErrorCodes {
		errs[topic] = err
	}

	return errs, err
}

func (c *kafkaAdmin) Close() {
	if err := c.broker.Close(); err != nil {
		c.logger.Warn(`k-stream.kafkaAdmin`,
			fmt.Sprintf(`kafkaAdmin cannot close broker : %+v`, err))
	}
}
