/**
 * Copyright 2018 PickMe (Digital Mobility Solutions Lanka (PVT) Ltd).
 * All rights reserved.
 * Authors:
 *    Gayan Yapa (gayan@pickme.lk)
 */

package topic_manager

import (
	"github.com/Shopify/sarama"
	"github.com/pickme-go/k-stream/logger"

	"fmt"

	"github.com/pickme-go/errors"
	"time"
)

type TopicManagerBuilder func() *TopicManager

var DefaultTopicManager TopicManagerBuilder = func() *TopicManager {
	panic(`[TopicManager.DefaultTopicManager] is empty`)
}

type TopicMeta struct {
	Topic     string
	Partition []int32
}

type ITopicManager interface {
	FetchInfo(topics []string) (*sarama.MetadataResponse, error)
	CreateTopics(topics map[string]*sarama.TopicDetail) error
}

type TopicManager struct {
	broker           *sarama.Broker
	client           sarama.Client
	currentBrokerIdx int
}

func NewTopicManager(client sarama.Client) *TopicManager {

	controller, err := client.Controller()
	if err != nil {
		logger.DefaultLogger.Fatal(`k-stream.TopicManager`, fmt.Sprintf(`cannot get controller - %+v`, err))
	}

	tpManager := &TopicManager{
		client: client,
		broker: controller,
	}

	return tpManager
}

func (c *TopicManager) FetchInfo(topics []string) (*sarama.MetadataResponse, error) {

	req := new(sarama.MetadataRequest)
	req.Topics = topics

	res, err := c.broker.GetMetadata(req)
	if err != nil {
		return nil, errors.WithPrevious(err, `k-stream.TopicManager`, `cannot get metadata : `)
	}

	return res, nil
}

func (c *TopicManager) CreateTopics(topics map[string]*sarama.TopicDetail) error {

	req := new(sarama.CreateTopicsRequest)
	req.TopicDetails = topics
	req.Timeout = 30 * time.Second
	res, err := c.broker.CreateTopics(req)
	if err != nil {
		return errors.WithPrevious(err, `k-stream.TopicManager`, `could not create topic :`)
	}

	for _, err := range res.TopicErrors {
		if err.Err == sarama.ErrTopicAlreadyExists {
			logger.DefaultLogger.Warn(`k-stream.TopicManager`, err.Err)
			return nil
		}

		if err.Err == sarama.ErrNoError {
			logger.DefaultLogger.Warn(`k-stream.TopicManager`, err.Err.Error())
			return nil
		}

		logger.DefaultLogger.Fatal(`k-stream.TopicManager`, err.Err)
	}

	if len(res.TopicErrors) < 1 {
		logger.DefaultLogger.Info(`k-stream.TopicManager`,
			fmt.Sprintf(`TopicManager topics created - %+v`, topics))
	}

	return err
}

func (c *TopicManager) DeleteTopics(topics []string) (*sarama.DeleteTopicsResponse, error) {

	req := new(sarama.DeleteTopicsRequest)
	req.Topics = topics

	res, err := c.broker.DeleteTopics(req)
	if err != nil {
		return nil, errors.WithPrevious(err, `k-stream.TopicManager`, `could not delete topic :`)
	}

	return res, err
}

func (c *TopicManager) Close() {
	if err := c.broker.Close(); err != nil {
		logger.DefaultLogger.Warn(`k-stream.TopicManager`,
			fmt.Sprintf(`TopicManager cannot close broker : %+v`, err))
	}
}
