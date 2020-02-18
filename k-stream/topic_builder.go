package kstream

import (
	"fmt"
	"github.com/pickme-go/k-stream/admin"
	"github.com/pickme-go/log/v2"
)

type topicBuilder struct {
	topics map[string]*admin.Topic
	admin  admin.KafkaAdmin
	logger log.Logger
}

func (tb *topicBuilder) apply(config *admin.Topic) {
	if _, ok := tb.topics[config.Name]; ok {
		tb.logger.Fatal(fmt.Sprintf(`topic [%s] already exst`, config.Name))
	}

	tb.topics[config.Name] = config
}

func (tb *topicBuilder) build() {
	if len(tb.topics) < 1 {
		return
	}

	tb.logger.Info(`creating changelog topics...`)

	if err := tb.admin.CreateTopics(tb.topics); err != nil {
		tb.logger.Fatal(err)
	}
}
