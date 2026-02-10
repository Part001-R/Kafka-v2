package kafkaconsumer

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer struct {
	consumer       *kafka.Consumer
	handler        *Handler
	stop           bool
	consumerNumber int
}

// Constructor.
func NewConsumer(hndl *Handler, addr []string, topic, group string, timeout int, consumerNumber int) (*Consumer, error) {

	conf := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(addr, ","),
		"group.id":                 group,
		"session.timeout.ms":       timeout,
		"enable.auto.offset.store": false,
		"auto.offset.reset":        "earliest",
	}

	c, err := kafka.NewConsumer(conf)
	if err != nil {
		return nil, fmt.Errorf("Error NewConsumer <%w>", err)
	}

	if err := c.Subscribe(topic, nil); err != nil {
		return nil, fmt.Errorf("Fault <%w> subscribe topic <%s>", err, topic)
	}

	return &Consumer{
		consumer:       c,
		handler:        hndl,
		consumerNumber: consumerNumber,
	}, nil
}
