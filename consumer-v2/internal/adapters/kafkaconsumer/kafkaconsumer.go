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

	// Lost of partitions.
	partitions := []kafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: 0,
			Offset:    kafka.OffsetStored,
		},
	}

	// Assign partition list.
	if err := c.Assign(partitions); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("Fault <%w> assign partitions", err)
	}

	return &Consumer{
		consumer:       c,
		handler:        hndl,
		consumerNumber: consumerNumber,
	}, nil
}
