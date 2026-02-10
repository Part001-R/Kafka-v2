package kafkaproducer

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer.
type Producer struct {
	producer  *kafka.Producer
	flushTime int
	topic     string
	partition int32
}

// Interface.
type ProducerI interface {
	// Produce message.
	SendMessage(message, key string) error
	// Close connect.
	Close()
}

// Constructor.
func New(addr []string, topic string, partition int32) (ProducerI, error) {

	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(addr, ","),
	}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, fmt.Errorf("Function NewProducer, return error:<%w>", err)
	}
	return &Producer{
		producer:  p,
		flushTime: 10000,
		topic:     topic,
		partition: partition,
	}, nil
}
