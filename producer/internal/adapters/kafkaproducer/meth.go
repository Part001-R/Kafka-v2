package kafkaproducer

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Send message. Return error.
//
// Params:
//
//	message - tx message.
//	key - key of message.
func (p *Producer) SendMessage(message, key string) error {

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: p.partition,
		},
		Value:     []byte(message),
		Key:       []byte(key),
		Timestamp: time.Now(),
	}

	rxChanStatus := make(chan kafka.Event)

	if err := p.producer.Produce(msg, rxChanStatus); err != nil {
		return fmt.Errorf("Function Produce, return error: <%w>", err)
	}

	event := <-rxChanStatus

	switch rx := event.(type) {
	case *kafka.Message:
		if rx.TopicPartition.Error != nil {
			return fmt.Errorf("Failed to produce message: <%w>", rx.TopicPartition.Error)
		}
		return nil
	case kafka.Error:
		return fmt.Errorf("Fault produce message <%w>", rx)
	default:
		return ErrUnknownType
	}
}

// Close connect
func (p *Producer) Close() {

	p.producer.Flush(p.flushTime)
	p.producer.Close()
}
