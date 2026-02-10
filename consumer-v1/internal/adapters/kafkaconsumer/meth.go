package kafkaconsumer

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// Start processing
func (c *Consumer) Start() {
	for !c.stop {

		kafkaMessage, err := c.consumer.ReadMessage(-1) // wait new message
		if err != nil {
			logrus.Errorf("error read message <%v>", err)
			continue
		}
		if kafkaMessage == nil {
			continue
		}

		// Processing message
		if err := c.handler.HandleMessage(kafkaMessage.Value, kafkaMessage.TopicPartition.Offset, c.consumerNumber); err != nil {
			logrus.Error("HandleMessage return error <%w>", err)
			continue
		}

		// Commit
		if _, err := c.consumer.CommitMessage(kafkaMessage); err != nil {
			logrus.Errorf("Failed to commit offset %v: %v", kafkaMessage.TopicPartition.Offset, err)
			continue
		}
	}
}

// Stop Processing.
func (c *Consumer) Stop() error {
	c.stop = true
	logrus.Info("Initiating consumer shutdown...")

	// Close connect
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		err := c.consumer.Close()
		errChan <- err
	}()

	// Wait result
	select {
	case closeErr := <-errChan:
		if closeErr != nil {
			logrus.Errorf("Failed to close consumer: %v", closeErr)
			return fmt.Errorf("close error: %w", closeErr)
		}
		logrus.Info("Consumer closed successfully")
		return nil

	case <-ctx.Done():
		logrus.Warn("Consumer Close() timed out after 10s. Forcing shutdown.")
		return nil
	}
}
