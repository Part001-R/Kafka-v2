package main

import (
	"os"
	"os/signal"
	"syscall"

	"study/consumer-v3/internal/adapters/kafkaconsumer"

	"github.com/sirupsen/logrus"
)

var (
	addr          = []string{"localhost:9091", "localhost:9092", "localhost:9093"}
	topic         = "message"
	consumerGroup = "consumer-1"
)

func main() {
	h := kafkaconsumer.NewHandler()
	c, err := kafkaconsumer.NewConsumer(h, addr, topic, consumerGroup, 7000, 1)
	if err != nil {
		logrus.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	go c.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logrus.Info("Received shutdown signal (SIGINT/SIGTERM). Initiating graceful shutdown...")

	if err := c.Stop(); err != nil {
		logrus.Errorf("Error during consumer shutdown: %v", err)
	}

	logrus.Info("Application terminated.")
}
