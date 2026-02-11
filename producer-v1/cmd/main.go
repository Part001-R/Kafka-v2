package main

import (
	"fmt"
	"log"
	"study/producer-v1/internal/adapters/kafkaproducer"
)

var (
	addr = []string{"localhost:9091", "localhost:9092", "localhost:9093"}
)

func main() {

	// Prepare
	p0, err := kafkaproducer.New(addr, "message", int32(0))
	if err != nil {
		log.Fatalf("Fault create producer: <%v>", err)
	}
	defer p0.Close()

	p1, err := kafkaproducer.New(addr, "message", int32(1))
	if err != nil {
		log.Fatalf("Fault create producer: <%v>", err)
	}
	defer p1.Close()

	// Send message
	msg := "Message 1 for partition-0"
	key := "0"
	if err := p0.SendMessage(msg, key); err != nil {
		log.Fatalf("Error produce message-1 p0 <%v>", err)
	}

	msg = "Message 2 for partition-0"
	key = "10"
	if err := p0.SendMessage(msg, key); err != nil {
		log.Fatalf("Error produce message-2 p0 <%v>", err)
	}

	msg = "Message 1 for partition-1"
	key = "100"
	if err := p1.SendMessage(msg, key); err != nil {
		log.Fatalf("Error produce message-1 p1 <%v>", err)
	}

	fmt.Println("Tx SUCCESS")
}
