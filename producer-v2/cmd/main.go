package main

import (
	"fmt"
	"log"
	"study/producer-v2/internal/adapters/kafkaproducer"
)

var (
	addr  = []string{"localhost:9091", "localhost:9092", "localhost:9093"}
	topic = "message"
)

func main() {

	// Prepare
	p, err := kafkaproducer.New(addr, topic)
	if err != nil {
		log.Fatalf("Fault create producer: <%v>", err)
	}
	defer p.Close()

	// Keys
	keys, err := kafkaproducer.KeysGenerate(3)
	if err != nil {
		log.Fatalf("Fault generate keys: <%v>", err)
	}

	// Send message with key
	for k := 0; k < len(keys); k++ {

		for i := 0; i < 10; i++ {
			key := keys[k]
			msg := fmt.Sprintf("Message numb <%d>, with key <%s>", i, key)
			if err := p.SendMessage(msg, key); err != nil {
				log.Fatalf("Error: <%v>, produce message numb <%d>, with key <%s>", err, i, key)
			}
		}
	}

	fmt.Println("Tx SUCCESS")
}
