package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("failed to start kafka consumer: %v", err)
	}

	defer func() {
		log.Println("defer func consumer.Close()")
		if err := consumer.Close(); err != nil {
			log.Fatalf("failed to close kafka consumer: %v", err)
		}
	}()

	topic := "my-topic"

	// subscribe to the topic
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("failed to start consumer for partition: %v", err)
	}
	defer partitionConsumer.AsyncClose()

	// trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)

	log.Println("Consumer is ready and listening for messages...")

	// consume messages
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Received message: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		case err := <-partitionConsumer.Errors():
			log.Println("Failed to receive message:", err)
		case <-signals:
			log.Println("received signal...")

			close(signals)

			// AsyncClose initiates a shutdown of the PartitionConsumer.
			// This method will return immediately, after which you should continue to service the 'Messages' and 'Errors' channels until they are empty.
			// It is required to call this function, or Close before a consumer object passes out of scope, as it will otherwise leak memory.
			// You must call this before calling Close on the underlying client.

			// partitionConsumer.AsyncClose()

			return
		}
	}
}
