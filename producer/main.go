package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("failed to start kafka producer: %v", err)
	}

	defer func() {
		log.Println("defer func producer.Close()")
		if err := producer.Close(); err != nil {
			log.Fatalf("failed to close kafka producer: %v", err)
		}
	}()

	// trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)

	// start producing messages in a separate goroutine
	go func() {
		for {
			message := &sarama.ProducerMessage{
				Topic: "my-topic",
				Value: sarama.StringEncoder("hello from kafka"),
			}

			select {
			case producer.Input() <- message:
				log.Println("producer: attempting to enqueue message:", message.Value)
			case <-signals:
				return
			}

			time.Sleep(1 * time.Second)
		}
	}()

	// consume from the Successes and Errors channels
	for {
		select {
		case producerSuccess := <-producer.Successes():
			if producerSuccess != nil {
				log.Printf("producer: successfully enqueued message: %s\n", producerSuccess.Value)
			}
		case producerError := <-producer.Errors():
			if producerError != nil {
				log.Printf("producer: failed to enqueue message: error: %v", producerError)
			}
		case <-signals:
			log.Println("received signal...")

			close(signals)

			// AsyncClose triggers a shutdown of the producer.
			// The shutdown has completed when both the Errors and Successes channels have been closed.
			// When calling AsyncClose, you *must* continue to read from those channels in order to drain the results of any messages in flight.
			// producer.AsyncClose()

			return
		}
	}
}
