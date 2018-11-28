package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_10_0_0
	// Specify brokers address. This is default one
	brokers := []string{"127.0.0.1:8000"}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	topic := "logstream1"
	// How to decide partition, is it fixed value...?
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0
	start := time.Now()
	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case <-consumer.Messages():
				msgCount++
				if msgCount >= 300000 {
					duration := time.Since(start)
					seconds := duration.Seconds()
					fmt.Printf("total latency is:%.2f ms\n",
						float64(seconds*1000)/300000)
					fmt.Printf("tps is %.2f\n", float64(300000)/seconds)
					break
				}
			}
			if msgCount > 300000 {
				doneCh <- struct{}{}
				break
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}
