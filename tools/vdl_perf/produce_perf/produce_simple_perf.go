package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
)

var (
	topic       *string = flag.String("topic", "logstream1", "the topic of VDL")
	broker      *string = flag.String("broker", "127.0.0.1:8181", "broker ip:port")
	messageSize *int    = flag.Int("message_size", 500, "The size of message")
)

const testLetterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// create size = n random string
func testRandStringBytes(n int) string {
	if n <= 0 {
		n = 1
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = testLetterBytes[rand.Intn(len(testLetterBytes))]
	}
	return string(b)
}

func main() {

	flag.Parse()

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	//	config.Producer.Flush.Bytes = 16384 //batch.size
	//config.Producer.Return.Errors = true
	brokers := make([]string, 0, 1)
	brokers = append(brokers, *broker)
	producer, _ := sarama.NewSyncProducer(brokers, config)

	for {
		msg := &sarama.ProducerMessage{
			Topic:     *topic,
			Partition: 0,
			Key:       sarama.StringEncoder(""),
			Value:     sarama.ByteEncoder([]byte(testRandStringBytes(*messageSize))),
		}
		<-time.After(time.Second)
		startTime := time.Now()
		partition, offset, err := producer.SendMessage(msg)
		perDuration := time.Since(startTime)
		fmt.Printf("send to paration %d, offset:%d, err:%v, used %v\n", partition, offset, err, perDuration)
	}

}
