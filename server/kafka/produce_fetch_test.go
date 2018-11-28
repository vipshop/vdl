// Copyright 2018 vip.com.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/vipshop/vdl/server/kafka/handler"
)

//only has a topic[logstream1] by default, so we only use this topic
var kafkaAddr string = "127.0.0.1:8181"

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

func newMockKafkaServer(t *testing.T) *Server {
	mockVdlServer, err := handler.NewMockVDLServer()
	if err != nil {
		t.Fatalf("NewMockVDLServer error:%s", err.Error())
	}
	// new kafka server
	kafkaServer, err := NewServer(kafkaAddr, 600000, mockVdlServer,nil)
	if err != nil {
		t.Fatalf("kafka.NewServer error:%s", err.Error())
	}
	return kafkaServer
}

func produceMessage(config *sarama.Config, t *testing.T, messageSize int, expectErrorStr string) {
	//config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.MaxMessageBytes = 30 * 1024 * 1024
	config.Version = sarama.V0_10_0_0
	brokers := []string{"127.0.0.1:8181"}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		t.Fatalf("sarama.NewAsyncProducer error")
	}

	defer func() {
		if err := producer.Close(); err != nil {
			t.Fatalf("producer.Close error:%s", err.Error())
		}
	}()
	for i := 0; i < 10; i++ {
		strTime := strconv.Itoa(int(time.Now().Unix()))
		msg := &sarama.ProducerMessage{
			Topic: "logstream1",
			Key:   sarama.StringEncoder(strTime),
			Value: sarama.StringEncoder(testRandStringBytes(messageSize)),
		}
		producer.Input() <- msg
		select {
		case <-producer.Successes():
			if len(expectErrorStr) > 0 {
				t.Fatalf("expect error %v, but actually success", expectErrorStr)
			}
		case err := <-producer.Errors():
			if strings.Index(err.Error(), expectErrorStr) == -1 {
				t.Fatalf("expect error %v, but actually %v", expectErrorStr, err)
			}
		}
	}
}

func consumeMessage(config *sarama.Config, consumeCount int32, t *testing.T) {
	if consumeCount <= 0 {
		t.Fatalf("consumeCount is %d", consumeCount)
	}
	//config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_10_0_0
	// Specify brokers address. This is default one
	brokers := []string{"127.0.0.1:8181"}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		t.Fatalf("sarama.NewConsumer error:%s", err.Error())
	}
	defer func() {
		if err := master.Close(); err != nil {
			t.Fatalf("master.Close error:%s", err.Error())
		}
	}()

	topic := "logstream1"
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("master.ConsumePartition error:%s", err.Error())
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			t.Fatalf("producer.Close error:%s", err.Error())
		}
	}()

	// Count how many message processed
	var msgCount int32 = 0
	select {
	case err := <-consumer.Errors():
		t.Fatalf("consumer.Error:%s", err.Error())
	case msg := <-consumer.Messages():
		msgCount++
		t.Log("Received messages", string(msg.Key), string(msg.Value))
		return
	}
}

func TestProduceMessage(t *testing.T) {
	kafkaServer := newMockKafkaServer(t)
	go kafkaServer.Start()
	defer kafkaServer.Close()
	config := sarama.NewConfig()
	produceMessage(config, t, 1024, "")
}

func TestProduceLargeMessage(t *testing.T) {
	kafkaServer := newMockKafkaServer(t)
	go kafkaServer.Start()
	defer kafkaServer.Close()
	config := sarama.NewConfig()
	produceMessage(config, t, 5*1024*1024, "Message was too large")
}

//sarama can't produce and consume in one function
func TestConsumeMessage(t *testing.T) {
	kafkaServer := newMockKafkaServer(t)
	go kafkaServer.Start()
	defer kafkaServer.Close()

	config := sarama.NewConfig()
	consumeMessage(config, 1, t)
}
