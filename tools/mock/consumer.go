package main

import (
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"strings"

	"fmt"

	"time"

	"github.com/Shopify/sarama"
)

var (
	ctype  *string = flag.String("ctype", "s", "consume type:random[r] or sequential[s]")
	offset *string = flag.String("offset", "newest", "if ctype is sequential,offset is the position of start consume(newest/oldest)")
	topic  *string = flag.String("topic", "logstream1", "the topic of VDL")
	broker *string = flag.String("broker", "192.168.0.1:8200", "broker ip:port[;ip port]")
	print  *bool   = flag.Bool("print", false, "print consume message or not")
)

func main() {
	flag.Parse()
	if *ctype != "r" && *ctype != "s" {
		PrintHelp()
		return
	}
	if *offset != "newest" && *offset != "oldest" {
		PrintHelp()
		return
	}
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_10_0_0

	// Specify brokers address. This is default one
	brokers := make([]string, 0, 1)
	configBrokers := strings.Split(*broker, ";")
	for _, b := range configBrokers {
		brokers = append(brokers, b)
	}
	// Create new consumer
	client, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			panic(err)
		}
	}()

	//随机消费
	if *ctype == "r" {
		RandomConsume(client, brokers, *topic, *print)
	} else {
		//顺序消费
		SequentialConsume(client, *topic, *offset, *print)
	}
	return
}

func RandomConsume(c sarama.Consumer, brokers []string, topic string, print bool) {
	var partitionConsumer sarama.PartitionConsumer
	var err error
	// Count how many message processed

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		var processedCount, totalCount int64
		var offset, count int64
		var needNewPC bool //是否需要重新创建partition consumer
		var ticker *time.Ticker
		var newErr error

		ticker = time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		offset, count = GetOffsetAndCount(brokers, topic)
		partitionConsumer, err = c.ConsumePartition(topic, 0, offset)
		if err != nil {
			fmt.Printf("%s:[error_tag]:ConsumePartition[1] error:%s\n", GetTimeStr(), err.Error())
			return
		}
		for {
			select {
			case err := <-partitionConsumer.Errors():
				fmt.Printf("%s:[error_tag]:partitionConsumer error:%s\n", GetTimeStr(), err.Error())
				if Closeerr := partitionConsumer.Close(); Closeerr != nil {
					panic(Closeerr)
				}
				//重新生成一个partitionConsumer
				processedCount = 0
				offset, count = GetOffsetAndCount(brokers, topic)
				partitionConsumer, newErr = c.ConsumePartition(topic, 0, offset)
				if newErr != nil {
					panic(newErr)
				}
				needNewPC = false
			case msg := <-partitionConsumer.Messages():
				processedCount++
				totalCount++
				if print {
					fmt.Printf("Receive messages,key:%s,value:%v\n", string(msg.Key), msg.Value)
				}
				if count <= processedCount {
					needNewPC = true
				}
			case <-signals:
				fmt.Printf("%s:Interrupt is detected\n", GetTimeStr())
				if err := partitionConsumer.Close(); err != nil {
					panic(err)
				}
				doneCh <- struct{}{}
				return
			case <-ticker.C:
				fmt.Printf("%s:Received messages count is %d\n", GetTimeStr(), totalCount)
			}

			if needNewPC {
				//关闭partitionConsumer
				if err := partitionConsumer.Close(); err != nil {
					panic(err)
				}
				//重置processedCount
				processedCount = 0
				//重新创建partition consumer
				offset, count = GetOffsetAndCount(brokers, topic)
				partitionConsumer, err = c.ConsumePartition(topic, 0, offset)
				if err != nil {
					fmt.Printf("%s:[error_tag]:ConsumePartition[1] error:%s\n", GetTimeStr(), err.Error())
					return
				}
				needNewPC = false
			}
		}
	}()
	<-doneCh
	return
}

func SequentialConsume(c sarama.Consumer, topic string, offset string, print bool) {
	var partitionConsumer sarama.PartitionConsumer
	var err error
	// Count how many message processed
	var processedCount int64

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	if offset == "newest" {
		partitionConsumer, err = c.ConsumePartition(topic, 0, sarama.OffsetNewest)
	} else {
		partitionConsumer, err = c.ConsumePartition(topic, 0, sarama.OffsetOldest)
	}
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			panic(err)
		}
	}()

	// Get signnal for finish
	doneCh := make(chan struct{})
	var ticker *time.Ticker
	ticker = time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case err := <-partitionConsumer.Errors():
				fmt.Printf("%s:[error_tag]:partitionConsumer error:%s\n", GetTimeStr(), err.Error())
				//退出
				doneCh <- struct{}{}
				return
			case msg := <-partitionConsumer.Messages():
				processedCount++
				if print {
					fmt.Printf("Receive messages,key:%s,value:%v\n", string(msg.Key), msg.Value)
				}
			case <-signals:
				fmt.Printf("%s:Interrupt is detected\n", GetTimeStr())
				doneCh <- struct{}{}
				return
			case <-ticker.C:
				fmt.Printf("%s:Received messages count is %d\n", GetTimeStr(), processedCount)
			}
		}

	}()
	<-doneCh
	return
}

//返回随机读取的位置
func GetOffsetAndCount(brokers []string, topic string) (int64, int64) {
	var firstOffset, lastOffset int64
	var err error
	client, err := sarama.NewClient(brokers, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			panic(err)
		}
	}()
	firstOffset, err = client.GetOffset(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	lastOffset, err = client.GetOffset(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	return RandInt64(firstOffset, lastOffset)
}

func RandInt64(min, max int64) (int64, int64) {
	rand.Seed(time.Now().Unix())
	offset := rand.Int63n(max-min) + min
	count := max - offset
	return offset, count
}

func PrintHelp() {
	fmt.Printf("use ./consume --help for help\n example:./consume -ctype=r or ./consume -ctype=s -offset=newest(oldest)\n")
}

func GetTimeStr() string {
	timestamp := time.Now().Unix()
	tm := time.Unix(timestamp, 0)
	return tm.Format("2006-01-02 03:04:05 PM")
}
