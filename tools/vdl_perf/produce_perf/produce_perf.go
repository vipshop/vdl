package main

import (
	"flag"
	"math/rand"

	"sync"

	"fmt"

	"time"

	"sort"

	"github.com/Shopify/sarama"
)

var (
	messages    *int     = flag.Int("messages", 32000, "the number of messages")
	batch       *int     = flag.Int("batch", 32, "The amount of message batch send")
	messageSize *int     = flag.Int("message_size", 500, "The size of message")
	topic       *string  = flag.String("topic", "logstream1", "the topic of VDL")
	thread      *int     = flag.Int("thread", 1, "the number of messages")
	sampling    *int     = flag.Int("sampling", 100, "The amount of sampling") //采样
	broker      *string  = flag.String("broker", "127.0.0.1:8181", "broker ip:port")
	rtt         *float64 = flag.Float64("rtt", 0.01, "the rtt between producer and VDL")
)

const testLetterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type SamplingPoint struct {
	messageCount int
	messageSize  int
	costTime     float64
}

type SpSlice []*SamplingPoint

func (s SpSlice) Len() int {
	return len(s)
}

func (s SpSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SpSlice) Less(i, j int) bool {
	return (s[i].costTime-(*rtt))/float64(s[i].messageCount) < (s[j].costTime-(*rtt))/float64(s[j].messageCount)
}

func main() {
	var err error
	flag.Parse()
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	//config.Producer.Flush.Bytes = 16384 //in MacOS need comment! batch.size
	//config.Producer.Return.Errors = true
	brokers := make([]string, 0, 1)
	brokers = append(brokers, *broker)
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		// Should not reach here
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			// Should not reach here
			panic(err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(*thread)
	for i := 0; i < *thread; i++ {
		go func() {
			var totalDuration float64     //生产*messages条消息总耗时(ms)
			var totalSize int64           //*messages条消息总大小
			var perDuration time.Duration //一次采用耗时
			//采样次数为:*sampling
			samplingPoints := make([]*SamplingPoint, 0, *sampling) //采样点
			perMessageCount := (*messages) / (*sampling)           //每发送perMessageCount条消息作为一个采样点
			//sendCount表示一次采用需要调用SendMessages的次数
			sendCount := perMessageCount / (*batch)
			//*sampling为采样次数
			for j := 0; j < (*sampling); j++ {
				//一次待发送的message
				sendMsgs := NewMessage(*messageSize, perMessageCount)
				startTime := time.Now()
				for i := 0; i < sendCount; i++ {
					err = producer.SendMessages(sendMsgs[i*(*batch) : (i+1)*(*batch)])
					if err != nil {
						fmt.Printf("SendMessages message error,err=%s", err.Error())
						wg.Done()
						return
					}
				}
				perDuration = time.Since(startTime)
				sp := &SamplingPoint{
					messageCount: perMessageCount,
					messageSize:  perMessageCount * (*messageSize),                   //采样的消息总大小
					costTime:     float64(perDuration.Nanoseconds()) / (1000 * 1000), //采样的耗时(ms)
				}
				samplingPoints = append(samplingPoints, sp)
				totalDuration += float64(perDuration.Nanoseconds()) / (1000 * 1000) //总时间(ms)
				totalSize += int64(sp.messageSize)                                  //更新总大小
			}
			avgTps := float64(*messages) / (totalDuration / 1000)
			//MB/s
			avgThroughput := (float64(totalSize) / (1000 * 1000)) / (totalDuration / 1000)
			//95th/99th/max latency
			pos95 := int(float64(len(samplingPoints)) * float64(0.95))
			pos99 := int(float64(len(samplingPoints)) * float64(0.99))
			posMax := len(samplingPoints) - 1
			sort.Sort(SpSlice(samplingPoints))
			lantency95 := float64(samplingPoints[pos95].costTime-(*rtt))/float64(samplingPoints[pos95].messageCount) + (*rtt)
			lantency99 := float64(samplingPoints[pos99].costTime-(*rtt))/float64(samplingPoints[pos99].messageCount) + (*rtt)
			lantencyMax := float64(samplingPoints[posMax].costTime-(*rtt))/float64(samplingPoints[posMax].messageCount) + (*rtt)
			avgLatency := float64(totalDuration-float64(sendCount)*float64(*sampling)*(*rtt))/float64(*messages) + (*rtt)
			fmt.Printf("thread[%d]:message_count:%d,totalDuration=%.2fs,sampling=%d,perMessageCount=%d,batch=%d,sendCount=%d\n"+
				"avg tps:%.2f ,avg throughput:%.2fMB/s\n"+
				"avg_latency=%.2f ms,latency95:%.2f ms ,latency99:%.2fms ,latencyMax:%.2fms\n",
				i, *messages, totalDuration/1000, *sampling, perMessageCount, *batch, sendCount,
				avgTps, avgThroughput, avgLatency, lantency95, lantency99, lantencyMax)
			wg.Done()
		}()
	}
	wg.Wait()
}

// create size = n random string
func testRandStringBytes(n int) []byte {
	if n <= 0 {
		n = 1
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = testLetterBytes[rand.Intn(len(testLetterBytes))]
	}
	return b
}

func NewMessage(size, count int) []*sarama.ProducerMessage {
	msgs := make([]*sarama.ProducerMessage, 0, count)
	for i := 0; i < count; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     *topic,
			Partition: 0,
			Key:       sarama.StringEncoder(""),
			Value:     sarama.ByteEncoder(testRandStringBytes(size)),
		}
		msgs = append(msgs, msg)
	}
	return msgs
}
