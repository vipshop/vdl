package main

import (
	"fmt"
	"time"

	"flag"

	"sync"

	"sort"

	"github.com/Shopify/sarama"
)

var (
	messages  *int    = flag.Int("messages", 100000, "the number of messages")
	fetchSize *int    = flag.Int("fetch_size", 1048576, "The amount of data to fetch in a single request")
	topic     *string = flag.String("topic", "logstream1", "the topic of VDL")
	thread    *int    = flag.Int("thread", 1, "the number of messages")

	broker      *string  = flag.String("broker", "192.168.0.1:8000", "broker ip:port")
	offset      *int64   = flag.Int64("offset", 0, "the offset of consume") //读取消息序号
	rtt         *float64 = flag.Float64("rtt", 15.59, "the rtt between producer and VDL")
	messageSize *int     = flag.Int("message_size", 100, "The size of message")
)

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
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_10_0_0
	config.Consumer.Fetch.Default = int32(*fetchSize)
	flag.Parse()
	// Specify brokers address. This is default one
	brokers := make([]string, 0, 1)
	brokers = append(brokers, *broker)
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
	consumer, err := master.ConsumePartition(*topic, 0, *offset)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(*thread)
	for i := 0; i < *thread; i++ {
		go func() {
			var totalDuration float64 //消费*messages条消息总耗时(ms)
			var totalSize int64       //*messages条消息总大小
			var totalMessageCount int64
			var samplingRead int

			fetchMessagesCount := (*fetchSize) / (*messageSize) //一次批量接收的消息条数
			samplingCount := (*messages) / (fetchMessagesCount) //总共需要传输samplingCount次，samplingCount必须要大于0
			if samplingCount == 0 {
				fmt.Printf("message count is too small,messages=%d,fetchCount=%d", *messages, fetchMessagesCount)
				return
			}
			//采样次数为:samplingCount
			samplingPoints := make([]*SamplingPoint, 0, samplingCount) //采样点
			var durationMessageCount int
			var durationMessageSize int
			var perDuration time.Duration
			start := time.Now()
			for {
				select {
				case err := <-consumer.Errors():
					fmt.Printf("consumer message count is %d, but happen error:%s\n", totalMessageCount, err.Error())
					return
				case msg := <-consumer.Messages():
					durationMessageCount++
					durationMessageSize += len(msg.Key) + len(msg.Value)
					if durationMessageCount == fetchMessagesCount {
						perDuration = time.Since(start)
						sp := &SamplingPoint{
							messageCount: fetchMessagesCount,
							messageSize:  durationMessageSize,                                //每perMessageCount条message的总大小
							costTime:     float64(perDuration.Nanoseconds()) / (1000 * 1000), //获取perMessageCount的时间
						}
						samplingPoints = append(samplingPoints, sp)
						totalDuration += float64(perDuration.Nanoseconds()) / (1000 * 1000) //总时间(ms)
						totalSize += int64(durationMessageSize)
						totalMessageCount += int64(fetchMessagesCount)

						durationMessageSize = 0
						durationMessageCount = 0
						//reset start
						start = time.Now()
						samplingRead++
					}
				}
				//最后部分不读取，因为不能构成一次完整的批量相同大小message的读
				if samplingRead == samplingCount {
					break
				}
			}
			avgTps := float64(totalMessageCount) / (totalDuration / 1000)
			//MB/s
			avgThroughput := (float64(totalSize) / (1024 * 1024)) / (totalDuration / 1000)
			//95th/99th/max latency
			pos95 := int(float64(len(samplingPoints)) * float64(0.95))
			pos99 := int(float64(len(samplingPoints)) * float64(0.99))
			posMax := len(samplingPoints) - 1
			sort.Sort(SpSlice(samplingPoints))
			lantency95 := float64(samplingPoints[pos95].costTime-(*rtt))/float64(samplingPoints[pos95].messageCount) + (*rtt)
			lantency99 := float64(samplingPoints[pos99].costTime-(*rtt))/float64(samplingPoints[pos99].messageCount) + (*rtt)
			lantencyMax := float64(samplingPoints[posMax].costTime-(*rtt))/float64(samplingPoints[posMax].messageCount) + (*rtt)
			avgLatency := float64(totalDuration-(*rtt)*float64(samplingCount))/float64(totalMessageCount) + (*rtt)

			fmt.Printf("thread[%d]:message_count:%d,total_cost=%.2fms,fetch_count=%d,sampling_count=%d\n"+
				"avg tps:%.2f ,avg throughput:%.2fMB/s\n"+
				"avg_lantency:%.2fms ,latency95:%.2f ms ,latency99:%.2fms ,latencyMax:%.2fms\n",
				i, totalMessageCount, totalDuration, fetchMessagesCount, samplingCount,
				avgTps, avgThroughput, avgLatency, lantency95, lantency99, lantencyMax)
			wg.Done()
		}()
	}
	wg.Wait()
}
