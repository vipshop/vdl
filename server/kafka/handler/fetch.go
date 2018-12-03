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

package handler

import (
	"net"
	"reflect"
	"strconv"
	"time"

	"encoding/binary"

	"github.com/vipshop/vdl/logstream"

	"context"
	"github.com/vipshop/vdl/consensus/raftgroup"
	"github.com/vipshop/vdl/logstore"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/server/kafka/kafkametrics"
	"github.com/vipshop/vdl/server/kafka/kafkaratequota"
	"github.com/vipshop/vdl/server/kafka/protocol"
)

type waitInfo struct {
	topic     string
	partition int32
	wrapper   raftgroup.LogStreamWrapper
}

func (h *KafkaAPIHandler) HandleFetch(conn net.Conn, header *protocol.RequestHeader, req *protocol.FetchRequest) error {

	beginTime := time.Now()
	var fetchCount int = 0

	h.printFetchRequest(req)

	resp := &protocol.FetchResponses{}
	resp.APIVersion = header.APIVersion

	// time out chan use for long polling
	timeWaitChan := time.After(time.Duration(req.MaxWaitTime) * time.Millisecond)

	// fast first fetch
	// in order to improve performance,
	// the first fetch operation should reduce operations relate to long polling
	// isReadFromMemory is just use for first fetch, because when first fetch exists in memory, the following will in memory
	firstFetchedBytes, firstFetchResp, isReadFromMemory := h.firstFetch(req, &fetchCount)

	resp.Responses = firstFetchResp
	//implement long polling
	if firstFetchedBytes < req.MinBytes {
		h.longPollingFetch(req, timeWaitChan, firstFetchedBytes, resp, &fetchCount)
	}

	h.printFetchResponse(resp)

	r := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          resp,
	}

	err := h.write(conn, r)

	// TODO. avoid data mem copy using h.write(conn, r)
	// implemented but test needed, remove implement after version 419b536e93573e8fe966269f8b3a41a8bb00588f

	// rate limit
	kafkaRate := kafkaratequota.GetKafkaRate()
	if kafkaRate.IsEnableRateQuota {
		if isReadFromMemory {
			kafkaRate.EndReadRequestLimiter.Wait(context.Background())
			kafkaRate.EndReadByteRateLimiter.WaitN(context.Background(), resp.FetchResponseSize())
		} else {
			kafkaRate.CatchupReadRequestLimiter.Wait(context.Background())
			kafkaRate.CatchupReadByteLimiter.WaitN(context.Background(), resp.FetchResponseSize())
		}
	}

	// metrics
	if fetchCount > 0 {
		kafkametrics.MetricsKsReadLatency.Observe(float64(time.Now().Sub(beginTime) / time.Millisecond))
		kafkametrics.MetricsKsReadLogTps.Add(float64(fetchCount))

	}

	return err
}

// in order to improve performance, the first fetch operation should reduce operations relate to long polling
// return : int32:fetchedByes
// bool return whether read from cache
func (h *KafkaAPIHandler) firstFetch(req *protocol.FetchRequest, fetchCount *int) (int32, []*protocol.FetchResponse, bool) {

	var fetchedBytes int32 = 0
	topicResps := make([]*protocol.FetchResponse, len(req.Topics))
	isReadFromMemory := false
	for i, reqTopic := range req.Topics {

		topicResps[i] = &protocol.FetchResponse{
			Topic: reqTopic.Topic,
		}

		// get raft group from fetch topic
		foundTopic := true
		wrapper, err := h.LogStreamWrapperGetter.GetLogStreamWrapper(reqTopic.Topic)
		if err != nil {
			foundTopic = false
			glog.Warning("HandleFetch, can't find topic:", reqTopic.Topic)
		}

		// handle partition for each
		topicResps[i].PartitionResponses = make([]*protocol.FetchPartitionResponse, len(reqTopic.Partitions))
		for j, reqPartition := range reqTopic.Partitions {

			topicResps[i].PartitionResponses[j] = &protocol.FetchPartitionResponse{
				Partition:     reqPartition.Partition,
				ErrorCode:     NONE,
				HighWatermark: 0,
				Messages:      make([]*protocol.FetchMessage, 0, 30),
			}

			// fast fail if topic error
			if !foundTopic {
				topicResps[i].PartitionResponses[j].ErrorCode = UNKNOWN_TOPIC_OR_PARTITION
				continue
			}

			// fast fail if not leader
			if !wrapper.IsLeader() {
				topicResps[i].PartitionResponses[j].ErrorCode = NOT_LEADER_FOR_PARTITION
				continue
			}

			topicResps[i].PartitionResponses[j].HighWatermark = wrapper.GetHighWater()

			// consume message from raft group
			entries, err, fromMemoryThisTime := wrapper.ConsumeMessages(reqPartition.FetchOffset, reqPartition.MaxBytes)
			if err != nil {
				glog.Error("HandleFetch, ConsumeMessages error:", err)
				if err == logstore.ErrOutOfRange {
					topicResps[i].PartitionResponses[j].ErrorCode = OFFSET_OUT_OF_RANGE
					//兼容kafka协议，out_of_range错误的highwatermark为-1
					topicResps[i].PartitionResponses[j].HighWatermark = -1
				} else {
					topicResps[i].PartitionResponses[j].ErrorCode = UNKNOWN
				}
				continue
			}
			if fromMemoryThisTime {
				isReadFromMemory = true
			}
			*fetchCount = *fetchCount + len(entries)

			h.printVdlLogs(entries)

			var partitionFetchedBytes int32 = 0
			for _, entry := range entries {
				topicResps[i].PartitionResponses[j].Messages = append(topicResps[i].PartitionResponses[j].Messages, &protocol.FetchMessage{
					Offset: entry.Offset,
					Record: entry.Data,
				})
				partitionFetchedBytes = partitionFetchedBytes + int32(len(entry.Data))
				reqPartition.FetchOffset = entry.Offset + 1
			}

			reqPartition.MaxBytes = reqPartition.MaxBytes - partitionFetchedBytes
			fetchedBytes = fetchedBytes + partitionFetchedBytes
		}
	}
	return fetchedBytes, topicResps, isReadFromMemory
}

func (h *KafkaAPIHandler) longPollingFetch(req *protocol.FetchRequest, timeWaitChan <-chan time.Time,
	fetchedBytes int32, resp *protocol.FetchResponses, fetchCount *int) {

	// get all waiting message arrive chan
	// when raft group apply message, this arrive chan will close, and all client will get notify
	waitSelectCases, waitInfos := h.generateFetchWait(req)
	waitSelectCases = append(waitSelectCases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timeWaitChan)})

	for {
		if fetchedBytes >= req.MinBytes {
			break
		}
		chosenIndex, _, ok := reflect.Select(waitSelectCases)
		if ok { // ok means receive timeout
			break
		} else {
			// get which raft group[log stream] have message arrived
			receiveMessageRaftInfo := waitInfos[chosenIndex]
			wrapper := receiveMessageRaftInfo.wrapper

			// fast fail if no leader
			// one raft group change can cancel fetch immediately because NO IMPACT for client fetch again
			if !wrapper.IsLeader() {
				return
			}

			// reset wait chan
			waitSelectCases[chosenIndex].Chan = reflect.ValueOf(wrapper.GetMessageArriveChan())

			respPartition := findRespPartition(receiveMessageRaftInfo, resp.Responses)
			reqPartition := findReqPartition(receiveMessageRaftInfo, req)

			// update HighWatermark when have new message
			respPartition.HighWatermark = wrapper.GetHighWater()

			canFetchBytes := reqPartition.MaxBytes
			// this partition can't fetch anymore
			if canFetchBytes < 0 {
				continue
			}
			// adjust fetch bytes when partition can fetch bytes than global can fetch bytes
			if canFetchBytes > (req.MinBytes - fetchedBytes) {
				canFetchBytes = req.MinBytes - fetchedBytes
			}
			entries, err, _ := wrapper.ConsumeMessages(reqPartition.FetchOffset, canFetchBytes)
			if err != nil {
				glog.Error("HandleFetch, ConsumeMessages error:", err)
				break
			}

			*fetchCount = *fetchCount + len(entries)

			h.printVdlLogs(entries)

			var partitionFetchedBytes int32 = 0
			for _, entry := range entries {
				respPartition.Messages = append(respPartition.Messages, &protocol.FetchMessage{
					Offset: entry.Offset,
					Record: entry.Data,
				})
				partitionFetchedBytes = partitionFetchedBytes + int32(len(entry.Data))
				reqPartition.FetchOffset = entry.Offset + 1
			}
			reqPartition.MaxBytes = reqPartition.MaxBytes - partitionFetchedBytes
			fetchedBytes = fetchedBytes + partitionFetchedBytes
		}
	}
}

func (h *KafkaAPIHandler) generateFetchWait(req *protocol.FetchRequest) ([]reflect.SelectCase, []*waitInfo) {

	cases := make([]reflect.SelectCase, 0)
	waitInfos := make([]*waitInfo, 0)

	for _, reqTopic := range req.Topics {
		wrapper, err := h.LogStreamWrapperGetter.GetLogStreamWrapper(reqTopic.Topic)
		if err != nil {
			continue
		}
		for _, reqPartition := range reqTopic.Partitions {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(wrapper.GetMessageArriveChan())})
			waitInfos = append(waitInfos, &waitInfo{
				topic:     reqTopic.Topic,
				partition: reqPartition.Partition,
				wrapper:   wrapper,
			})

		}
	}
	return cases, waitInfos
}

func (h *KafkaAPIHandler) printFetchRequest(req *protocol.FetchRequest) {
	if glog.V(1) {
		glog.Infof("D:+HandleFetch, Request Information: ReplicaID: %d, MinBytes: %d, MaxWaitTime: %d ", req.ReplicaID, req.MinBytes, req.MaxWaitTime)
		for _, topic := range req.Topics {
			glog.Infof("D:|- req topic: %s ", topic.Topic)
			for _, partition := range topic.Partitions {
				glog.Infof("D:|-|- req partition %d, FetchOffset: %d, MaxBytes:%d ", partition.Partition, partition.FetchOffset, partition.MaxBytes)
			}
		}
	}
}

func (h *KafkaAPIHandler) printFetchResponse(resp *protocol.FetchResponses) {
	if glog.V(1) {
		glog.Infof("D:+HandleFetch response")
		for _, respTopic := range resp.Responses {
			glog.Infof("D:|-topic: %s", respTopic.Topic)
			for _, respPartition := range respTopic.PartitionResponses {
				glog.Infof("D:|-|-partitionID: %d, ErrorCode: %d, HighWatermark: %d", respPartition.Partition, respPartition.ErrorCode, respPartition.HighWatermark)
				for _, msg := range respPartition.Messages {
					glog.Infof("D:|-|-|-offset: %d", msg.Offset)
				}
			}
		}
	}
}

func findRespPartition(w *waitInfo, resp []*protocol.FetchResponse) *protocol.FetchPartitionResponse {
	for _, fetchResponse := range resp {
		if fetchResponse.Topic != w.topic {
			continue
		}
		for _, fetchPartitionResponse := range fetchResponse.PartitionResponses {
			if fetchPartitionResponse.Partition == w.partition {
				return fetchPartitionResponse
			}
		}
	}
	panic("should find partition with topic " + w.topic + " and partition " + strconv.Itoa(int(w.partition)))
}

func findReqPartition(w *waitInfo, req *protocol.FetchRequest) *protocol.FetchPartition {

	for _, reqTopic := range req.Topics {
		if reqTopic.Topic != w.topic {
			continue
		}
		for _, reqPartition := range reqTopic.Partitions {
			if reqPartition.Partition == w.partition {
				return reqPartition
			}
		}
	}
	panic("should find partition with topic " + w.topic + " and partition " + strconv.Itoa(int(w.partition)))
}

//TODO only print v1 version message
//TODO do not support v0 message
func (h *KafkaAPIHandler) printVdlLogs(entries []logstream.Entry) {
	//debug info
	if glog.V(1) {
		for _, entry := range entries {
			//var key, value string
			//var valueOffset int32
			offset := entry.Offset
			glog.Infof("D:[logstream_wrapper.go-printVdlLogs]:len(entry.Data)=%d,data=%v", len(entry.Data), entry.Data)
			crc := int32(binary.BigEndian.Uint32(entry.Data[:4]))
			magicByte := int8(entry.Data[4])
			Attributes := int8(entry.Data[5])
			glog.Infof("D:[fetch.go-printVdlLogs]:print vdl log:"+
				"offset=%d,crc=%d,magicByte=%d,Attributes=%d,",
				offset, crc, magicByte, Attributes)
		}
	}
}
