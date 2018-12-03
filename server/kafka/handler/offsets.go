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

	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/server/kafka/protocol"
)

func (h *KafkaAPIHandler) HandleOffsets(conn net.Conn, header *protocol.RequestHeader, req *protocol.OffsetsRequest) error {

	resp := &protocol.OffsetsResponse{}
	resp.Responses = make([]*protocol.OffsetResponse, len(req.Topics))

	for reqTopicIndex, reqTopic := range req.Topics {
		resp.Responses[reqTopicIndex] = &protocol.OffsetResponse{
			Topic: reqTopic.Topic,
		}

		var hasLogStream bool = true
		logStreamWrapper, err := h.LogStreamWrapperGetter.GetLogStreamWrapper(reqTopic.Topic)
		if err != nil {
			glog.Warning("Can't find log stream with name :", reqTopic.Topic)
			hasLogStream = false
		}

		resp.Responses[reqTopicIndex].PartitionResponses = make([]*protocol.PartitionResponse, len(reqTopic.Partitions))
		for reqPartitionIndex, reqPartition := range reqTopic.Partitions {
			resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex] = &protocol.PartitionResponse{}
			resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex].Partition = reqPartition.Partition
			if !hasLogStream {
				resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex].ErrorCode = UNKNOWN_TOPIC_OR_PARTITION
				resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex].Offsets = []int64{0}
			} else if !logStreamWrapper.IsLeader() {
				resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex].ErrorCode = NOT_LEADER_FOR_PARTITION
				resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex].Offsets = []int64{0}
			} else {
				resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex].ErrorCode = NONE
				// -1 to receive latest offset, -2 to receive earliest offset
				if reqPartition.Timestamp == -1 {
					resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex].Offsets = []int64{logStreamWrapper.GetHighWater()}
				} else if reqPartition.Timestamp == -2 {
					minOffset := logStreamWrapper.MinOffset()
					if minOffset < 0 {
						minOffset = 0
					}
					resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex].Offsets = []int64{minOffset}
				} else {
					resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex].ErrorCode = UNKNOWN
					resp.Responses[reqTopicIndex].PartitionResponses[reqPartitionIndex].Offsets = []int64{0}
				}
			}
		}
	}
	//debug info
	if glog.V(1) {
		glog.Infof("D:+HandleOffsets, Request Information: ")
		for _, topic := range req.Topics {
			glog.Infof("D:|- req topic", topic.Topic)
			for _, partition := range topic.Partitions {
				glog.Infof("D:|-|- req partition ", partition.Partition, " Timestamp ", partition.Timestamp, " MaxNumOffsets ", partition.MaxNumOffsets)
			}
		}
		glog.Infof("D:+HandleOffsets Response")
		for _, respTopic := range resp.Responses {
			glog.Infof("D:|-topic:", respTopic.Topic)
			for _, respPartition := range respTopic.PartitionResponses {
				glog.Infof("D:|-|-partitionid:", respPartition.Partition, " ErrorCode:", respPartition.ErrorCode, " Offsets:", respPartition.Offsets)
			}
		}
	}

	r := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          resp,
	}
	return h.write(conn, r)
}
