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

	"github.com/vipshop/vdl/consensus/raft"
	"github.com/vipshop/vdl/consensus/raftgroup/api/apicommon"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/server/kafka/protocol"
)

func (h *KafkaAPIHandler) HandleMetadataV2(conn net.Conn, header *protocol.RequestHeader, req *protocol.MetadataRequest) error {
	resp := &protocol.MetadataResponseV2{
		ClusterID: "",
	}
	var controllerID int32 = 0
	vdlServerMap := make(map[int32]*apicommon.VDLServerInfo)
	// If empty the request will yield metadata for all topics
	//https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TopicMetadataRequest
	if len(req.Topics) == 0 {
		req.Topics = h.LogStreamWrapperGetter.GetAllLogStreamNames()
	}
	topics := make([]*protocol.TopicMetadataV2, len(req.Topics))
	for reqIndex, reqTopic := range req.Topics {
		topics[reqIndex] = &protocol.TopicMetadataV2{
			Topic:      reqTopic,
			IsInternal: false,
		}
		partitionMetadata := make([]*protocol.PartitionMetadata, 1)
		partitionMetadata[0] = &protocol.PartitionMetadata{
			PartitionErrorCode: NONE,
			ParititionID:       0,
			Leader:             0,
			Replicas:           []int32{},
			ISR:                []int32{},
		}
		topics[reqIndex].PartitionMetadata = partitionMetadata

		wrapper, err := h.LogStreamWrapperGetter.GetLogStreamWrapper(reqTopic)
		if err != nil {
			glog.Warning("HandleMetadata error , topic:", reqTopic, " error:", err)
			topics[reqIndex].TopicErrorCode = UNKNOWN_TOPIC_OR_PARTITION
			continue
		}

		vdlServerInfos, err := wrapper.GetVDLServerInfo()
		if err != nil {
			glog.Warning("HandleMetadata error , topic:", reqTopic, " error:", err)
			topics[reqIndex].TopicErrorCode = UNKNOWN
			continue
		}

		topics[reqIndex].TopicErrorCode = NONE

		// if no leader
		var raftLeaderID uint64 = wrapper.GetLeaderNodeID()
		if raftLeaderID == raft.None {
			glog.Warning("HandleMetadata error , can't find leader of topic:", reqTopic)
			partitionMetadata[0].PartitionErrorCode = LEADER_NOT_AVAILABLE
			continue
		}

		var vdlServerLeaderID int32 = 0
		replicas := make([]int32, len(vdlServerInfos))
		for i, vdlServerInfo := range vdlServerInfos {
			replicas[i] = vdlServerInfo.VDLServerID
			if vdlServerInfo.RaftNodeID == raftLeaderID {
				vdlServerLeaderID = vdlServerInfo.VDLServerID
			}
			vdlServerMap[vdlServerInfo.VDLServerID] = vdlServerInfo
		}
		if vdlServerLeaderID == 0 {
			glog.Errorf("HandleMetadata error , should find the VDL Server LeaderID, but can't find currently, topic:", reqTopic)
			partitionMetadata[0].PartitionErrorCode = UNKNOWN
			continue
		}

		partitionMetadata[0].Leader = vdlServerLeaderID
		partitionMetadata[0].Replicas = replicas
		partitionMetadata[0].ISR = replicas
		controllerID = vdlServerLeaderID
	}

	brokers := make([]*protocol.BrokerV2, 0, len(vdlServerMap))
	for _, v := range vdlServerMap {
		brokers = append(brokers, &protocol.BrokerV2{
			NodeID: v.VDLServerID,
			Host:   v.Host,
			Port:   v.Port,
			Rack:   "",
		})
	}

	resp.Brokers = brokers
	resp.ControllerID = controllerID
	resp.TopicMetadata = topics

	glog.Infof("accept a get metadata connection V2, from %v", conn.RemoteAddr())
	glog.Infoln("D:+HandleMetadata")
	glog.Infoln("D:|-brokers")
	for _, v := range brokers {
		glog.Infof("D:|-|- node_id:%d, Host:%s, Port:%d, Rack:%s",
			v.NodeID, v.Host, v.Port, v.Rack)
	}
	glog.Infoln("D:|-cluster_id:", resp.ClusterID, " controller_id:", resp.ControllerID)
	glog.Infoln("D:|-metadata")
	for _, v := range resp.TopicMetadata {
		glog.Infoln("D:|-|- topic:", v.Topic, " IsInternal:", v.IsInternal, " TopicErrorCode:", v.TopicErrorCode)
		for _, m := range v.PartitionMetadata {
			glog.Infoln("D:|-|-|- partition_metadata, ParititionID:", m.ParititionID, " PartitionErrorCode:",
				m.PartitionErrorCode, " ISR:", m.ISR, " Leader:", m.Leader, " Replicas:", m.Replicas)
		}
	}

	r := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          resp,
	}
	return h.write(conn, r)
}

func (h *KafkaAPIHandler) HandleMetadataV1(conn net.Conn, header *protocol.RequestHeader, req *protocol.MetadataRequest) error {
	resp := &protocol.MetadataResponseV1{}
	var controllerID int32 = 0
	vdlServerMap := make(map[int32]*apicommon.VDLServerInfo)
	// If empty the request will yield metadata for all topics
	//https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TopicMetadataRequest
	if len(req.Topics) == 0 {
		req.Topics = h.LogStreamWrapperGetter.GetAllLogStreamNames()
	}
	topics := make([]*protocol.TopicMetadataV1, len(req.Topics))
	for reqIndex, reqTopic := range req.Topics {
		topics[reqIndex] = &protocol.TopicMetadataV1{
			Topic:      reqTopic,
			IsInternal: false,
		}
		partitionMetadata := make([]*protocol.PartitionMetadata, 1)
		partitionMetadata[0] = &protocol.PartitionMetadata{
			PartitionErrorCode: NONE,
			ParititionID:       0,
			Leader:             0,
			Replicas:           []int32{},
			ISR:                []int32{},
		}
		topics[reqIndex].PartitionMetadata = partitionMetadata

		wrapper, err := h.LogStreamWrapperGetter.GetLogStreamWrapper(reqTopic)
		if err != nil {
			glog.Warning("HandleMetadata error , topic:", reqTopic, " error:", err)
			topics[reqIndex].TopicErrorCode = UNKNOWN_TOPIC_OR_PARTITION
			continue
		}

		vdlServerInfos, err := wrapper.GetVDLServerInfo()
		if err != nil {
			glog.Warning("HandleMetadata error , topic:", reqTopic, " error:", err)
			topics[reqIndex].TopicErrorCode = UNKNOWN
			continue
		}

		topics[reqIndex].TopicErrorCode = NONE

		// if no leader
		var raftLeaderID uint64 = wrapper.GetLeaderNodeID()
		if raftLeaderID == raft.None {
			glog.Warning("HandleMetadata error , can't find leader of topic:", reqTopic)
			partitionMetadata[0].PartitionErrorCode = LEADER_NOT_AVAILABLE
			continue
		}

		var vdlServerLeaderID int32 = 0
		replicas := make([]int32, len(vdlServerInfos))
		for i, vdlServerInfo := range vdlServerInfos {
			replicas[i] = vdlServerInfo.VDLServerID
			if vdlServerInfo.RaftNodeID == raftLeaderID {
				vdlServerLeaderID = vdlServerInfo.VDLServerID
			}
			vdlServerMap[vdlServerInfo.VDLServerID] = vdlServerInfo
		}
		if vdlServerLeaderID == 0 {
			glog.Error("HandleMetadata error , should find the VDL Server LeaderID, but can't find currently, topic:", reqTopic)
			partitionMetadata[0].PartitionErrorCode = UNKNOWN
			continue
		}

		partitionMetadata[0].Leader = vdlServerLeaderID
		partitionMetadata[0].Replicas = replicas
		partitionMetadata[0].ISR = replicas
		controllerID = vdlServerLeaderID
	}

	brokers := make([]*protocol.BrokerV1, 0, len(vdlServerMap))
	for _, v := range vdlServerMap {
		brokers = append(brokers, &protocol.BrokerV1{
			NodeID: v.VDLServerID,
			Host:   v.Host,
			Port:   v.Port,
			Rack:   "",
		})
	}

	resp.Brokers = brokers
	resp.ControllerID = controllerID
	resp.TopicMetadata = topics

	glog.Infof("accept a get metadata connection V1, from %v", conn.RemoteAddr())
	glog.Infoln("D:+HandleMetadata")
	glog.Infoln("D:|-brokers")
	for _, v := range brokers {
		glog.Infoln("D:|-|- node_id:", v.NodeID, " Host:", v.Host, " Port:", v.Port, " Rack:", v.Rack)
	}
	glog.Infoln("D:|-cluster_id:", " controller_id:", resp.ControllerID)
	glog.Infoln("D:|-metadata")
	for _, v := range resp.TopicMetadata {
		glog.Infoln("D:|-|- topic:", v.Topic, " IsInternal:", v.IsInternal, " TopicErrorCode:", v.TopicErrorCode)
		for _, m := range v.PartitionMetadata {
			glog.Infoln("D:|-|-|- partition_metadata, ParititionID:", m.ParititionID, " PartitionErrorCode:",
				m.PartitionErrorCode, " ISR:", m.ISR, " Leader:", m.Leader, " Replicas:", m.Replicas)
		}
	}

	r := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          resp,
	}
	return h.write(conn, r)
}

func (h *KafkaAPIHandler) HandleMetadataV0(conn net.Conn, header *protocol.RequestHeader, req *protocol.MetadataRequest) error {
	resp := &protocol.MetadataResponseV0{}
	vdlServerMap := make(map[int32]*apicommon.VDLServerInfo)
	// If empty the request will yield metadata for all topics
	//https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-TopicMetadataRequest
	if len(req.Topics) == 0 {
		req.Topics = h.LogStreamWrapperGetter.GetAllLogStreamNames()
	}
	topics := make([]*protocol.TopicMetadataV0, len(req.Topics))
	for reqIndex, reqTopic := range req.Topics {
		topics[reqIndex] = &protocol.TopicMetadataV0{
			Topic: reqTopic,
		}
		partitionMetadata := make([]*protocol.PartitionMetadata, 1)
		partitionMetadata[0] = &protocol.PartitionMetadata{
			PartitionErrorCode: NONE,
			ParititionID:       0,
			Leader:             0,
			Replicas:           []int32{},
			ISR:                []int32{},
		}
		topics[reqIndex].PartitionMetadata = partitionMetadata

		wrapper, err := h.LogStreamWrapperGetter.GetLogStreamWrapper(reqTopic)
		if err != nil {
			glog.Warning("HandleMetadata error , topic:", reqTopic, " error:", err)
			topics[reqIndex].TopicErrorCode = UNKNOWN_TOPIC_OR_PARTITION
			continue
		}

		vdlServerInfos, err := wrapper.GetVDLServerInfo()
		if err != nil {
			glog.Warning("HandleMetadata error , topic:", reqTopic, " error:", err)
			topics[reqIndex].TopicErrorCode = UNKNOWN
			continue
		}

		topics[reqIndex].TopicErrorCode = NONE

		// if no leader
		var raftLeaderID uint64 = wrapper.GetLeaderNodeID()
		if raftLeaderID == raft.None {
			glog.Warning("HandleMetadata error , can't find leader of topic:", reqTopic)
			partitionMetadata[0].PartitionErrorCode = LEADER_NOT_AVAILABLE
			continue
		}

		var vdlServerLeaderID int32 = 0
		replicas := make([]int32, len(vdlServerInfos))
		for i, vdlServerInfo := range vdlServerInfos {
			replicas[i] = vdlServerInfo.VDLServerID
			if vdlServerInfo.RaftNodeID == raftLeaderID {
				vdlServerLeaderID = vdlServerInfo.VDLServerID
			}
			vdlServerMap[vdlServerInfo.VDLServerID] = vdlServerInfo
		}
		if vdlServerLeaderID == 0 {
			glog.Error("HandleMetadata error , should find the VDL Server LeaderID, but can't find currently, topic:", reqTopic)
			partitionMetadata[0].PartitionErrorCode = UNKNOWN
			continue
		}

		partitionMetadata[0].Leader = vdlServerLeaderID
		partitionMetadata[0].Replicas = replicas
		partitionMetadata[0].ISR = replicas
	}

	brokers := make([]*protocol.BrokerV0, 0, len(vdlServerMap))
	for _, v := range vdlServerMap {
		brokers = append(brokers, &protocol.BrokerV0{
			NodeID: v.VDLServerID,
			Host:   v.Host,
			Port:   v.Port,
		})
	}

	resp.Brokers = brokers
	resp.TopicMetadata = topics

	glog.Infof("accept a get metadata connection V0, from %v", conn.RemoteAddr())
	glog.Infoln("D:+HandleMetadata")
	glog.Infoln("D:|-brokers")
	for _, v := range brokers {
		glog.Infoln("D:|-|- node_id:", v.NodeID, " Host:", v.Host, " Port:", v.Port)
	}
	glog.Infoln("D:|-metadata")
	for _, v := range resp.TopicMetadata {
		glog.Infoln("D:|-|- topic:", v.Topic, " TopicErrorCode:", v.TopicErrorCode)
		for _, m := range v.PartitionMetadata {
			glog.Infoln("D:|-|-|- partition_metadata, ParititionID:", m.ParititionID, " PartitionErrorCode:",
				m.PartitionErrorCode, " ISR:", m.ISR, " Leader:", m.Leader, " Replicas:", m.Replicas)
		}
	}

	r := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          resp,
	}
	return h.write(conn, r)
}
