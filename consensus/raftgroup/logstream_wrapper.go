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

package raftgroup

import (
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"context"

	"errors"

	"github.com/vipshop/vdl/consensus/raftgroup/api/apicommon"
	"github.com/vipshop/vdl/consensus/raftgroup/api/apiserver"
	"github.com/vipshop/vdl/logstream"
	"github.com/vipshop/vdl/pkg/glog"
)

var (
	ErrExceedMaxSizePerMsg = errors.New("The message exceed the max size")
)

// LogStreamWrapper is the interface for log stream operations
type LogStreamWrapper interface {

	// get wait message wait chan
	GetMessageArriveChan() <-chan struct{}

	// the enter interface for consume message, bool return whether read from cache
	ConsumeMessages(startOffset int64, maxBytes int32) ([]logstream.Entry, error, bool)

	GetHighWater() int64

	MinOffset() int64

	// get vdl server info relate to raft group
	GetVDLServerInfo() ([]*apicommon.VDLServerInfo, error)

	// get current leader node ID, 0 for no leader
	GetLeaderNodeID() uint64

	// store message, it's the enter interface for storing log
	StoreMessage(b []byte) (<-chan interface{}, error)

	// batch store message
	StoreMessageBatch(b [][]byte) ([]<-chan interface{}, error)

	// is leader for current node and can serve
	IsLeader() bool

	// get the max size per msg in configuration
	GetMaxSizePerMsg() int
}

var (
	vdlServerInfoCacheInterval = 60 * time.Second
)

type vdlServerInfo struct {
	vdlServerInfoCache          []*apicommon.VDLServerInfo
	vdlServerInfoCacheLock      sync.Mutex
	lastVDLServerInfoUpdateTime time.Time
}

func (r *RaftGroup) GetMessageArriveChan() <-chan struct{} {
	return r.messageArriveChan
}

// bool return whether read from cache
func (r *RaftGroup) ConsumeMessages(startOffset int64, maxBytes int32) ([]logstream.Entry, error, bool) {
	//debug info
	if glog.V(1) {
		glog.Infof("D:FetchLogStreamMessages from store, startOffset:%d,endOffset:%d,maxBytes:%d",
			startOffset, r.getApplyIndex(), maxBytes)
	}
	logs, err, isReadFromCache := r.storage.FetchLogStreamMessages(startOffset, r.getApplyIndex(), maxBytes)
	if err != nil {
		return nil, err, isReadFromCache
	}
	return logs, nil, isReadFromCache
}

func (r *RaftGroup) GetHighWater() int64 {
	max, err := r.storage.MaxVindex(r.getApplyIndex())
	if err != nil {
		panic(err)
	}
	//兼容kafka协议，返回最后一条记录的下一个offset
	return max + 1
}

func (r *RaftGroup) MinOffset() int64 {
	// -1 if no entry, from storage
	min, err := r.storage.MinVindex()
	if err != nil {
		panic(err)
	}
	return min
}

// return request id
func (r *RaftGroup) StoreMessage(data []byte) (<-chan interface{}, error) {

	// msg size check
	if len(data) > r.GroupConfig.MaxSizePerMsg {
		return nil, ErrExceedMaxSizePerMsg
	}

	reqId := r.reqIDGen.Next()
	dataWithReqId := make([]byte, 8)
	binary.BigEndian.PutUint64(dataWithReqId, reqId)
	dataWithReqId = append(dataWithReqId, data...)
	ch := r.wait.Register(reqId)
	err := r.propose(context.Background(), dataWithReqId)
	if err != nil {
		glog.Errorf("[logstream_wrapper.go-StoreMessage]:Propose message error,error=%s,reqId=%d",
			err.Error(), reqId)
		return nil, err
	}
	return ch, nil
}

// return err when the following cases:
// 1) raftGroup not start
// 2) raft node close
// 3) cannot put into raft process (ctx timeout)
// all cases haven't add to raft
func (r *RaftGroup) StoreMessageBatch(data [][]byte) ([]<-chan interface{}, error) {

	dataLen := len(data)
	datasWithReqID := make([][]byte, dataLen)
	chs := make([]<-chan interface{}, dataLen)

	//TODO. performance consideration
	for i := 0; i < dataLen; i++ {
		reqId := r.reqIDGen.Next()
		dataWithReqId := make([]byte, 8)
		binary.BigEndian.PutUint64(dataWithReqId, reqId)
		dataWithReqId = append(dataWithReqId, data[i]...)
		ch := r.wait.Register(reqId)
		datasWithReqID[i] = dataWithReqId
		chs[i] = ch
	}

	err := r.proposeBatch(context.Background(), datasWithReqID)
	if err != nil {
		glog.Errorf("[logstream_wrapper.go-StoreMessage]:Propose message error,error=%s",
			err.Error())
		return nil, err
	}

	return chs, nil
}

func (r *RaftGroup) GetVDLServerInfo() ([]*apicommon.VDLServerInfo, error) {

	if time.Now().Sub(r.lastVDLServerInfoUpdateTime) > vdlServerInfoCacheInterval {
		r.vdlServerInfoCacheLock.Lock()
		defer r.vdlServerInfoCacheLock.Unlock()

		if time.Now().Sub(r.lastVDLServerInfoUpdateTime) > vdlServerInfoCacheInterval {
			members := r.Membership.Members()
			newInfo := make([]*apicommon.VDLServerInfo, 0)
			for _, member := range members {
				if member.ID == r.Membership.SelfMemberID {
					newInfo = append(newInfo, &apicommon.VDLServerInfo{
						RaftNodeID:  uint64(r.Membership.GetSelfMemberID()),
						VDLServerID: r.GroupConfig.VDLServerID,
						Host:        r.GroupConfig.VDLClientListenerHost,
						Port:        r.GroupConfig.VDLClientListenerPort,
					})
				} else {
					url := member.PeerURLs[0] + apiserver.VDLServerInfoPrefix
					info, err := getVDLServerInfo(url)
					if err != nil {
						glog.Errorf("Can't not fetch VDL server info from peer url: %s, error:%v", url, err)
					} else {
						newInfo = append(newInfo, info)
						//debug info
						if glog.V(1) {
							glog.Infof("D:[logstream_wrapper.go-GetVDLServerInfo]:info.VDLServerID=%d", info.VDLServerID)
						}
					}
				}
			}

			r.vdlServerInfoCache = newInfo
			r.lastVDLServerInfoUpdateTime = time.Now()
		}
	}

	return r.vdlServerInfoCache, nil
}

func (r *RaftGroup) GetLeaderNodeID() uint64 {
	return r.leaderID
}

func (r *RaftGroup) IsLeader() bool {
	return r.runState == raftGroupStarted && // is raft group start
		uint64(r.Membership.SelfMemberID) == r.leaderID && // is leader status
		r.currentApplyTerm == r.currentTerm && // can provide service when new leader apply current term entry (normally no-op)
		r.currentApplyTerm != 0 && r.currentTerm != 0 // should not initial status (eg: just start)
}

func (r *RaftGroup) GetMaxSizePerMsg() int {
	return r.GroupConfig.MaxSizePerMsg
}

func getVDLServerInfo(addr string) (*apicommon.VDLServerInfo, error) {

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(addr)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	vdlServerInfo := &apicommon.VDLServerInfo{}
	body, err := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(body, vdlServerInfo)
	if err != nil {
		return nil, err
	}

	return vdlServerInfo, nil
}
