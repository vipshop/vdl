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
	"time"

	"github.com/coreos/etcd/pkg/idutil"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/vipshop/vdl/consensus/raftgroup/api/apicommon"
	"github.com/vipshop/vdl/logstream"
)

var defaultEntry = []byte{131, 59, 34, 51, 1, 0, 0, 0, 1, 94, 14, 70, 240, 189, 0, 0, 0, 10, 49, 53, 48, 51, 52, 55, 56, 48,
	56, 51, 0, 0, 0, 14, 83, 111, 109, 101, 116, 104, 105, 110, 103, 32, 67, 111, 111, 108}

type MockLogStreamWrapper struct {
	entries [][]byte
	wait.Wait
	reqIDGen          *idutil.Generator
	messageArriveChan chan struct{}
}

func NewMockLogStreamWrapper() (*MockLogStreamWrapper, error) {
	m := &MockLogStreamWrapper{
		entries:           make([][]byte, 0, 100),
		Wait:              wait.New(),
		reqIDGen:          idutil.NewGenerator(0, time.Now()),
		messageArriveChan: make(chan struct{}),
	}
	m.entries = append(m.entries, defaultEntry)
	return m, nil
}

func (l *MockLogStreamWrapper) GetMessageArriveChan() <-chan struct{} {
	return l.messageArriveChan
}

func (l *MockLogStreamWrapper) ConsumeMessages(startOffset int64, maxBytes int32) ([]logstream.Entry, error, bool) {
	var size int32
	ies := make([]logstream.Entry, 0, 10)
	for i := startOffset; i <= int64(len(l.entries)-1); i++ {
		ies = append(ies, logstream.Entry{
			Offset: i,
			Data:   l.entries[i],
		})
		size = size + int32(len(l.entries[i]))
		if size > maxBytes {
			break
		}
	}
	return ies, nil, false
}

func (l *MockLogStreamWrapper) GetHighWater() int64 {
	return int64(len(l.entries))
}

func (l *MockLogStreamWrapper) MinOffset() int64 {
	return 0
}

func (l *MockLogStreamWrapper) GetVDLServerInfo() ([]*apicommon.VDLServerInfo, error) {
	vdlServers := make([]*apicommon.VDLServerInfo, 0, 3)
	s1 := &apicommon.VDLServerInfo{
		RaftNodeID:  1,
		VDLServerID: 1,
		Host:        "127.0.0.1",
		Port:        8181,
	}
	s2 := &apicommon.VDLServerInfo{
		RaftNodeID:  2,
		VDLServerID: 2,
		Host:        "127.0.0.1",
		Port:        8181,
	}
	s3 := &apicommon.VDLServerInfo{
		RaftNodeID:  3,
		VDLServerID: 3,
		Host:        "127.0.0.1",
		Port:        8181,
	}
	vdlServers = append(vdlServers, s1)
	vdlServers = append(vdlServers, s2)
	vdlServers = append(vdlServers, s3)
	return vdlServers, nil
}

func (l *MockLogStreamWrapper) StoreMessage(b []byte) (<-chan interface{}, error) {
	reqId := l.reqIDGen.Next()
	dataWithReqId := make([]byte, 8)
	binary.BigEndian.PutUint64(dataWithReqId, reqId)
	dataWithReqId = append(dataWithReqId, b...)
	l.entries = append(l.entries, dataWithReqId)
	ch := l.Wait.Register(reqId)
	offset := int64(len(l.entries) - 1)
	l.Wait.Trigger(reqId, offset)

	newNotifyChan := make(chan struct{})
	oldNotifyChan := l.messageArriveChan
	l.messageArriveChan = newNotifyChan
	close(oldNotifyChan)
	return ch, nil
}

func (l *MockLogStreamWrapper) StoreMessageBatch(data [][]byte) ([]<-chan interface{}, error) {
	return nil, nil
}

func (l *MockLogStreamWrapper) IsLeader() bool {
	return true
}

func (l *MockLogStreamWrapper) GetLeaderNodeID() uint64 {
	return 1
}

func (r *MockLogStreamWrapper) GetMaxSizePerMsg() int {
	return 1024 * 1024
}
