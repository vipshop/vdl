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
	"github.com/errors"
	"github.com/vipshop/vdl/consensus/raftgroup"
)

type MockVDLServer struct {
	mockLogstream map[string]raftgroup.LogStreamWrapper
}

func NewMockVDLServer() (*MockVDLServer, error) {
	var err error
	s := make(map[string]raftgroup.LogStreamWrapper)
	s["logstream1"], err = raftgroup.NewMockLogStreamWrapper()
	if err != nil {
		return nil, err
	}
	return &MockVDLServer{mockLogstream: s}, nil
}

func (s *MockVDLServer) GetLogStreamWrapper(logStreamName string) (raftgroup.LogStreamWrapper, error) {
	if logStreamName != "logstream1" {
		return nil, errors.New("no topic")
	}
	return s.mockLogstream[logStreamName], nil
}

func (s *MockVDLServer) GetAllLogStreamNames() []string {
	return []string{"logstream1"}
}
