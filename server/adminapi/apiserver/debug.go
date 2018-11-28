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

package apiserver

import (
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/server/adminapi/apierror"
	pb "github.com/vipshop/vdl/server/adminapi/apipb"
	"golang.org/x/net/context"
)

type DevToolServer struct {
}

func NewDevToolServer() *DevToolServer {
	return &DevToolServer{}
}

func (s *DevToolServer) DebugSwitch(ctx context.Context, r *pb.DebugSwitchRequest) (*pb.DebugSwitchResponse, error) {
	var err error
	if r.Switch != uint64(0) && r.Switch != uint64(1) {
		return nil, apierror.ErrDebugSwitchArgs
	}

	if r.Switch == uint64(0) {
		glog.Infof("disable output glog debug message")
		err = glog.SetV("0")
	} else {
		glog.Infof("enable output glog debug message")
		err = glog.SetV("1")
	}
	if err != nil {
		return nil, apierror.ToRPCError(err)
	}
	return &pb.DebugSwitchResponse{
		ResultStatus: "OK",
		ErrorMsg:     "",
	}, nil
}
