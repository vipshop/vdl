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
	"errors"
	"github.com/vipshop/vdl/consensus/raftgroup"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/server/adminapi/apierror"
	pb "github.com/vipshop/vdl/server/adminapi/apipb"
	"golang.org/x/net/context"
)

// This handler use to process
// 1) LogStream start
// 2) LogStream stop
// 3) LogStream delete
type LogStreamAdminServer struct {
	raftGroupGetter RaftGroupGetter
	//key is raft group, ignore value
	DeleteForbid map[string]bool
}

func NewLogStreamAdminServer(getter RaftGroupGetter) *LogStreamAdminServer {
	return &LogStreamAdminServer{
		raftGroupGetter: getter,
		DeleteForbid:    make(map[string]bool),
	}
}

func (l *LogStreamAdminServer) LogStreamStart(ctx context.Context, r *pb.LogStreamStartRequest) (*pb.LogStreamStartResponse, error) {

	raftGroup, getError := l.raftGroupGetter.GetRaftGroup(r.LogstreamName)
	if getError != nil {
		return nil, apierror.ToRPCError(getError)
	}

	err := raftGroup.Start()
	if err != nil {
		return nil, apierror.ToRPCError(err)
	}

	return &pb.LogStreamStartResponse{Header: l.header(raftGroup)}, nil

}

func (l *LogStreamAdminServer) LogStreamStop(ctx context.Context, r *pb.LogStreamStopRequest) (*pb.LogStreamStopResponse, error) {

	raftGroup, getError := l.raftGroupGetter.GetRaftGroup(r.LogstreamName)
	if getError != nil {
		return nil, apierror.ToRPCError(getError)
	}

	err := raftGroup.Stop()
	if err != nil {
		return nil, apierror.ToRPCError(err)
	}

	return &pb.LogStreamStopResponse{Header: l.header(raftGroup)}, nil

}

func (l *LogStreamAdminServer) DeleteFile(ctx context.Context, r *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	raftGroup, getError := l.raftGroupGetter.GetRaftGroup(r.LogstreamName)
	if getError != nil {
		return nil, apierror.ToRPCError(getError)
	}
	if _, ok := l.DeleteForbid[raftGroup.GroupConfig.GroupName]; ok == true {
		return &pb.DeleteFileResponse{
			ResultStatus: "Error",
			ErrorMsg:     "not allow to delete file in this raft group,need enable delete permission",
		}, nil
	}
	logStore := raftGroup.GetLogStore()
	err := logStore.DeleteFiles(r.SegmentFiles)
	if err != nil {
		return nil, apierror.ToRPCError(err)
	}
	return &pb.DeleteFileResponse{
		ResultStatus: "OK",
		ErrorMsg:     "",
	}, nil
}

func (l *LogStreamAdminServer) GetDeletePermission(ctx context.Context,
	r *pb.GetDeletePermissionReq) (*pb.GetDeletePermissionResp, error) {
	raftGroupName := r.LogstreamName
	_, ok := l.DeleteForbid[raftGroupName]
	if ok {
		return &pb.GetDeletePermissionResp{
			ResultStatus: "disable",
		}, nil
	} else {
		return &pb.GetDeletePermissionResp{
			ResultStatus: "enable",
		}, nil
	}
	return nil, nil
}

func (l *LogStreamAdminServer) SetDeletePermission(ctx context.Context,
	r *pb.SetDeletePermissionReq) (*pb.SetDeletePermissionResp, error) {
	raftGroupName := r.LogstreamName

	if r.Switch != 0 && r.Switch != 1 {
		return &pb.SetDeletePermissionResp{
			ResultStatus: "Error",
			ErrorMsg:     "args not avaiable",
		}, nil
	}
	if r.Switch == 0 {
		l.DeleteForbid[raftGroupName] = true
	} else {
		delete(l.DeleteForbid, raftGroupName)
	}
	return &pb.SetDeletePermissionResp{
		ResultStatus: "OK",
		ErrorMsg:     "",
	}, nil
}

// rpc handler for leader transfer
func (l *LogStreamAdminServer) LeaderTransfer(ctx context.Context, r *pb.LogStreamLeaderTransferRequeset) (*pb.LogStreamLeaderTransferResponse, error) {

	raftGroup, getError := l.raftGroupGetter.GetRaftGroup(r.LogstreamName)
	if getError != nil {
		return nil, apierror.ToRPCError(getError)
	}

	// already leader
	if raftGroup.GetLeaderID() == r.Transferee {
		return nil, apierror.ToRPCError(errors.New(types.ID(r.Transferee).String() + " already leader"))
	}

	// must executed in leader node
	if !raftGroup.IsLeader() {
		return nil, apierror.ToRPCError(errors.New("please execute leader transfer at leader node"))
	}

	// Transferee check
	if !raftGroup.Membership.IsIDInMembership(types.ID(r.Transferee)) {
		return nil, apierror.ToRPCError(errors.New(types.ID(r.Transferee).String() + " not valid"))
	}

	// do transfer
	err := raftGroup.LeaderTransfer(ctx, r.Transferee)
	if err != nil {
		return nil, apierror.ToRPCError(err)
	}

	return &pb.LogStreamLeaderTransferResponse{Header: l.header(raftGroup)}, nil
}

func (l *LogStreamAdminServer) LogStreamDelete(context.Context, *pb.LogStreamDeleteRequest) (*pb.LogStreamDeleteResponse, error) {

	return nil, apierror.ToRPCError(errors.New("NOT IMPLEMENT"))

}

func (l *LogStreamAdminServer) header(raftGroup *raftgroup.RaftGroup) *pb.ResponseHeader {

	return &pb.ResponseHeader{
		LogstreamName: raftGroup.GroupConfig.GroupName,
		ClusterId:     uint64(raftGroup.Membership.ClusterID),
		ServerName:    raftGroup.GroupConfig.VDLServerName,
	}
}
