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
	"time"

	"github.com/vipshop/vdl/consensus/raftgroup"
	"github.com/vipshop/vdl/consensus/raftgroup/membership"
	"github.com/vipshop/vdl/pkg/raftutil"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/server/adminapi/apierror"
	pb "github.com/vipshop/vdl/server/adminapi/apipb"
	"golang.org/x/net/context"
)

// This handler use to process
// 1) query membership within raft group
// 2) add membership within raft group
// 3) delete membership within raft group
// 4) update membership within raft group
type MembershipAdminServer struct {
	raftGroupGetter RaftGroupGetter
}

func NewMembershipAdminServer(getter RaftGroupGetter) *MembershipAdminServer {

	return &MembershipAdminServer{
		raftGroupGetter: getter,
	}

}

func (ms *MembershipAdminServer) MemberAdd(ctx context.Context, r *pb.MemberAddRequest) (*pb.MemberAddResponse, error) {

	// check urls
	urls, urlError := types.NewURLs(r.PeerURLs)
	if urlError != nil {
		return nil, apierror.ErrGRPCMemberBadURLs
	}

	// get raft group
	raftGroup, getError := ms.raftGroupGetter.GetRaftGroup(r.LogstreamName)
	if getError != nil {
		return nil, apierror.ToRPCError(getError)
	}
	if !raftGroup.IsStarted() {
		return nil, apierror.ErrLogstreamNotStart
	}

	// check leader
	if !raftGroup.IsLeader() {
		return nil, apierror.ErrNoLeader
	}

	now := time.Now()
	m := membership.NewMember(r.ServerName, urls, "", &now)
	member, addError := raftGroup.AddMember(ctx, *m)
	if addError != nil {
		return nil, apierror.ToRPCError(addError)
	}

	return &pb.MemberAddResponse{
		Header:  ms.header(raftGroup),
		Member:  &pb.Member{ID: uint64(m.ID), PeerURLs: m.PeerURLs},
		Members: raftutil.MembersToProtoMembers(member),
	}, nil

}

func (ms *MembershipAdminServer) MemberRemove(ctx context.Context, r *pb.MemberRemoveRequest) (*pb.MemberRemoveResponse, error) {

	// get raft group
	raftGroup, getError := ms.raftGroupGetter.GetRaftGroup(r.LogstreamName)
	if getError != nil {
		return nil, apierror.ToRPCError(getError)
	}
	if !raftGroup.IsStarted() {
		return nil, apierror.ErrLogstreamNotStart
	}

	// check leader
	if !raftGroup.IsLeader() {
		return nil, apierror.ErrNoLeader
	}

	// check member ID
	if !raftGroup.Membership.IsMemberIDExistsInStore(r.ID) {
		return nil, apierror.ErrMemberIDNotExists
	}

	member, err := raftGroup.RemoveMember(ctx, r.ID)
	if err != nil {
		return nil, apierror.ToRPCError(err)
	}
	return &pb.MemberRemoveResponse{Header: ms.header(raftGroup), Members: raftutil.MembersToProtoMembers(member)}, nil
}

func (ms *MembershipAdminServer) MemberUpdate(ctx context.Context, r *pb.MemberUpdateRequest) (*pb.MemberUpdateResponse, error) {

	// get raft group
	raftGroup, getError := ms.raftGroupGetter.GetRaftGroup(r.LogstreamName)
	if getError != nil {
		return nil, apierror.ToRPCError(getError)
	}
	if !raftGroup.IsStarted() {
		return nil, apierror.ErrLogstreamNotStart
	}

	// check leader
	if !raftGroup.IsLeader() {
		return nil, apierror.ErrNoLeader
	}

	// check member ID
	if !raftGroup.Membership.IsMemberIDExistsInStore(r.ID) {
		return nil, apierror.ErrMemberIDNotExists
	}

	// only update peer urls
	m := raftGroup.Membership.GetMemberFromStore(r.ID)
	m.PeerURLs = r.PeerURLs

	members, err := raftGroup.UpdateMember(ctx, *m)
	if err != nil {
		return nil, apierror.ToRPCError(err)
	}
	return &pb.MemberUpdateResponse{Header: ms.header(raftGroup), Members: raftutil.MembersToProtoMembers(members)}, nil
}

func (ms *MembershipAdminServer) MemberList(ctx context.Context, r *pb.MemberListRequest) (*pb.MemberListResponse, error) {
	raftGroup, getError := ms.raftGroupGetter.GetRaftGroup(r.LogstreamName)
	if getError != nil {
		return nil, apierror.ToRPCError(getError)
	}
	if !raftGroup.IsStarted() {
		return nil, apierror.ErrLogstreamNotStart
	}

	members := raftutil.MembersToProtoMembers(raftGroup.Membership.Members())
	removedMembers := raftutil.RemovedIDsToUint64Slice(raftGroup.Membership.RemovedIDs())

	return &pb.MemberListResponse{
		Header:         ms.header(raftGroup),
		Members:        members,
		Leader:         raftGroup.GetLeaderID(),
		RemovedMembers: removedMembers,
	}, nil
}

func (ms *MembershipAdminServer) header(raftGroup *raftgroup.RaftGroup) *pb.ResponseHeader {

	return &pb.ResponseHeader{
		LogstreamName: raftGroup.GroupConfig.GroupName,
		ClusterId:     uint64(raftGroup.Membership.ClusterID),
		ServerName:    raftGroup.GroupConfig.VDLServerName,
	}
}
