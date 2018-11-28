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
	"encoding/binary"
	"io/ioutil"

	"encoding/json"

	"path"

	"fmt"
	"os"

	"github.com/errors"
	"github.com/vipshop/vdl/consensus/raftgroup"
	"github.com/vipshop/vdl/consensus/raftgroup/membership"
	"github.com/vipshop/vdl/logstream"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/server/adminapi/apierror"
	pb "github.com/vipshop/vdl/server/adminapi/apipb"
	"golang.org/x/net/context"
)

const (
	fileNameSuffix = "_snapshot_meta.json"

	DirPermission      = 0755
	MetaFilePermission = 0644
)

type SnapshotAdminServer struct {
	raftGroupGetter RaftGroupGetter
}

type SnapshotMeta struct {
	//RaftGroupName
	RaftGroupName string
	//ApplyIndex
	ApplyIndex uint64
	//VoteFor
	Vote raftgroup.PersistentVote
	//Membership
	Members  membership.MembershipMembers
	Segments []*logstream.SegmentFile
}

func NewSnapshotAdminServer(getter RaftGroupGetter) *SnapshotAdminServer {
	return &SnapshotAdminServer{
		raftGroupGetter: getter,
	}
}

func (s *SnapshotAdminServer) CreateSnapshotMeta(ctx context.Context, r *pb.SnapshotRequeest) (*pb.SnapshotResponse, error) {
	var applyIndex uint64
	var voteFor raftgroup.PersistentVote
	var members membership.MembershipMembers

	//get raft group
	raftGroup, getError := s.raftGroupGetter.GetRaftGroup(r.RaftGroup)
	if getError != nil {
		return nil, apierror.ToRPCError(getError)
	}

	//get key name
	membershipsKey := membership.MembersKeyPrefix + raftGroup.GroupConfig.GroupName
	voteKey := raftgroup.VoteKeyPrefix + raftGroup.GroupConfig.GroupName
	applyIndexKey := raftgroup.ApplyIndexKeyPrefix + raftGroup.GroupConfig.GroupName

	keys := make([]string, 0, 3)
	keys = append(keys, membershipsKey, voteKey, applyIndexKey)
	metaMap, err := raftGroup.GetMetaBatch(keys)
	if err != nil {
		glog.Errorf("raftGroup GetMetaBatch error,err=%s,keys=%v,metaMap=%v", err.Error(), keys, metaMap)
		return nil, apierror.ToRPCError(err)
	}

	//decode applyIndexKey
	if 0 < len(metaMap[applyIndexKey]) {
		applyIndex = binary.BigEndian.Uint64(metaMap[applyIndexKey])
	} else {
		glog.Warningf("applyIndex is nil,metaMap=%v,no snapshot", metaMap)
		applyIndex = 0
		err := errors.New("no snapshot,because applyIndex is 0")
		return nil, apierror.ToRPCError(err)
	}

	//decode voteKey
	if 0 < len(metaMap[voteKey]) {
		err = json.Unmarshal(metaMap[voteKey], &voteFor)
		if err != nil {
			glog.Errorf("metaMap[voteKey] Unmarshal error,,err=%s,voteKey=%s", err.Error(), voteKey)
			return nil, apierror.ToRPCError(err)
		}
	} else {
		glog.Warningf("voteFor is nil,metaMap=%v", metaMap)
		voteFor.Term = 0
		voteFor.Vote = 0
	}

	//decode memberships
	if 0 < len(metaMap[membershipsKey]) {
		err = json.Unmarshal(metaMap[membershipsKey], &members)
		if err != nil {
			glog.Errorf("Unmarshal metaMap[membershipsKey] error,err=%s,metaMap[membershipsKey]=%v,members=%v",
				metaMap[membershipsKey], members)
			return nil, apierror.ToRPCError(err)
		}
	} else {
		glog.Warningf("members is nil,metaMap=%v", metaMap)
		members = membership.MembershipMembers{
			MembersMap: make(map[types.ID]*membership.Member),
			RemovedMap: make(map[types.ID]bool),
		}
	}

	//get from logstore
	logStorage := raftGroup.GetLogStore()
	segmentFiles, err := logStorage.CreateSnapshotMeta(applyIndex)
	if err != nil {
		glog.Errorf("logstore CreateSnapshotMeta error,err=%s,applyIndex=%d", err.Error(), applyIndex)
		return nil, apierror.ToRPCError(err)
	}

	//new snapshot metadata
	snapMeta := &SnapshotMeta{
		RaftGroupName: raftGroup.GroupConfig.GroupName,
		ApplyIndex:    applyIndex,
		Vote:          voteFor,
		Members:       members,
		Segments:      segmentFiles,
	}
	snapMetaBuf, err := json.Marshal(snapMeta)
	if err != nil {
		glog.Errorf("json marshal snapshot meta error,err=%s,snapMeta=%v", err.Error(), snapMeta)
		return nil, apierror.ToRPCError(err)
	}
	return &pb.SnapshotResponse{
		ResultStatus: "OK",
		Msg:          snapMetaBuf,
	}, nil
}

//write snapshot meta into file
func (s *SnapshotAdminServer) writeIntoFile(snapMeta *SnapshotMeta, dir string) error {
	snapMetaBuf, err := json.Marshal(snapMeta)
	if err != nil {
		return err
	}

	//check snapshot metadata dir, if not exist,create dir
	dirExist, err := pathExists(dir)
	if err != nil {
		return err
	}
	if dirExist == false {
		err := os.MkdirAll(dir, DirPermission)
		if err != nil {
			return fmt.Errorf("MkdirAll error(%s) in writeIntoFile", err.Error())
		}
	}

	fileName := snapMeta.RaftGroupName + fileNameSuffix
	filePath := path.Join(dir, fileName)

	err = ioutil.WriteFile(filePath, snapMetaBuf, MetaFilePermission)
	if err != nil {
		return err
	}
	return nil
}

func pathExists(dir string) (bool, error) {
	_, err := os.Stat(dir)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
