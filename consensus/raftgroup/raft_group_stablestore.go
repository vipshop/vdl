// Copyright 2018 The etcd Authors. All rights reserved.
// Use of this source code is governed by a Apache
// license that can be found in the LICENSES/etcd-LICENSE file.
//
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

	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/jsonutil"
	"github.com/vipshop/vdl/stablestore"
)

const (
	VoteKeyPrefix       = "VoteKey_"
	ApplyIndexKeyPrefix = "ApplyIndexKey_"
)

type RaftGroupStableStore struct {
	stableStore stablestore.StableStore
}

type PersistentVote struct {
	Vote uint64 `json:"vote"`
	Term uint64 `json:"term"`
}

func NewRaftGroupStableStore(stableStore stablestore.StableStore) *RaftGroupStableStore {
	return &RaftGroupStableStore{
		stableStore: stableStore,
	}
}

func (r *RaftGroupStableStore) MustSaveVote(raftGroupName string, voteNodeID uint64, term uint64) {

	saveVote := PersistentVote{
		Vote: voteNodeID,
		Term: term,
	}

	err := r.stableStore.Put([]byte(VoteKeyPrefix+raftGroupName), jsonutil.MustMarshal(saveVote))
	if err != nil {
		glog.Fatalln("save vote to stable store should not be fail", err)
	}
}

func (r *RaftGroupStableStore) MustGetVote(raftGroupName string) (term uint64, voteNodeID uint64) {

	lastPersistentVoteByte, err := r.stableStore.Get([]byte(VoteKeyPrefix + raftGroupName))
	if err != nil {
		glog.Fatalln(err)
	}
	lastPersistentVote := PersistentVote{}
	if len(lastPersistentVoteByte) > 0 {
		jsonutil.MustUnMarshal(lastPersistentVoteByte, &lastPersistentVote)
		return lastPersistentVote.Term, lastPersistentVote.Vote
	} else {
		return 0, 0
	}
}

func (r *RaftGroupStableStore) MustSaveApplyIndex(raftGroupName string, applyIndex uint64) {

	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, applyIndex)
	err := r.stableStore.Put([]byte(ApplyIndexKeyPrefix+raftGroupName), buf)
	if err != nil {
		glog.Fatalln("save ApplyIndex to stable store should not be fail", err)
	}
}

func (r *RaftGroupStableStore) MustGetApplyIndex(raftGroupName string) uint64 {

	buf, err := r.stableStore.Get([]byte(ApplyIndexKeyPrefix + raftGroupName))
	if err != nil {
		glog.Fatalln(err)
	}
	if len(buf) > 0 {
		return binary.BigEndian.Uint64(buf)
	}
	return 0
}
