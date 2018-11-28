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
	"encoding/json"
	"time"

	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/vipshop/vdl/consensus/raft"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/consensus/raftgroup/membership"
	"github.com/vipshop/vdl/consensus/raftgroup/raftgroupmetrics"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/vdlfiu"
	"gitlab.tools.vipshop.com/distributedstorage/fiu"
)

const (
	// when to save apply index
	// in VDL, apply entry can be
	// 1) membership change, apply immediately
	// 2) normal(log) entry, apply by time duration or by count
	saveApplyIndexCount    = 20000
	saveApplyIndexDuration = 5 * time.Second
)

func applyAll(apply *apply, r *RaftGroup) {

	var shouldStop bool = false
	defer func() {
		// finish all apply
		apply.notifyc <- struct{}{}

		if shouldStop {
			glog.Errorf("the member has been permanently removed from the logstream, and stop now")
			go r.Stop()
		}
	}()

	if len(apply.entries) == 0 {
		return
	}

	// apply entries
	shouldStop = applyEntries(apply, r)

	// metrics
	lastApplyIndex := r.getApplyIndex()
	raftgroupmetrics.MetricsRaftApplyIndex.WithLabelValues(r.GroupConfig.GroupName).Set(float64(lastApplyIndex))
}

func applyEntries(apply *apply, r *RaftGroup) bool {

	lastApplyIndex := r.getApplyIndex()
	firstNeedApplyIndex := apply.entries[0].Index

	// check
	if firstNeedApplyIndex > lastApplyIndex+1 {
		glog.Fatalf("first index of committed entry[%d] should <= appliedi[%d] + 1", firstNeedApplyIndex, lastApplyIndex)
	}

	// avoid re-apply
	var actuallyNeedApplyEntries []raftpb.Entry
	if lastApplyIndex+1-firstNeedApplyIndex < uint64(len(apply.entries)) {
		actuallyNeedApplyEntries = apply.entries[lastApplyIndex+1-firstNeedApplyIndex:]
	}
	if len(actuallyNeedApplyEntries) == 0 {
		return false
	}

	// apply entries
	shouldStop := doApply(actuallyNeedApplyEntries, r)

	// trigger persistence apply index into stable store
	hasConfigChange := hasConfigChangeEntry(actuallyNeedApplyEntries)
	appliedIndex := actuallyNeedApplyEntries[len(actuallyNeedApplyEntries)-1].Index
	if hasConfigChange ||
		appliedIndex-r.lastSaveApplyIndex >= saveApplyIndexCount ||
		time.Now().Sub(r.lastSaveApplyTime) >= saveApplyIndexDuration {

		r.raftGroupStableStore.MustSaveApplyIndex(r.GroupConfig.GroupName, appliedIndex)
		r.lastSaveApplyIndex = appliedIndex
		r.lastSaveApplyTime = time.Now()
	}

	// close messageArriveChan to notify there have new message apply;
	// currently use for consumer long polling
	if hasNormalEntry(actuallyNeedApplyEntries) {
		newNotifyChan := make(chan struct{})
		oldNotifyChan := r.messageArriveChan
		r.messageArriveChan = newNotifyChan
		safeClose(oldNotifyChan)
	}

	return shouldStop
}

// apply takes entries received from Raft (after it has been committed) and
// applies them to the current state of the VDL Server.
// The given entries should not be empty.
func doApply(es []raftpb.Entry, r *RaftGroup) (shouldStop bool) {

	for i := range es {
		e := es[i]
		switch e.Type {
		case raftpb.EntryNormal:
			applyEntryNormal(&e, r)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			pbutil.MustUnmarshal(&cc, e.Data)
			removedSelf, err := applyConfChange(cc, r)

			// test not persistent apply index
			if fiu.IsSyncPointExist(vdlfiu.FiuRaftConfChangePanicBeforeSaveApplyIndex) {
				vdlfiu.MockCrash()
			}

			r.setApplyIndex(e.Index)
			shouldStop = shouldStop || removedSelf
			r.wait.Trigger(cc.ID, &confChangeResponse{r.Membership.Members(), err})
		default:
			glog.Fatalf("entry type should be either EntryNormal or EntryConfChange")
		}
	}

	return shouldStop
}

func applyEntryNormal(e *raftpb.Entry, r *RaftGroup) {

	defer func() {
		if fiu.IsSyncPointExist(vdlfiu.FiuRaftCannotUpdateApplyIndex) {
			return
		}
		r.setApplyIndex(e.Index)
	}()

	// update apply term
	if r.currentApplyTerm != e.Term {
		if fiu.IsSyncPointExist(vdlfiu.FiuRaftCannotUpdateApplyTerm) {
			return
		}
		r.currentApplyTerm = e.Term
	}

	// no-op log is a special log using in etcd lib, not the vdl log
	if e.Type == raftpb.EntryNormal && len(e.Data) == 0 {
		return
	}

	//get requestId
	reqId := binary.BigEndian.Uint64(e.Data[:8])

	// the request is not come from this node, do not need trigger
	if r.wait.IsRegistered(reqId) == false {
		return
	}

	// if vindex equal -1, panic
	vindex, err := r.storage.GetVindexByRindex(e.Index)
	if err != nil {
		glog.Fatalf("[raft_group_fsm.go-applyEntryNormal]:GetVindexByRindex error,error=%s,rindex=%d,reqID=%d,e.Data=%s",
			err.Error(), e.Index, reqId, string(e.Data))
	}
	if vindex == -1 {
		glog.Fatalf("[raft_group_fsm.go-applyEntryNormal]:the vindex of raft log[type:%s] is -1, rindex:%d,data:%v",
			raftpb.EntryType_name[int32(e.Type)], e.Index, string(e.Data))
	}

	// trigger the request
	r.wait.Trigger(reqId, vindex)
}

// applyConfChange applies a ConfChange to the server. It is only
// invoked with a ConfChange that has already passed through Raft
func applyConfChange(cc raftpb.ConfChange, r *RaftGroup) (bool, error) {

	if err := r.Membership.ValidateConfigurationChange(cc); err != nil {
		cc.NodeID = raft.None
		r.raftNode.ApplyConfChange(cc)
		return false, err
	}

	r.raftNode.ApplyConfChange(cc)

	// test not persistent membership
	if fiu.IsSyncPointExist(vdlfiu.FiuRaftConfChangePanicBeforePersistent) {
		vdlfiu.MockCrash()
	}

	switch cc.Type {

	case raftpb.ConfChangeAddNode:
		m := new(membership.Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			glog.Fatalf("unmarshal member should never fail: %v", err)
		}
		if cc.NodeID != uint64(m.ID) {
			glog.Fatalf("nodeID should always be equal to member ID")
		}
		r.Membership.AddMemberAndPersistence(m)
		if m.ID != r.Membership.GetSelfMemberID() {
			r.transport.AddPeer(m.ID, m.PeerURLs)
		}

	case raftpb.ConfChangeRemoveNode:
		id := types.ID(cc.NodeID)
		r.Membership.RemoveMemberAndPersistence(id)
		if id == r.Membership.GetSelfMemberID() {
			return true, nil
		}
		r.transport.RemovePeer(id)

	case raftpb.ConfChangeUpdateNode:
		m := new(membership.Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			glog.Fatalf("unmarshal member should never fail: %v", err)
		}
		if cc.NodeID != uint64(m.ID) {
			glog.Fatalf("nodeID should always be equal to member ID")
		}
		r.Membership.UpdateMemberAndPersistence(m)
		if m.ID != r.Membership.GetSelfMemberID() {
			r.transport.UpdatePeer(m.ID, m.PeerURLs)
		}
	}

	return false, nil
}

func safeClose(ch chan struct{}) {
	defer func() {
		if recover() != nil {
			glog.Warning("close chan err, chan already close")
		}
	}()
	close(ch)
}
