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
	"sync/atomic"
	"time"

	"math"

	"github.com/vipshop/vdl/consensus/raft"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/consensus/raftgroup/raftgroupmetrics"
	"github.com/vipshop/vdl/pkg/alarm"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/types"
)

func (r *RaftGroup) startRaftGroupHandler() {
	go startRaftGroupReadyLoop(r)
	go startRaftGroupApplyLoop(r)
	go startErrorCheckLoop(r)
}

func startRaftGroupReadyLoop(r *RaftGroup) {
	var inVDLStart = true
	// return when raft group stop
	defer func() {
		r.raftLoopStopped <- struct{}{}
	}()

	isLeader := false
	inLeaderStatus := false
	// use for hard state persistence
	preHardState := raftpb.HardState{Term: 0, Vote: 0}
	// add logical check, the commit index should not large than save entry which save to log store
	lastSaveEntryIndex := uint64(math.MaxUint64)
	for {
		select {

		case <-r.ticker.C:
			r.raftNode.Tick()

		case rd := <-r.raftNode.Ready():

			beginTime := time.Now()

			// handle SoftState when Leadership changes
			if rd.SoftState != nil {
				glog.Infof("[raft_group_handler.go-startRaftGroupReadyLoop]: %s SoftState changes %v",
					r.Membership.SelfMemberID, rd.SoftState)
				handleSoftStateChange(r, rd.SoftState)
				isLeader = rd.RaftState == raft.StateLeader

				// metrics
				// Candidate will not stat
				if rd.RaftState == raft.StateLeader || rd.RaftState == raft.StateFollower {
					raftgroupmetrics.MetricsRaftRoleSwitch.WithLabelValues(r.GroupConfig.GroupName).Inc()
					if rd.RaftState == raft.StateLeader {
						if inVDLStart == false {
							//start a new goroutine, do not affect the normal flow
							go alarm.Alarm("VDL:the role of node become leader")
						}
						//node is leader
						inLeaderStatus = true
					}
					if rd.RaftState == raft.StateFollower {
						if inLeaderStatus == true {
							go alarm.Alarm("VDL:the node role in cluster from leader change to follower")
						}
						inLeaderStatus = false
					}
					inVDLStart = false
				}
			}

			// update commit index
			if len(rd.CommittedEntries) != 0 {
				r.setCommitIndex(rd.CommittedEntries[len(rd.CommittedEntries)-1].Index)
			}

			// save HardState before raft peer send
			// just save if term changes or vote changes
			saveHardState(r, &preHardState, &rd.HardState)

			// The leader can write to its disk in parallel with replicating to the followers and them
			// writing to their disks.
			// For more details, check raft thesis 10.2.1
			if isLeader {
				r.transport.Send(processMessages(r, rd.Messages))
			}

			// Save raft log, in some cases, log maybe delete
			// For detail check raft consensus
			if len(rd.Entries) > 0 {
				saveRaftLog(r, rd.Entries)
				// update lastSaveEntryIndex after save to log store
				lastSaveEntryIndex = rd.Entries[len(rd.Entries)-1].Index
			}

			// trigger apply when len(rd.CommittedEntries) > 0
			var applyNotifyChan chan struct{}
			if len(rd.CommittedEntries) > 0 {

				// add logical check,
				// the commit index should not large than save entry which save to log store
				if rd.CommittedEntries[len(rd.CommittedEntries)-1].Index > lastSaveEntryIndex {
					glog.Fatalf("[raft_group_handler.go-startRaftGroupReadyLoop]: "+
						"CommittedEntries should not large then last save entry, save entry index:%d, commit entry index:%d",
						lastSaveEntryIndex, rd.CommittedEntries[len(rd.CommittedEntries)-1].Index)
				}

				needApply := apply{
					entries:  rd.CommittedEntries,
					snapshot: rd.Snapshot,
					notifyc:  make(chan struct{}, 1),
				}
				r.applyc <- needApply
				applyNotifyChan = needApply.notifyc
			}

			if !isLeader {

				// Candidate or follower needs to wait for all pending configuration
				// changes to be applied before sending messages.
				// Otherwise we might incorrectly count votes (e.g. votes from removed members).
				// Also slow machine's follower raft-layer could proceed to become the leader
				// on its own single-node cluster, before apply-layer applies the config change.
				// We simply wait for ALL pending entries to be applied for now.
				// We might improve this later on if it causes unnecessary long blocking issues.
				if hasConfigChangeEntry(rd.CommittedEntries) {
					// wait for applied
					<-applyNotifyChan
				}

				// finish processing incoming messages before we signal raftdone chan
				r.transport.Send(processMessages(r, rd.Messages))
			}

			r.raftNode.Advance()

			handleDuration := time.Now().Sub(beginTime)
			if handleDuration > time.Millisecond*100 {
				glog.Warningf("[raft_group_handler.go-startRaftGroupReadyLoop]: handle ready use %v, "+
					"Entries count %d, CommittedEntries count %d",
					handleDuration, len(rd.Entries), len(rd.CommittedEntries))
			}

		case <-r.raftLoopStop:
			return

		}
	}
}

func startRaftGroupApplyLoop(r *RaftGroup) {

	defer func() {

		// save last apply idx before normal stop(normal stop vdl server)
		r.raftGroupStableStore.MustSaveApplyIndex(r.GroupConfig.GroupName, r.getApplyIndex())
		r.applyLoopStopped <- struct{}{}
	}()

	for {
		select {
		case ap := <-r.applyc:
			applyAll(&ap, r)

		case <-r.applyLoopStop:
			return
		}
	}
}

func startErrorCheckLoop(r *RaftGroup) {

	for {
		select {
		case err := <-r.errorChanForStop:
			glog.Errorf("critical errors occur %s", err)
			go r.Stop()
			return

		case <-r.stopping:
			return
		}
	}
}

// SoftStateChange is when leadership changes
func handleSoftStateChange(r *RaftGroup, softState *raft.SoftState) {
	// TODO. prometheus
	atomic.StoreUint64(&r.leaderID, softState.Lead)
}

func hasConfigChangeEntry(entries []raftpb.Entry) bool {
	var have bool = false
	for _, ent := range entries {
		if ent.Type == raftpb.EntryConfChange {
			have = true
			break
		}
	}
	return have
}

func hasNormalEntry(entries []raftpb.Entry) bool {
	var have bool = false
	for _, ent := range entries {
		if ent.Type == raftpb.EntryNormal {
			have = true
			break
		}
	}
	return have
}

// Save hard state before send
// save if term changes or vote changes
// didn't save commit index, use apply idx to restart recover in VDL
func saveHardState(r *RaftGroup, preHardState *raftpb.HardState, currentHardState *raftpb.HardState) {
	if !raft.IsEmptyHardState(*currentHardState) {

		// update current term
		if currentHardState.Term != r.currentTerm {
			r.currentTerm = currentHardState.Term
		}

		// persistent hard state when term changes or vote changes
		isTermChange := currentHardState.Term > preHardState.Term
		isVoteChange := (currentHardState.Term == preHardState.Term) &&
			(preHardState.Vote != currentHardState.Vote)

		if isTermChange || isVoteChange {
			r.raftGroupStableStore.MustSaveVote(r.GroupConfig.GroupName, currentHardState.Vote, currentHardState.Term)
			preHardState.Term = currentHardState.Term
			preHardState.Vote = currentHardState.Vote
		}
	}
}

func saveRaftLog(r *RaftGroup, entries []raftpb.Entry) {
	storageLastIndex, err := r.storage.LastIndex()
	if err != nil {
		glog.Fatalln("read last index from storage should not be error", err)
	}
	if storageLastIndex >= entries[0].Index {
		startTime := time.Now()
		err = r.storage.DeleteRaftLog(entries[0].Index)
		//debug info
		if glog.V(1) {
			glog.Infof("[raft_group_handler.go-saveRaftLog]: delete raft log,use %v", time.Now().Sub(startTime))
		}
		if err != nil {
			glog.Fatalf("[raft_group_handler.go-saveRaftLog]:DeleteRaftLog error,raft index=%d,error=%s",
				entries[0].Index, err.Error())
		}
	}
	err = r.storage.StoreEntries(entries)
	if err != nil {
		glog.Fatalf("[raft_group_handler.go-saveRaftLog]: StoreEntries error: %v", err)
	}
}

func processMessages(r *RaftGroup, ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if r.Membership.IsIDRemoved(types.ID(ms[i].To)) {
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case r.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.heartbeatTimeoutDetector.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				glog.Warningf("%s failed to send out heartbeat to %x on time (exceeded the %v timeout for %v, is likely overloaded)",
					r.Membership.SelfMemberID, ms[i].To, r.heartbeatTimeoutDetector.GetMaxDuration(), exceed)
			}
		}
	}
	return ms
}
