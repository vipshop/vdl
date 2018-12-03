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
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"fmt"

	"github.com/coreos/etcd/pkg/idutil"
	"github.com/vipshop/vdl/consensus/raft"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/consensus/raftgroup/membership"
	"github.com/vipshop/vdl/consensus/rafthttp"
	"github.com/vipshop/vdl/consensus/raftstore"
	"github.com/vipshop/vdl/pkg/contention"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/pkg/wait"
	"github.com/vipshop/vdl/stablestore"
	"golang.org/x/net/context"
)

const (
	// max number of in-flight snapshot messages allows to have
	// This number is more than enough for most clusters with 5 machines.
	maxInFlightMsgSnap = 16

	// HealthInterval is the minimum time the cluster should be healthy
	// before accepting add member requests.
	HealthInterval = 5 * time.Second
)

var (
	ErrStopped               = errors.New("VDL: raftgroup stopped")
	ErrTimeoutLeaderTransfer = errors.New("VDL: Leader Transfer Timeout")
)

// apply contains entries, snapshot to be applied. Once
// an apply is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type apply struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot

	// notifyc synchronizes VDL applies with the raft node
	notifyc chan struct{}
}

type confChangeResponse struct {
	members []*membership.Member
	err     error
}

type RaftGroup struct {
	GroupConfig *GroupConfig

	Membership *membership.Membership

	// Cache of the current leader the server has seen.
	// this fields need to be atomic access
	leaderID uint64

	// Cache of current term and apply term
	// didn't persistent
	currentTerm      uint64
	currentApplyTerm uint64

	// Cache of the latest commitIndex and applyIndex.
	// this fields need to be atomic access
	commitIndex uint64
	applyIndex  uint64

	// use to trigger save apply index processor
	lastSaveApplyIndex uint64
	lastSaveApplyTime  time.Time

	// a chan to send/receive snapshot
	msgSnapC chan raftpb.Message

	// a chan to send out apply
	applyc chan apply

	// ticker is a timer to trigger raft lib state machine
	ticker *time.Ticker

	// contention detectors for raft heartbeat message
	// when server receive/send heartbeat longer than heartbeat time
	// server maybe overload
	heartbeatTimeoutDetector *contention.TimeoutDetector

	// stop chan is use to stop raft loop
	raftLoopStop     chan struct{}
	raftLoopStopped  chan struct{}
	applyLoopStop    chan struct{}
	applyLoopStopped chan struct{}

	//stopping is use to fast fail for client request
	stopping chan struct{}

	//peer listener
	peerListener []*peerListener

	// transport specifies the transport to send and receive msgs to members.
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If transport is nil, server will panic.
	transport rafthttp.Transporter

	// raft node
	raftNode raft.Node

	// use for receive error and trigger raft group stop
	errorChanForStop chan error

	//request id generator
	reqIDGen *idutil.Generator

	// wait is use for waiting response
	wait wait.Wait

	// log store, it's use for raft log
	storage raftstore.LogStore

	// basic stable store, don't use directly
	basicStableStore stablestore.StableStore

	// raft group stable store
	raftGroupStableStore *RaftGroupStableStore

	// raft state
	runState RaftGroupRunState

	// use to wait message arrive
	messageArriveChan chan struct{}

	vdlServerInfo
}

func NewRaftGroup(conf *GroupConfig, stableStore stablestore.StableStore) (*RaftGroup, error) {

	//validate configuration
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	r := &RaftGroup{
		GroupConfig:      conf,
		basicStableStore: stableStore,
	}
	r.runState = raftGroupStopped

	return r, nil
}

func (r *RaftGroup) GetLogStore() raftstore.LogStore {
	return r.storage
}

func (r *RaftGroup) AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {

	// check urls is exists in current membership
	clusterMembers := r.Membership.Members()
	for _, clusterMember := range clusterMembers {
		if IsStringEqualAny(clusterMember.PeerURLs, memb.PeerURLs) {
			errMsg := fmt.Sprintf("Can't add new node, new node url [%v] had exists in cluster"+
				"[ %s, %s, %v]",
				memb.PeerURLs, clusterMember.Name, clusterMember.ID, clusterMember.PeerURLs)
			return nil, errors.New(errMsg)
		}
	}

	if r.GroupConfig.StrictMemberChangesCheck {
		// because must be executed in leader, so check the membership from memory can be ok
		oldMemberCount := r.Membership.MemberCount()

		// the follow is check current cluster can add new node and run normally
		// eg.
		// 1) when current have 1 node, add new node, that's ok for special case
		// 2) when current have 3 nodes, add new node, new cluster should be 4 nodes and quorum can be 3
		//    so current cluster's 3 nodes must be healthy,
		//    otherwise, when new node add, cluster cannot run
		newQuorumCount := (oldMemberCount+1)/2 + 1
		currentActiveMemberCount := numConnectedSince(r.transport,
			time.Now().Add(-HealthInterval), r.Membership.SelfMemberID, r.Membership.Members())

		glog.Infof("cluster info when add member, oldMemberCount :%d, newQuorumCount:%d,currentActiveMemberCount :%d",
			oldMemberCount, newQuorumCount, currentActiveMemberCount)

		if currentActiveMemberCount < newQuorumCount && oldMemberCount != 1 {
			errMsg := fmt.Sprintf("current cluster have %d nodes, "+
				"but just have %d nodes healthy, "+
				"cluster cannot run when add new node, please recover the bad node first",
				oldMemberCount, currentActiveMemberCount)
			return nil, errors.New(errMsg)
		}
	}

	b, err := json.Marshal(memb)
	if err != nil {
		return nil, err
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(memb.ID),
		Context: b,
	}
	return r.configure(ctx, cc)
}

func (r *RaftGroup) RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error) {

	if r.GroupConfig.StrictMemberChangesCheck {
		// because must be executed in leader, so check the membership from memory can be ok
		oldMemberCount := r.Membership.MemberCount()

		if oldMemberCount == 1 {
			return nil, errors.New("cannot remove member because there just have one node")
		}

		// 1) cluster have 2 nodes and remove one can be ok for all conditions
		// 2) check need when cluster more than 2 nodes
		if oldMemberCount > 2 {
			newQuorumCount := (oldMemberCount-1)/2 + 1
			exceptRemoveActiveMemberCount := numConnectedSinceExcept(r.transport,
				time.Now().Add(-HealthInterval), r.Membership.SelfMemberID,
				r.Membership.Members(), types.ID(id))
			if exceptRemoveActiveMemberCount < newQuorumCount {
				errMsg := fmt.Sprintf("current cluster have %d nodes, "+
					"except remove member, have %d nodes healthy, "+
					"cluster cannot run when remove this node, please recover the bad node first",
					oldMemberCount, exceptRemoveActiveMemberCount)
				return nil, errors.New(errMsg)
			}

		}
	}

	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}
	return r.configure(ctx, cc)
}

func (r *RaftGroup) UpdateMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {

	// check update urls is exists in cluster other memberships
	clusterMembers := r.Membership.Members()
	for _, clusterMember := range clusterMembers {
		// the update node urls has equal any other member urls, throw error
		if clusterMember.ID != memb.ID && IsStringEqualAny(clusterMember.PeerURLs, memb.PeerURLs) {
			errMsg := fmt.Sprintf("Can't update node, update node url[%v] had exists in cluster"+
				"[ %s, %s, %v]",
				memb.PeerURLs, clusterMember.Name, clusterMember.ID, clusterMember.PeerURLs)
			return nil, errors.New(errMsg)
		}
	}

	b, merr := json.Marshal(memb)
	if merr != nil {
		return nil, merr
	}

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeUpdateNode,
		NodeID:  uint64(memb.ID),
		Context: b,
	}
	return r.configure(ctx, cc)
}

func (r *RaftGroup) Start() (err error) {

	if r.runState != raftGroupStopped {
		err = errors.New("Raft Group is " + r.runState.String())
		return err
	}

	defer func() {
		if err != nil {
			r.runState = raftGroupStopped
		}
	}()

	glog.Infof("begin start node")

	r.init()

	r.runState = raftGroupStarting

	// start peer listener
	if err = r.startPeerListener(); err != nil {
		return
	}

	// start or restart raft group
	if err = r.startOrRestartNode(); err != nil {
		return
	}

	// init transport()
	if err = r.startPeerTransport(); err != nil {
		return
	}

	// set http transport handle
	r.setPeerHandler()

	// add members
	//TODO.
	for _, m := range r.Membership.Members() {
		if m.ID != r.Membership.GetSelfMemberID() {
			r.transport.AddPeer(m.ID, m.PeerURLs)
		}
	}

	// start raft handler
	r.startRaftGroupHandler()

	r.startPeerHandler()

	r.runState = raftGroupStarted

	glog.Infof("node %v started", r.Membership.SelfMemberID)

	return
}

func (r *RaftGroup) Stop() (err error) {

	if r.runState != raftGroupStarted {
		err = errors.New("Raft Group is " + r.runState.String())
		return err
	}

	glog.Infof("begin stop node:%v", r.Membership.SelfMemberID)

	r.runState = raftGroupStopping

	// no leader first
	r.setLeaderID(0)

	// notify stopping, fast fail
	close(r.stopping)

	// stop ticker
	r.ticker.Stop()

	// stop raft loop
	select {
	case r.raftLoopStop <- struct{}{}:
		<-r.raftLoopStopped
	case <-r.raftLoopStopped:
	}

	// stop apply loop
	select {
	case r.applyLoopStop <- struct{}{}:
		<-r.applyLoopStopped
	case <-r.applyLoopStopped:
	}

	// stop raft lib inner process
	r.raftNode.Stop()
	r.transport.Stop()

	//stop listener
	for _, peerListener := range r.peerListener {
		err := peerListener.close(context.Background())
		if err != nil {
			glog.Errorf("can not close peer listener [%s] on raft group [%s]",
				peerListener.Addr().String(), r.GroupConfig.GroupName)
		}
	}

	// stop storage
	if err := r.storage.Close(); err != nil {
		glog.Fatalf("raft close storage error: %v", err)
	}

	r.runState = raftGroupStopped

	glog.Infof("node:%v stopped", r.Membership.SelfMemberID)

	return nil
}

// LeaderTransfer transfers the leader to the given transferee.
func (r *RaftGroup) LeaderTransfer(ctx context.Context, transferee uint64) error {

	now := time.Now()

	oldLeader := r.GetLeaderID()
	checkInterval := r.GroupConfig.HeartbeatMs

	glog.Infof("%s[%s] starts leadership transfer from %s to %s",
		r.Membership.SelfMemberID, r.GroupConfig.VDLServerName, types.ID(oldLeader), types.ID(transferee))

	r.raftNode.TransferLeadership(ctx, oldLeader, transferee)
	for r.GetLeaderID() != transferee {
		select {
		case <-ctx.Done(): // time out
			return ErrTimeoutLeaderTransfer
		case <-time.After(checkInterval):
		}
	}

	glog.Infof("%s[%s] finished leadership transfer from %s to %s (took %v)",
		r.Membership.SelfMemberID, r.GroupConfig.VDLServerName, types.ID(oldLeader), types.ID(transferee), time.Since(now))
	return nil
}

// propose data to step raft process
// data should be encode to application request ( kafka msg in VDL )
// this method should not be call outside the raft group
func (r *RaftGroup) propose(ctx context.Context, data []byte) error {
	if r.runState != raftGroupStarted {
		return errors.New("raft group not started")
	}
	return r.raftNode.Propose(ctx, data)
}

func (r *RaftGroup) proposeBatch(ctx context.Context, data [][]byte) error {
	if r.runState != raftGroupStarted {
		return errors.New("raft group not started")
	}
	return r.raftNode.ProposeBatch(ctx, data)
}

func (r *RaftGroup) init() (err error) {

	r.Membership = nil
	r.leaderID = 0
	r.commitIndex = 0
	r.applyIndex = 0
	r.lastSaveApplyIndex = 0
	r.currentApplyTerm = 0
	r.currentTerm = 0
	r.lastSaveApplyTime = time.Now()
	r.peerListener = nil
	r.transport = nil
	r.raftNode = nil
	r.reqIDGen = nil

	// set up contention detectors for raft heartbeat message.
	// expect to send a heartbeat within 3 heartbeat intervals.
	r.heartbeatTimeoutDetector = contention.NewTimeoutDetector(3 * r.GroupConfig.HeartbeatMs)
	r.msgSnapC = make(chan raftpb.Message, maxInFlightMsgSnap)
	r.applyc = make(chan apply)
	r.raftLoopStop = make(chan struct{})
	r.raftLoopStopped = make(chan struct{})
	r.applyLoopStop = make(chan struct{})
	r.applyLoopStopped = make(chan struct{})
	r.stopping = make(chan struct{})
	r.errorChanForStop = make(chan error)
	r.messageArriveChan = make(chan struct{})
	//r.wait = wait.NewWithStats(raftgroupmetrics.MetricsRaftLatency,r.GroupConfig.GroupName)
	r.wait = wait.New()
	r.raftGroupStableStore = NewRaftGroupStableStore(r.basicStableStore)
	r.runState = raftGroupStopped

	// init ticker
	if r.GroupConfig.HeartbeatMs == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.GroupConfig.HeartbeatMs)
	}

	return nil
}

// configure sends a configuration change through consensus and
// then waits for it to be applied to the server. It
// will block until the change is performed or there is an error.
func (r *RaftGroup) configure(ctx context.Context, cc raftpb.ConfChange) ([]*membership.Member, error) {
	cc.ID = r.reqIDGen.Next()
	ch := r.wait.Register(cc.ID)
	if err := r.raftNode.ProposeConfChange(ctx, cc); err != nil {
		r.wait.Trigger(cc.ID, nil)
		return nil, err
	}
	select {
	case x := <-ch:
		if x == nil {
			glog.Fatalf("configure trigger value should never be nil")
		}
		resp := x.(*confChangeResponse)
		return resp.members, resp.err
	case <-ctx.Done():
		r.wait.Trigger(cc.ID, nil) // GC wait
		return nil, ctx.Err()
	case <-r.stopping:
		return nil, ErrStopped
	}
}

// this function is use to handle the error that http.serve return
// http serve always returns a non-nil error. After Shutdown or Close, the
// returned error is ErrServerClosed.
func (r *RaftGroup) peerListenerErrHandler(err error) {

	if err == http.ErrServerClosed {
		glog.Info(err)
		return
	}

	select {
	case r.errorChanForStop <- err:
	default:
	}
}

func (r *RaftGroup) getApplyIndex() uint64 {
	return atomic.LoadUint64(&r.applyIndex)
}

func (r *RaftGroup) setApplyIndex(v uint64) {
	atomic.StoreUint64(&r.applyIndex, v)
}

func (r *RaftGroup) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *RaftGroup) setCommitIndex(v uint64) {
	atomic.StoreUint64(&r.commitIndex, v)
}

func (r *RaftGroup) GetLeaderID() uint64 {
	return atomic.LoadUint64(&r.leaderID)
}

func (r *RaftGroup) setLeaderID(v uint64) {
	atomic.StoreUint64(&r.leaderID, v)
}

func (r *RaftGroup) IsStarted() bool {
	return r.runState == raftGroupStarted
}

// numConnectedSince counts how many members are connected to the local member
// since the given time.
func numConnectedSince(transport rafthttp.Transporter, since time.Time, self types.ID, members []*membership.Member) int {
	connectedNum := 0
	for _, m := range members {
		if m.ID == self || isConnectedSince(transport, since, m.ID) {
			connectedNum++
		}
	}
	return connectedNum
}

// numConnectedSince counts how many members are connected to the local member
// since the given time.
func numConnectedSinceExcept(transport rafthttp.Transporter, since time.Time,
	self types.ID, members []*membership.Member, except types.ID) int {

	connectedNum := 0
	for _, m := range members {
		if (m.ID != except) && (m.ID == self || isConnectedSince(transport, since, m.ID)) {
			connectedNum++
		}
	}
	return connectedNum
}

// isConnectedSince checks whether the local member is connected to the
// remote member since the given time.
func isConnectedSince(transport rafthttp.Transporter, since time.Time, remote types.ID) bool {
	t := transport.ActiveSince(remote)
	return !t.IsZero() && t.Before(since)
}

//get keys from stablestore by batch
//use for snapshot metadata
func (r *RaftGroup) GetMetaBatch(keys []string) (map[string][]byte, error) {
	return r.basicStableStore.GetBatch(keys)
}
