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
	"fmt"
	"time"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/idutil"
	"github.com/vipshop/vdl/consensus/raft"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/consensus/raftgroup/membership"
	"github.com/vipshop/vdl/logstore"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/raftutil"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/server/adminapi/apiclient"
	"github.com/vipshop/vdl/server/adminapi/apipb"
	"github.com/vipshop/vdl/version"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (r *RaftGroup) startOrRestartNode() (err error) {

	isNewStore := !existRaftLog(r.GroupConfig.DataDir)

	isNewNodeAndInitialCluster := isNewStore && r.GroupConfig.IsInitialCluster
	isNewNodeButAddToExistsCluster := isNewStore && !r.GroupConfig.IsInitialCluster
	isExistsNode := !isNewStore

	// if new node and initial cluster
	if isNewNodeAndInitialCluster {

		glog.Infof("start a new raft group and using initial cluster mode, "+
			"raft group name %s ", r.GroupConfig.GroupName)

		if err = r.GroupConfig.ValidateNewRaftGroup(); err != nil {
			return
		}

		r.Membership, err = membership.NewMembershipFromInitialPeerURLsMap(
			r.GroupConfig.InitialPeerURLsMap,
			r.GroupConfig.GroupName,
			r.GroupConfig.VDLServerName,
			r.basicStableStore)
		if err != nil {
			return err
		}

		glog.Infof("NewMembershipFromInitialPeerURLsMap")
		r.Membership.Print()

		// when new node and it's initial cluster, NewMembershipFromInitialPeerURLsMap will create meta also.
		// so persistence meta when create
		// Members in Membership will be persistence by Raft Apply process
		r.Membership.PersistenceMeta()

		if err = r.initStore(); err != nil {
			return err
		}

		// use initial peer to start a new raft, so raft will add peer log immediate after raft start
		peers := getMembershipPeers(r.Membership)
		r.startNode(peers)

	} else if isNewNodeButAddToExistsCluster {

		glog.Infof("start a new raft group and join to exists cluster, "+
			"raft group name %s ", r.GroupConfig.GroupName)

		if err = r.GroupConfig.ValidateNewRaftGroup(); err != nil {
			return
		}

		// create membership from initial peer urls
		// NOTE: will merge exists cluster membership later
		r.Membership, err = membership.NewMembershipFromInitialPeerURLsMap(
			r.GroupConfig.InitialPeerURLsMap,
			r.GroupConfig.GroupName,
			r.GroupConfig.VDLServerName,
			r.basicStableStore)
		if err != nil {
			return err
		}
		glog.Infof("NewMembershipFromInitialPeerURLsMap")
		r.Membership.Print()

		//get exists cluster membership to check
		existsClusterMembership, err := getExistsClusterMembership(
			r.GroupConfig.ExistsClusterAdminUrls.StringSlice(), r.GroupConfig.GroupName)
		if err != nil {
			glog.Warningf("cannot fetch exists cluster information, please veriy the %s, url is %v",
				r.GroupConfig.GroupName+"-existing-admin-urls", r.GroupConfig.ExistsClusterAdminUrls.StringSlice())
			return err
		}
		glog.Infof("getExistsClusterMembership")
		existsClusterMembership.Print()

		// validate local peer urls and exists urls
		// then re-create MembersMap , using the exists cluster peer ids
		if err = membership.ValidateClusterAndAssignIDs(r.Membership, existsClusterMembership); err != nil {
			return fmt.Errorf("error validating peerURLs %v: %v", existsClusterMembership, err)
		}

		// using exists cluster information for current node
		// NOTE: members is re-create in ValidateClusterAndAssignIDs func
		mergeMembershipFromExists(r.Membership, existsClusterMembership)
		r.Membership.PersistenceMeta()

		glog.Infof("After merge membership")
		r.Membership.Print()

		if err = r.initStore(); err != nil {
			return err
		}

		// will not use any initial peer, let raft using raft log to replay
		r.startNode(nil)

	} else if isExistsNode {

		glog.Infof("restart a exists raft group, "+
			"raft group name %s ", r.GroupConfig.GroupName)

		//restart
		r.Membership = membership.NewMembershipFromStore(
			r.GroupConfig.GroupName,
			r.basicStableStore,
			r.GroupConfig.VDLServerName)

		glog.Infof("NewMembershipFromStore")
		r.Membership.Print()

		if err = r.initStore(); err != nil {
			return err
		}

		r.restartNode()

	} else {
		glog.Fatalf("should not reach here in startOrRestartNode")
	}

	r.reqIDGen = idutil.NewGenerator(uint16(r.Membership.GetSelfMemberID()), time.Now())

	return nil
}

// when use initial peer to start a new raft, need peer from configuration
// when NewNodeButAddToExistsCluster, should use nil peers param
//    because new node must use exists raft log to handle membership
func (r *RaftGroup) startNode(peers []raft.Peer) {

	c := createRaftConfig(r)
	c.LastHardState = raftpb.HardState{}

	glog.Infof("starting member %s, ID %s in raft group %s", r.GroupConfig.VDLServerName,
		r.Membership.GetSelfMemberID(), r.GroupConfig.GroupName)

	r.raftNode = raft.StartNode(c, peers)

	advanceTicksForElection(r.raftNode, c.ElectionTick)
}

func (r *RaftGroup) restartNode() {

	// get persistent hard state from stable store
	lastPersistenceTerm, lastPersistenceVote := r.raftGroupStableStore.MustGetVote(r.GroupConfig.GroupName)
	lastPersistenceApplyIndex := r.raftGroupStableStore.MustGetApplyIndex(r.GroupConfig.GroupName)

	lastPersistentPeers := make([]uint64, len(r.Membership.MemberIDs()))
	for idx, id := range r.Membership.MemberIDs() {
		lastPersistentPeers[idx] = uint64(id)
	}

	r.setApplyIndex(lastPersistenceApplyIndex)
	r.setCommitIndex(lastPersistenceApplyIndex)

	// create raft config, NOTE: when restart node, should set LastHardState, LastPersistentPeers and Commit
	c := createRaftConfig(r)
	c.LastHardState = raftpb.HardState{Term: lastPersistenceTerm, Vote: lastPersistenceVote, Commit: lastPersistenceApplyIndex}
	c.LastPersistentPeers = lastPersistentPeers
	c.Applied = lastPersistenceApplyIndex

	glog.Infof("re-starting member %s, ID %s in raft group %s",
		r.GroupConfig.VDLServerName, r.Membership.GetSelfMemberID(), r.GroupConfig.GroupName)

	r.raftNode = raft.RestartNode(c)

	advanceTicksForElection(r.raftNode, c.ElectionTick)
}

func (r *RaftGroup) initStore() (err error) {

	maxSegmentSize := logstore.MaxSegmentSize
	if r.GroupConfig.LogStoreSegmentSize > 0 {
		maxSegmentSize = r.GroupConfig.LogStoreSegmentSize
	}

	// storage
	r.storage, err = logstore.NewLogStore(&logstore.LogStoreConfig{
		Dir:                 r.GroupConfig.DataDir,
		Meta:                &logstore.FileMeta{VdlVersion: version.LogStoreVersion},
		SegmentSizeBytes:    maxSegmentSize,
		ReserveSegmentCount: r.GroupConfig.ReserveSegmentCount,
		MemCacheSizeByte:    r.GroupConfig.MemCacheSizeByte,
		MaxSizePerMsg:       r.GroupConfig.MaxSizePerMsg,
	})

	if err != nil {
		return err
	}
	return nil
}

func createRaftConfig(r *RaftGroup) *raft.Config {
	raftConfig := &raft.Config{
		ID:              uint64(r.Membership.GetSelfMemberID()),
		ElectionTick:    r.GroupConfig.ElectionTicks,
		HeartbeatTick:   1,
		Storage:         r.storage,
		MaxSizePerMsg:   r.GroupConfig.MaxSizePerAppendEntries,
		MaxInflightMsgs: r.GroupConfig.MaxInflightAppendEntriesRequest,
		CheckQuorum:     true,
		PreVote:         r.GroupConfig.PreVote,
	}
	glog.Infof("create raft with config -")
	glog.Infof("  |- ID: %s", r.Membership.GetSelfMemberID())
	glog.Infof("  |- ElectionTick: %d", raftConfig.ElectionTick)
	glog.Infof("  |- HeartbeatTick: %d", raftConfig.HeartbeatTick)
	glog.Infof("  |- MaxSizePerMsg: %d", raftConfig.MaxSizePerMsg)
	glog.Infof("  |- MaxInflightMsgs: %d", raftConfig.MaxInflightMsgs)
	glog.Infof("  |- PreVote:%v", raftConfig.PreVote)
	return raftConfig
}

func getMembershipPeers(membership *membership.Membership) []raft.Peer {
	ids := membership.MemberIDs()
	peers := make([]raft.Peer, len(ids))
	for i, id := range ids {
		ctx, err := json.Marshal(membership.Member(id))
		if err != nil {
			glog.Fatalf("marshal member should never fail: %v", err)
		}
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}
	return peers
}

// get exists cluster information from exists admin listener urls
func getExistsClusterMembership(urls []string, logstream string) (*membership.Membership, error) {

	client, err := apiclient.NewClient(urls, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer func() {
		if client != nil {
			client.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, errList := client.MemberList(ctx, &apipb.MemberListRequest{LogstreamName: logstream}, grpc.FailFast(false))
	cancel()
	if errList != nil {
		return nil, errList
	}

	meta := membership.MembershipMeta{
		ClusterID: types.ID(resp.Header.ClusterId),
	}
	members := membership.MembershipMembers{
		MembersMap: raftutil.ProtoMembersToMemberMap(resp.Members),
		RemovedMap: raftutil.ProtoRemovedMembersToRemovedMap(resp.RemovedMembers),
	}

	membership := &membership.Membership{
		MembershipMeta:    meta,
		MembershipMembers: members,
		RaftGroupName:     resp.Header.LogstreamName,
	}

	return membership, nil
}

func mergeMembershipFromExists(local *membership.Membership, existsMembership *membership.Membership) {
	local.ClusterID = existsMembership.ClusterID
	for key, value := range existsMembership.RemovedMap {
		local.RemovedMap[key] = value
	}
}

// advanceTicksForElection advances ticks to the node for fast election.
// This reduces the time to wait for first leader election if bootstrapping the whole
// cluster, while leaving at least 1 heartbeat for possible existing leader
// to contact it.
func advanceTicksForElection(n raft.Node, electionTicks int) {
	for i := 0; i < electionTicks-1; i++ {
		n.Tick()
	}
}

func existRaftLog(dirpath string) bool {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return false
	}
	return len(names) != 0
}
