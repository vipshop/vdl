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
	"github.com/vipshop/vdl/consensus/raftgroup/membership"
	"github.com/vipshop/vdl/pkg/types"
	"golang.org/x/net/context"
	"strconv"
	"testing"
	"time"
)

// Membership Integration Test
// cases refer to testcases.md

// Case1
func TestMembership_AddMemberSuccess(t *testing.T) {

	testInitAllDataFolder("TestMembership_AddMemberSuccess")

	raftGroupNodes := testCreateThreeNodes(t, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()

	time.Sleep(10 * time.Second)

	leaderNode := testGetLeaderNode(t, raftGroupNodes)

	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	urls, _ := types.NewURLs([]string{"http://127.0.0.1:13100"})
	m := membership.NewMember("vdladd", urls, "", &now)
	_, addError := leaderNode.AddMember(ctx, *m)
	cancel()

	if addError != nil {
		t.Fatalf("add member should success, but have error :%v", addError)
	}

	time.Sleep(1 * time.Second)
	for _, node := range raftGroupNodes {
		if !node.IsLeader() {
			if len(node.Membership.MembersMap) != 4 {
				t.Fatalf("node.Membership.MembersMap should be 4, but currently only have :%d",
					len(node.Membership.MembersMap))
			}
			if node.Membership.MembersMap[m.ID] == nil {
				t.Fatalf("node.Membership.MembersMap should have new member")
			}
		}
	}
	status := leaderNode.raftNode.Status()
	if len(status.Progress) != 4 {
		t.Fatalf("status should be 4, but currently only have :%d",
			len(status.Progress))
	}

	// should success after add one member
	proposeMessages := testCreateMessages(t, 10000)
	testDoProposeData(t, raftGroupNodes, proposeMessages)
	// 5s is enough to process raft group
	time.Sleep(5 * time.Second)
	// each cases use diff fetch bytes
	testDoCheckData(t, proposeMessages, raftGroupNodes, 1024*100, false)
}

// Case2
func TestMembership_RemoveFollowerSuccess(t *testing.T) {

	testInitAllDataFolder("TestMembership_RemoveFollowerSuccess")

	raftGroupNodes := testCreateThreeNodes(t, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()

	proposeMessages := testCreateMessages(t, 10000)
	testDoProposeData(t, raftGroupNodes, proposeMessages)

	time.Sleep(10 * time.Second)

	var removeFollower *RaftGroup
	for _, node := range raftGroupNodes {
		if !node.IsLeader() {
			removeFollower = node
			break
		}
	}
	leaderNode := testGetLeaderNode(t, raftGroupNodes)

	// remove follower
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, removeError := leaderNode.RemoveMember(ctx, uint64(removeFollower.Membership.GetSelfMemberID()))
	if removeError != nil {
		t.Fatalf("remove member should success, but have error :%v", removeError)
	}
	cancel()
	// 5s is enough to process raft group
	time.Sleep(5 * time.Second)

	// send another message, should be success
	proposeMessagesNew := testCreateMessages(t, 10000)
	testDoProposeData(t, raftGroupNodes, proposeMessagesNew)
	time.Sleep(5 * time.Second)

	// check whether progress have removed
	status := leaderNode.raftNode.Status()
	if len(status.Progress) != 2 {
		t.Fatalf("status should be 2, but currently have :%d",
			len(status.Progress))
	}
}

// Case3
func TestMembership_RemoveLeaderSuccess(t *testing.T) {

	testInitAllDataFolder("TestMembership_RemoveLeaderSuccess")

	raftGroupNodes := testCreateThreeNodes(t, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	proposeMessages := testCreateMessages(t, 10000)
	testDoProposeData(t, raftGroupNodes, proposeMessages)

	// 5s is enough to process raft group
	time.Sleep(10 * time.Second)
	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	leaderNodeID := leaderNode.Membership.SelfMemberID

	// remove leader
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, removeError := leaderNode.RemoveMember(ctx, uint64(leaderNode.Membership.GetSelfMemberID()))
	if removeError != nil {
		t.Fatalf("remove member should success, but have error :%v", removeError)
	}
	cancel()
	// 5s is enough to process raft group
	time.Sleep(5 * time.Second)

	// check leader changed
	leaderNodeNew := testGetLeaderNode(t, raftGroupNodes)
	if leaderNodeNew.Membership.GetSelfMemberID() == leaderNodeID {
		t.Fatalf("leader should change")
	}

	// send another message, should be success
	proposeMessagesNew := testCreateMessages(t, 60000)
	testDoProposeData(t, raftGroupNodes, proposeMessagesNew)
	time.Sleep(5 * time.Second)

	// check whether progress have removed
	status := leaderNodeNew.raftNode.Status()
	if len(status.Progress) != 2 {
		t.Fatalf("status should be 2, but currently have :%d",
			len(status.Progress))
	}
}

// Case4
func TestMembership_UpdateFollowerSuccess(t *testing.T) {

	testInitAllDataFolder("TestMembership_UpdateFollowerSuccess")
	raftGroupNodes := testCreateThreeNodes(t, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()

	time.Sleep(10 * time.Second)

	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	var updateFollower *RaftGroup
	for _, node := range raftGroupNodes {
		if !node.IsLeader() {
			updateFollower = node
			break
		}
	}
	updateMember := membership.Member{
		ID:       updateFollower.Membership.GetSelfMemberID(),
		PeerURLs: []string{"http://127.0.0.1:13200"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := leaderNode.UpdateMember(ctx, updateMember)
	cancel()
	if err != nil {
		t.Fatalf("update member should success, but have error :%v", err)
	}

	// after update node peer, when not restart, should be ok using pipeline channel
	proposeMessages := testCreateMessages(t, 10000)
	testDoProposeData(t, raftGroupNodes, proposeMessages)

	// 5s is enough to process raft group
	time.Sleep(5 * time.Second)
	// each cases use diff fetch bytes
	testDoCheckData(t, proposeMessages, raftGroupNodes, 1024*100, false)
}

// Case5
func TestMembership_AddMemberForSingleNodeCluster(t *testing.T) {

	testInitAllDataFolder("TestMembership_AddMemberForSingleNodeCluster")

	raftGroupNode := testCreateSingleNode(t, 0)
	defer func() {
		if raftGroupNode.IsStarted() {
			raftGroupNode.Stop()
		}
	}()

	time.Sleep(10 * time.Second)

	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	urls, _ := types.NewURLs([]string{"http://127.0.0.1:13100"})
	m := membership.NewMember("vdladd", urls, "", &now)
	_, addError := raftGroupNode.AddMember(ctx, *m)
	cancel()

	if addError != nil {
		t.Fatalf("add member should success, but have error :%v", addError)
	}
}

// Case6
func TestMembership_AddMemberForHealthTwoNodeCluster(t *testing.T) {

	testInitAllDataFolder("TestMembership_AddMemberForHealthTwoNodeCluster")

	raftGroupNodes := testCreateTwoNodes(t, 0)
	defer func() {
		testCloseRaftGroups(raftGroupNodes)
	}()

	time.Sleep(10 * time.Second)
	leaderNode := testGetLeaderNode(t, raftGroupNodes)

	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	urls, _ := types.NewURLs([]string{"http://127.0.0.1:13100"})
	m := membership.NewMember("vdladd", urls, "", &now)
	_, addError := leaderNode.AddMember(ctx, *m)
	cancel()

	if addError != nil {
		t.Fatalf("add member should success, but have error :%v", addError)
	}

	// send message, should be success
	proposeMessagesNew := testCreateMessages(t, 60000)
	testDoProposeData(t, raftGroupNodes, proposeMessagesNew)
	time.Sleep(5 * time.Second)

	// check whether progress have removed
	status := leaderNode.raftNode.Status()
	if len(status.Progress) != 3 {
		t.Fatalf("status should be 3, but currently have :%d",
			len(status.Progress))
	}
}

// Case7
func TestMembership_AddMemberForUnHealthTwoNodeCluster(t *testing.T) {

	testInitAllDataFolder("TestMembership_AddMemberForUnHealthTwoNodeCluster")

	raftGroupNodes := testCreateTwoNodes(t, 0)
	defer func() {
		testCloseRaftGroups(raftGroupNodes)
	}()

	time.Sleep(1 * time.Second)
	// stop a raft group node
	if err := raftGroupNodes[0].Stop(); err != nil {
		t.Fatalf("stop raft group[0] error :%v", err)
	}
	time.Sleep(10 * time.Second)

	// should not have leader, check 10s
	for i := 0; i < 1000; i++ {
		for _, group := range raftGroupNodes {
			if group.IsLeader() {
				t.Fatalf("should have leader")
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	urls, _ := types.NewURLs([]string{"http://127.0.0.1:13100"})
	m := membership.NewMember("vdladd", urls, "", &now)
	_, addError := raftGroupNodes[1].AddMember(ctx, *m)
	cancel()

	if addError == nil {
		t.Fatalf("should fail to add member in un health 2 node cluster")
	}
	t.Logf("add member success return err because cannot pass health check, error: %v", addError)

}

// Case8
func TestMembership_RemoveMemberForOneNodeCluster(t *testing.T) {

	testInitAllDataFolder("TestMembership_RemoveMemberForOneNodeCluster")

	raftGroupNode := testCreateSingleNode(t, 0)
	defer func() {
		if raftGroupNode.IsStarted() {
			raftGroupNode.Stop()
		}
	}()

	time.Sleep(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := raftGroupNode.RemoveMember(ctx, uint64(raftGroupNode.Membership.SelfMemberID))
	cancel()

	if err == nil {
		t.Fatalf("should fail to remove member in one node cluster")
	}
	t.Logf("remove member success return err because cannot pass health check, error: %v", err)

}

// Case9
func TestMembership_RemoveMemberForTwoNodeCluster(t *testing.T) {

	testInitAllDataFolder("TestMembership_RemoveMemberForTwoNodeCluster")

	raftGroupNodes := testCreateTwoNodes(t, 0)
	defer func() {
		testCloseRaftGroups(raftGroupNodes)
	}()

	time.Sleep(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := raftGroupNodes[0].RemoveMember(ctx, uint64(raftGroupNodes[0].Membership.SelfMemberID))
	cancel()

	if err != nil {
		t.Fatalf("should success to remove member in two node cluster, error : %v", err)
	}

	time.Sleep(1 * time.Second)
	// check leader node
	testGetLeaderNode(t, raftGroupNodes)

}

// Case10
func TestMembership_RemoveMemberForUnHealthThreeNodeCluster(t *testing.T) {

	testInitAllDataFolder("TestMembership_RemoveMemberForUnHealthThreeNodeCluster")

	raftGroupNodes := testCreateThreeNodes(t, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()

	time.Sleep(10 * time.Second)

	// stop a raft group node
	if err := raftGroupNodes[0].Stop(); err != nil {
		t.Fatalf("stop raft group[0] error :%v", err)
	}

	time.Sleep(5 * time.Second)
	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := leaderNode.RemoveMember(ctx, uint64(leaderNode.Membership.SelfMemberID))
	cancel()

	if err == nil {
		t.Fatalf("should fail to remove member in UnHealth Three Node Cluster")
	}

	t.Logf("remove member success return err because cannot pass health check, error: %v", err)
}

// Case11
func TestMembership_AddMemberWithSameUrl(t *testing.T) {

	testInitAllDataFolder("TestMembership_AddMemberWithSameUrl")

	raftGroupNode := testCreateSingleNode(t, 0)
	defer func() {
		if raftGroupNode.IsStarted() {
			raftGroupNode.Stop()
		}
	}()

	time.Sleep(10 * time.Second)

	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	urls, _ := types.NewURLs([]string{"http://127.0.0.1:13001"})
	m := membership.NewMember("vdladd", urls, "", &now)
	_, addError := raftGroupNode.AddMember(ctx, *m)
	cancel()

	if addError == nil {
		t.Fatalf("AddMemberWithSameUrl should fail, but success")
	} else {
		t.Log(addError)
	}
}

// Case12
func TestMembership_UpdateMemberWithLeaderUrl(t *testing.T) {

	testInitAllDataFolder("TestMembership_UpdateMemberWithLeaderUrl")
	raftGroupNodes := testCreateThreeNodes(t, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()

	time.Sleep(10 * time.Second)

	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	var updateFollower *RaftGroup
	for _, node := range raftGroupNodes {
		if !node.IsLeader() {
			updateFollower = node
			break
		}
	}
	leaderPort := 13001
	if leaderNode.GroupConfig.VDLServerName == "vdl2" {
		leaderPort = 13002
	} else if leaderNode.GroupConfig.VDLServerName == "vdl3" {
		leaderPort = 13003
	}

	updateMember := membership.Member{
		ID:       updateFollower.Membership.GetSelfMemberID(),
		PeerURLs: []string{"http://127.0.0.1:" + strconv.Itoa(leaderPort)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := leaderNode.UpdateMember(ctx, updateMember)
	cancel()
	if err == nil {
		t.Fatalf("update UpdateMemberWithSameUrl should fail, but success")
	} else {
		t.Log(err)
	}

}

// Case13
func TestMembership_UpdateMemberWithSameUrl(t *testing.T) {

	testInitAllDataFolder("TestMembership_UpdateMemberWithSameUrl")
	raftGroupNodes := testCreateThreeNodes(t, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()

	time.Sleep(10 * time.Second)

	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	var updateFollower *RaftGroup
	for _, node := range raftGroupNodes {
		if !node.IsLeader() {
			updateFollower = node
			break
		}
	}

	updateFollowerPort := 13001
	if updateFollower.GroupConfig.VDLServerName == "vdl2" {
		updateFollowerPort = 13002
	} else if updateFollower.GroupConfig.VDLServerName == "vdl3" {
		updateFollowerPort = 13003
	}
	updateMember := membership.Member{
		ID:       updateFollower.Membership.GetSelfMemberID(),
		PeerURLs: []string{"http://127.0.0.1:" + strconv.Itoa(updateFollowerPort)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := leaderNode.UpdateMember(ctx, updateMember)
	cancel()
	if err != nil {
		t.Fatalf("UpdateMemberWithSameUrl should success, but have error :%v", err)
	}
}

func testCreateSingleNode(t *testing.T, segmentSize int64) *RaftGroup {

	// create 1 node
	InitialPeerURLsMap, _ := types.NewURLsMap("vdl1=http://127.0.0.1:13001")
	conf := testCreateRaftGroupConfigWithURLsMap(t, 1, InitialPeerURLsMap)
	if segmentSize > 0 {
		conf.LogStoreSegmentSize = segmentSize
	}
	raftGroup := testCreateRaftGroup(conf)
	if err := raftGroup.Start(); err != nil {
		t.Fatal(err)
	}
	return raftGroup
}

func testCreateTwoNodes(t *testing.T, segmentSize int64) []*RaftGroup {

	// create 3 nodes
	InitialPeerURLsMap, _ := types.NewURLsMap("vdl1=http://127.0.0.1:13001,vdl2=http://127.0.0.1:13002")
	raftGroupNodes := make([]*RaftGroup, 0)
	for i := 1; i <= 2; i++ {
		conf := testCreateRaftGroupConfigWithURLsMap(t, i, InitialPeerURLsMap)
		if segmentSize > 0 {
			conf.LogStoreSegmentSize = segmentSize
		}
		raftGroup := testCreateRaftGroup(conf)
		raftGroupNodes = append(raftGroupNodes, raftGroup)
		if err := raftGroup.Start(); err != nil {
			t.Fatal(err)
		}
	}
	return raftGroupNodes
}
