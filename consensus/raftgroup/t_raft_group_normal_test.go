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

// cases in this file will start 3 raft node
// cases refer to testcases.md

package raftgroup

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/vipshop/vdl/consensus/raft/raftpb"
)

// ## Case 1
// RGIT: Raft Group Integration Test
func TestRGITBasic_Test5WProposeUsingDefaultConfig(t *testing.T) {

	testInitAllDataFolder("TestRGIT_Test5WProposeUsingDefaultConfig")

	raftGroupNodes := testCreateThreeNodes(t, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()

	proposeMessages := testCreateMessages(t, 50000)
	testDoProposeData(t, raftGroupNodes, proposeMessages)
	// 20S wait for raft process
	time.Sleep(20 * time.Second)
	// each cases use diff fetch bytes
	testDoCheckData(t, proposeMessages, raftGroupNodes, 1024*40, false)
}

// ## Case 2
func TestRGITBasic_Test2WProposeWithSmallSegmentSize(t *testing.T) {

	testInitAllDataFolder("TestRGIT_Test2WProposeWithSmallSegmentSize")
	raftGroupNodes := testCreateThreeNodes(t, 20*1024*1024)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()

	proposeMessages := testCreateMessages(t, 20000)
	testDoProposeData(t, raftGroupNodes, proposeMessages)
	// 5s is enough to process raft group
	time.Sleep(5 * time.Second)
	// each cases use diff fetch bytes
	testDoCheckData(t, proposeMessages, raftGroupNodes, 1024*100, false)

}

// ## Case 3
func TestRGITBasic_TestFetchRandomBytesPerTime(t *testing.T) {

	testInitAllDataFolder("TestRGIT_TestFetchRandomBytesPerTime")
	raftGroupNodes := testCreateThreeNodes(t, 20*1024*1024)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	proposeMessages := testCreateMessages(t, 3000)
	testDoProposeData(t, raftGroupNodes, proposeMessages)
	// 5s is enough to process raft group
	time.Sleep(5 * time.Second)
	testDoCheckData(t, proposeMessages, raftGroupNodes, 1024*5, true)
}

// ## Case 4
func TestRGITBasic_TestParallelProposeAndFetch(t *testing.T) {

	testInitAllDataFolder("TestRGIT_TestParallelProposeAndFetch")
	raftGroupNodes := testCreateThreeNodes(t, 20*1024*1024)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	proposeMessages := testCreateMessages(t, 20000)
	go testDoProposeData(t, raftGroupNodes, proposeMessages)

	var wg *sync.WaitGroup = new(sync.WaitGroup)
	for _, node := range raftGroupNodes {
		wg.Add(1)
		go func() {
			var fetchOffset int64 = 0
			for {
				actuallyMsgs, err, _ := node.ConsumeMessages(fetchOffset, 1024*100)
				if err != nil {
					panic(err)
				}
				if actuallyMsgs == nil {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				for _, actuallyMsg := range actuallyMsgs {
					if proposeMessages[fetchOffset] != string(actuallyMsg.Data) {
						t.Fatalf("data wrong, in offset: %d, expect data %v, actually data %v",
							fetchOffset, proposeMessages[fetchOffset], string(actuallyMsg.Data))
					}
					if actuallyMsg.Offset != fetchOffset {
						t.Fatalf("the offset inside data was wrong, expect offset: %d, actually offset %d",
							fetchOffset, actuallyMsg.Offset)
					}
					fetchOffset++
				}
				if fetchOffset == int64(len(proposeMessages)) {
					wg.Done()
					break
				}
			}
		}()
	}
	wg.Wait()
}

// case 5 , Raft Log will be delete when follow have wrong log for same index the diff term
// refer to raft consensus
func TestRGITBasic_TestDeleteRaftLogWhenNewTermElection(t *testing.T) {

	testInitAllDataFolder("TestRGIT_TestDeleteRaftLogWhenNewTermElection")
	raftGroupNodes := testCreateThreeNodes(t, 20*1024*1024)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	proposeMessages := testCreateMessages(t, 10)
	testDoProposeDataAndWait(t, raftGroupNodes, proposeMessages)

	// get leader information
	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	lastIndex, err := leaderNode.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	lastTerm, err := leaderNode.storage.Term(lastIndex)
	if err != nil {
		panic(err)
	}
	t.Logf("first propose, last index:%d, last term: %d\n", lastIndex, lastTerm)
	fmt.Printf("first propose, last index:%d, last term: %d\n", lastIndex, lastTerm)

	// stop 2 raft node
	raftGroupNodes[0].Stop()
	raftGroupNodes[1].Stop()

	// insert wrong message(which will be replace by new leader) into the last node
	wrongMessages := testCreateMessages(t, 20)
	wrongEntries := make([]raftpb.Entry, 0)

	for i, msg := range wrongMessages {
		b := make([]byte, 8+len(msg))
		binary.BigEndian.PutUint64(b, uint64(i+1))
		copy(b[8:], []byte(msg))

		entry := raftpb.Entry{
			Term:  lastTerm,
			Index: lastIndex + uint64(i+1),
			Type:  raftpb.EntryNormal,
			Data:  b,
		}
		wrongEntries = append(wrongEntries, entry)

	}
	raftGroupNodes[2].storage.StoreEntries(wrongEntries)
	fmt.Printf("save wrong message success, begin index: %d, term: %d\n", wrongEntries[0].Index, wrongEntries[0].Term)
	wrongIndex, err := raftGroupNodes[2].storage.LastIndex()
	if err != nil {
		panic(err)
	}
	t.Logf("save wrong message to node[2], last index :%d", wrongIndex)

	raftGroupNodes[2].Stop()

	// restart
	raftGroupNodes[0].Start()
	raftGroupNodes[1].Start()

	// wait a new leader
	time.Sleep(5 * time.Second)
	leaderNode = testGetLeaderNode(t, raftGroupNodes)
	leaderLastIndex, err := leaderNode.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	leaderLastTerm, err := leaderNode.storage.Term(leaderLastIndex)
	if err != nil {
		panic(err)
	}
	t.Logf("after restart, leader last term: %d, last index: %d\n", leaderLastTerm, leaderLastIndex)
	fmt.Printf("after restart, leader last term: %d, last index: %d\n", leaderLastTerm, leaderLastIndex)

	// start the last one node
	raftGroupNodes[2].Start()
	// wait leader append entries
	time.Sleep(5 * time.Second)

	// check the raft log of last node will be replicated by new leader
	lastIndex, err = raftGroupNodes[2].storage.LastIndex()
	if err != nil {
		panic(err)
	}

	lastTerm, err = raftGroupNodes[2].storage.Term(lastIndex)
	if err != nil {
		panic(err)
	}
	t.Logf("the node with wrong message after restart, last term: %d, last index:%d \n", lastTerm, lastIndex)
	fmt.Printf("the node with wrong message after restart, last term: %d, last index:%d \n", lastTerm, lastIndex)

	if lastIndex != leaderLastIndex {
		t.Fatalf("the node[2] after restart doesn't match the new leader, the wrong node index:%d, but the leader:%d", lastIndex, leaderLastIndex)
	}

	if lastTerm != leaderLastTerm {
		t.Fatalf("the node[2] after restart doesn't match the new leader, the wrong node term :%d, but the leader:%d", lastTerm, leaderLastTerm)
	}

}

// case 6
// check storage vote also, max/min Vindex, etc also
func Test_Unstable_RGIT_TestConsistentWhenRandomNodeStopMultiTimes(t *testing.T) {

	fullMessages := make([]string, 0)
	stopTimes := 30

	testInitAllDataFolder("TestRGIT_TestConsistentWhenLeaderChangeMultiTimes")
	raftGroupNodes := testCreateThreeNodes(t, 20*1024*1024)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	proposeMessages := testCreateMessages(t, 100)
	testDoProposeDataAndWait(t, raftGroupNodes, proposeMessages)

	fullMessages = append(fullMessages, proposeMessages...)

	for i := 0; i < stopTimes; i++ {
		stopNodeIndex := rand.Intn(3)
		memberID := raftGroupNodes[stopNodeIndex].Membership.GetSelfMemberID()
		if raftGroupNodes[stopNodeIndex].IsLeader() {
			fmt.Println("stop leader ", memberID)
		} else {
			fmt.Println("stop node ", memberID)
		}
		raftGroupNodes[stopNodeIndex].Stop()

		// append again
		proposeMessages = testCreateMessages(t, 100)
		testDoProposeDataAndWait(t, raftGroupNodes, proposeMessages)

		fullMessages = append(fullMessages, proposeMessages...)

		fmt.Println("start node ", memberID)
		raftGroupNodes[stopNodeIndex].Start()
	}

	// 5s is enough to process raft group
	time.Sleep(5 * time.Second)
	testDoCheckData(t, fullMessages, raftGroupNodes, 1024*100, false)
}

// case 7
func Test_Unstable_RGIT_TestStopLeaderWhileProposing(t *testing.T) {

	testInitAllDataFolder("TestRGIT_TestStopLeaderWhileProposing")
	raftGroupNodes := testCreateThreeNodes(t, 100*1024*1024)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()

	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			default:
				msgs := testCreateMessages(t, 20)
				leaderNode := testGetLeaderNode(t, raftGroupNodes)
				msgLen := len(msgs)
				// generate random string by random length [1-1024) and propose
				for i := 0; i < msgLen; i++ {
					_, err := leaderNode.StoreMessage([]byte(msgs[i]))
					if err != nil {
						fmt.Println(err)
						break
					}

				}
				//time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	time.Sleep(5 * time.Second)
	for i := 0; i < 20; i++ {
		leaderNode := testGetLeaderNode(t, raftGroupNodes)
		fmt.Println("stop leader node: ", leaderNode.Membership.GetSelfMemberID())
		leaderNode.Stop()

		timeout := time.After(10 * time.Second)
		for {
			select {
			case <-timeout:
				close(done)
				t.Fatalf("node should stop in 10 seconds")
			default:
			}
			if leaderNode.runState == raftGroupStopped {
				break
			}
		}

		time.Sleep(5 * time.Second) // wait sending

		leaderNode.Start()
		timeout = time.After(10 * time.Second)
		for {
			select {
			case <-timeout:
				close(done)
				t.Fatalf("node should start in 10 seconds")
			default:
			}
			if leaderNode.runState == raftGroupStarted {
				break
			}
		}
		time.Sleep(5 * time.Second)
	}

	// should have leader node
	testGetLeaderNode(t, raftGroupNodes)

	close(done)
}

// case 8
func Test_Unstable_RGIT_TestStopNodeLongTimeAndStartAgain(t *testing.T) {

	testInitAllDataFolder("TestRGIT_TestStopNodeLongTimeAndStartAgain")

	// create 3 nodes , but start 2 nodes
	raftGroupNodes := make([]*RaftGroup, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	for i := 1; i <= 3; i++ {
		conf := testCreateRaftGroupConfig(t, i)
		conf.LogStoreSegmentSize = 1 * 1024 * 1024
		raftGroup := testCreateRaftGroup(conf)
		raftGroupNodes = append(raftGroupNodes, raftGroup)
	}
	raftGroupNodes[0].Start()
	raftGroupNodes[1].Start()

	// propose messages
	count := 100000
	proposeMessages := testCreateMessages(t, count)
	startRaftGroupNodes := make([]*RaftGroup, 0)
	startRaftGroupNodes = append(startRaftGroupNodes, raftGroupNodes[0])
	startRaftGroupNodes = append(startRaftGroupNodes, raftGroupNodes[1])
	testDoProposeDataAndWait(t, startRaftGroupNodes, proposeMessages)

	raftGroupNodes[2].Start()
	var expectCatchupTime time.Duration = 60 * 2 * time.Second

	beginTime := time.Now()
	for {
		maxVindex, err := raftGroupNodes[2].GetLogStore().MaxVindex(raftGroupNodes[2].applyIndex)
		if err != nil {
			panic(err)
		}
		fmt.Printf("node 2 apply index :%d, vindex:%d \n", raftGroupNodes[2].applyIndex, maxVindex)
		if maxVindex != int64(count-1) {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
		if time.Now().Sub(beginTime) > expectCatchupTime {
			t.Fatalf("node 2 cannot catch up the log in %v", expectCatchupTime)
			break
		}
	}

}

// case 9
func TestRGIT_TestSnapshot(t *testing.T) {

	testInitAllDataFolder("TestRGIT_TestSnapshot")

	// create 3 nodes , but start 2 nodes
	raftGroupNodes := make([]*RaftGroup, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	for i := 1; i <= 3; i++ {
		conf := testCreateRaftGroupConfig(t, i)
		conf.LogStoreSegmentSize = 10 * 1024 * 1024
		raftGroup := testCreateRaftGroup(conf)
		raftGroupNodes = append(raftGroupNodes, raftGroup)
	}
	raftGroupNodes[0].Start()
	raftGroupNodes[1].Start()

	// propose messages
	proposeMessages := testCreateMessages(t, 3000)
	startRaftGroupNodes := make([]*RaftGroup, 0)
	startRaftGroupNodes = append(startRaftGroupNodes, raftGroupNodes[0])
	startRaftGroupNodes = append(startRaftGroupNodes, raftGroupNodes[1])
	testDoProposeDataAndWait(t, startRaftGroupNodes, proposeMessages)

	node1LastApplyIndex := raftGroupNodes[0].applyIndex

	// delete raft log
	raftGroupNodes[0].GetLogStore().DeleteFiles([]string{"0000000000000000.log", "0000000000000001.log"})
	raftGroupNodes[1].GetLogStore().DeleteFiles([]string{"0000000000000000.log", "0000000000000001.log"})

	// start node 3, but cannot fetch log because the other 2 node had delete log
	raftGroupNodes[2].Start()

	// wait and check
	// 1) other 2 nodes will not crash
	// 2) the node 3 cannot fetch the log
	time.Sleep(10 * time.Second)

	lastIndex, err := raftGroupNodes[2].storage.LastIndex()
	if err != nil {
		panic(err)
	}

	if lastIndex != 3 {
		t.Fatalf("the node 3 should just have only 3 entries because it haven't snapshot, but actually have %d", lastIndex)
	}

	// propose 100 message again
	proposeMessages = testCreateMessages(t, 100)
	testDoProposeDataAndWait(t, raftGroupNodes, proposeMessages)
	againNode1LastApplyIndex := raftGroupNodes[0].applyIndex

	if againNode1LastApplyIndex != node1LastApplyIndex+100 {
		t.Fatalf("node 1 apply index should be %d, but actually %d", node1LastApplyIndex+100, againNode1LastApplyIndex)
	}

}

// ## Case 10
func TestRGITBasic_TestMessageSizeExceedConfig(t *testing.T) {

	testInitAllDataFolder("TestRGIT_TestMessageSizeExceedConfig")

	raftGroupNodes := testCreateThreeNodes(t, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	proposeMessages := make([]string, 0)

	// generate 10M messages
	for i := 0; i < 10; i++ {
		msgString := testRandStringBytes(1024 * 1024 * 20)
		proposeMessages = append(proposeMessages, msgString)
	}

	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	msgLen := len(proposeMessages)
	// generate random string by random length [1-1024) and propose
	for i := 0; i < msgLen; i++ {
		_, err := leaderNode.StoreMessage([]byte(proposeMessages[i]))
		if err != ErrExceedMaxSizePerMsg {
			t.Fatalf("should have ErrExceedMaxSizePerMsg")
		}
	}
}

// ## Case 11
func TestRGIT_Unstable_TestLargeMessageSizeButUnderConfig(t *testing.T) {

	testInitAllDataFolder("TestRGIT_TestLargeMessageSizeButUnderConfig")

	// create 3 nodes
	raftGroupNodes := make([]*RaftGroup, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	for i := 1; i <= 3; i++ {
		conf := testCreateRaftGroupConfig(t, i)
		conf.MaxSizePerMsg = 21 * 1024 * 1024
		conf.LogStoreSegmentSize = 10 * 1024 * 1024
		raftGroup := testCreateRaftGroup(conf)
		raftGroupNodes = append(raftGroupNodes, raftGroup)
		if err := raftGroup.Start(); err != nil {
			t.Fatal(err)
		}
	}

	proposeMessages := make([]string, 0)
	// generate 10M messages
	for i := 0; i < 10; i++ {
		msgString := testRandStringBytes(1024 * 1024 * 20)
		proposeMessages = append(proposeMessages, msgString)
	}

	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	msgLen := len(proposeMessages)
	// generate random string by random length [1-1024) and propose
	for i := 0; i < msgLen; i++ {
		_, err := leaderNode.StoreMessage([]byte(proposeMessages[i]))
		if err != nil {
			t.Fatalf("should not have err %v", err)
		}
	}

	// 20S wait for raft process
	time.Sleep(20 * time.Second)
	// each cases use diff fetch bytes
	testDoCheckData(t, proposeMessages, raftGroupNodes, 1024*40, false)
}

// Case 12
func TestRGIT_Unstable_TestConsumeNotExistsMessages(t *testing.T) {

	testInitAllDataFolder("TestRGIT_TestConsumeNotExistsMessages")

	raftGroupNodes := testCreateThreeNodes(t, 20*1024*1024)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	proposeMessages := testCreateMessages(t, 3000)

	t.Logf("first message: %v", proposeMessages[0])
	testDoProposeData(t, raftGroupNodes, proposeMessages)

	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	leaderNode.GetLogStore().DeleteFiles([]string{"0000000000000000.log", "0000000000000001.log"})

	time.Sleep(2 * time.Second)

	entries, err, _ := leaderNode.ConsumeMessages(1, 1024*20)
	if err != nil {
		t.Errorf("consume message should not have err , err: %v", err)
	}
	if len(entries) <= 0 {
		t.Errorf("consume message should return entries > 0, entries:%v", entries)
	}

	if entries[0].Offset <= 1 {
		t.Errorf("consume message return entries should > 1 because log compaction, current: %v ", entries[0].Offset)
	}
	t.Logf("consume message return start entry offset:%d, data:%s", entries[0].Offset, string(entries[0].Data))

}

// case 13
func TestRGITBasic_TestSaveApplyIndexWhenNormalStop(t *testing.T) {

	testInitAllDataFolder("TestRGIT_TestSaveApplyIndexWhenNormalStop")

	raftGroupNodes := testCreateThreeNodes(t, 20*1024*1024)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	proposeMessages := testCreateMessages(t, 2000)
	beginTime := time.Now()
	testDoProposeDataAndWait(t, raftGroupNodes, proposeMessages)
	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	for _, group := range raftGroupNodes {
		err := group.Stop()
		if err != nil {
			panic(err)
		}
	}

	if time.Now().Sub(beginTime) > saveApplyIndexDuration {
		t.Fatalf("ProposeData Exceed %v", saveApplyIndexDuration)
	}
	t.Logf("ProposeData time:%v ", time.Now().Sub(beginTime))

	applyIndex := leaderNode.raftGroupStableStore.MustGetApplyIndex(leaderNode.GroupConfig.GroupName)
	t.Logf("applyIndex:%d", applyIndex)
	if applyIndex < 2000 {
		t.Fatalf("applyIndex should > 2000")
	}
}

// ## Case 14
func TestRGITBasic_Test5WProposeBatchUsingDefaultConfig(t *testing.T) {

	testInitAllDataFolder("TestRGIT_Test5WProposeBatchUsingDefaultConfig")

	msgLen := 32 * 1600
	raftGroupNodes := testCreateThreeNodes(t, 0)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	proposeMessages := testCreateMessages(t, msgLen)

	leaderNode := testGetLeaderNode(t, raftGroupNodes)

	// generate random string by random length [1-1024) and propose
	batch := 32
	batchDatas := make([][]byte, 32)
	index := 0
	for i := 0; i < msgLen; i++ {

		if index < batch {
			batchDatas[index] = []byte(proposeMessages[i])
			index++
		}

		if index == batch {
			_, err := leaderNode.StoreMessageBatch(batchDatas)
			if err != nil {
				panic(err)
			}
			index = 0
		}
	}

	// 20S wait for raft process
	time.Sleep(20 * time.Second)
	// each cases use diff fetch bytes
	testDoCheckData(t, proposeMessages, raftGroupNodes, 1024*40, false)
}

// TODO. need test unit all others function stable
//func TestMemberChanges(t *testing.T) {
//
//}

func testCreateThreeNodes(t *testing.T, segmentSize int64) []*RaftGroup {

	// create 3 nodes
	raftGroupNodes := make([]*RaftGroup, 0)
	for i := 1; i <= 3; i++ {
		conf := testCreateRaftGroupConfig(t, i)
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

func testCreateMessages(t *testing.T, recordCount int) []string {

	msgs := make([]string, 0)

	// generate random string by random length [1-1024) and propose
	for i := 0; i < recordCount; i++ {
		msgString := testRandStringBytes(rand.Intn(1024))
		msgs = append(msgs, msgString)
	}
	return msgs
}

func testDoProposeData(t *testing.T, raftGroupNodes []*RaftGroup, msgs []string) {

	leaderNode := testGetLeaderNode(t, raftGroupNodes)

	msgLen := len(msgs)
	// generate random string by random length [1-1024) and propose
	for i := 0; i < msgLen; i++ {
		_, err := leaderNode.StoreMessage([]byte(msgs[i]))
		if err != nil {
			panic(err)
		}
	}
}

func testDoProposeDataAndWait(t *testing.T, raftGroupNodes []*RaftGroup, msgs []string) {

	leaderNode := testGetLeaderNode(t, raftGroupNodes)
	waitChans := make([]<-chan interface{}, 0)

	msgLen := len(msgs)
	// generate random string by random length [1-1024) and propose
	for i := 0; i < msgLen; i++ {
		wait, err := leaderNode.StoreMessage([]byte(msgs[i]))
		if err != nil {
			panic(err)
		}
		waitChans = append(waitChans, wait)
	}
	for i := 0; i < len(waitChans); i++ {
		<-waitChans[i]
	}
}

func testDoCheckData(t *testing.T, expectMessages []string, raftGroupNodes []*RaftGroup, fetchMaxBytes int32, randomBytes bool) {
	msgLen := len(expectMessages)
	// check
	for _, node := range raftGroupNodes {

		t.Logf("checking data from node: %v", node.GroupConfig.VDLServerName)

		maxVIndex, err := node.storage.MaxVindex(node.applyIndex)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("commitindex: %d, applyIndex: %d, store vindex: %d",
			node.commitIndex, node.applyIndex, maxVIndex)

		// check raft: apply index and commit index,
		if node.commitIndex != node.applyIndex {
			t.Fatalf("commit index not equal with applyIndex, commit index %d, applyIndex index %d",
				node.commitIndex, node.applyIndex)
		}

		// check store: last vindex, start with 0
		if maxVIndex != int64(msgLen-1) {
			t.Fatalf("vindex wrong, expect %d, actually %d", msgLen-1, node.commitIndex)
		}

		// commit Index should large than vindex
		if node.commitIndex <= uint64(maxVIndex) {
			t.Fatalf("vindex should large than commitIndex, commit index %d, vindex %d",
				node.commitIndex, maxVIndex)
		}

		// check data
		// check for each node
		var fetchOffset int64 = 0
		for {
			fetchBytesThisTime := fetchMaxBytes
			if randomBytes {
				fetchBytesThisTime = rand.Int31n(fetchMaxBytes) + 1
			}

			actuallyMsgs, err, _ := node.ConsumeMessages(fetchOffset, fetchBytesThisTime)
			if err != nil {
				t.Fatal("consume message error", err)
			}
			for _, actuallyMsg := range actuallyMsgs {
				if expectMessages[fetchOffset] != string(actuallyMsg.Data) {
					t.Fatalf("data wrong, in offset: %d, expect data %v, actually data %v",
						fetchOffset, expectMessages[fetchOffset], string(actuallyMsg.Data))
				}
				if actuallyMsg.Offset != fetchOffset {
					t.Fatalf("the offset inside data was wrong, expect offset: %d, actually offset %d",
						fetchOffset, actuallyMsg.Offset)
				}
				fetchOffset++
			}

			if fetchOffset == int64(msgLen) {
				break
			}
		}
	}
}
