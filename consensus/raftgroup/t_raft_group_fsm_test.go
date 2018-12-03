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


// cases in this file will not start raft node
// it will mock data to test the process
// membership changes will test in raft group test which need start raft group

package raftgroup

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/consensus/raftstore"
	"github.com/vipshop/vdl/pkg/wait"
)

// case 1
// Apply an empty entry, should not enter any apply process
// using nil raft group to check whether enter apply process
func TestApply_ApplyEmptyEntry(t *testing.T) {

	notifyCh := make(chan struct{}, 1)
	applyNil := &apply{
		notifyc: notifyCh,
	}
	applyAll(applyNil, nil)

	select {
	case <-notifyCh:
	default:
		t.Fatal("should not apply empty entry")
	}

	applyEmpty := &apply{
		entries: make([]raftpb.Entry, 0),
		notifyc: notifyCh,
	}
	applyAll(applyEmpty, nil)

	select {
	case <-notifyCh:
	default:
		t.Fatal("should not apply empty entry")
	}

}

// case 2, apply single normal entry without conf changes
func TestApply_ApplyNormalEntry(t *testing.T) {
	testDoApplyEntries(t, 1)
}

// case 3, apply multi normal entry without conf changes
func TestApply_ApplyNormalEntries(t *testing.T) {
	testDoApplyEntries(t, 5000)
}

// case 4, apply normal entry without conf changes,
// test need apply entries that had apply before
func TestApply_ReApplyNormalEntry(t *testing.T) {

	raftGroup := testCreateRaftGroupUsingApplyTest(t)
	raftGroup.applyIndex = 2
	entryCount := 3
	entries := testCreateMockNormalEntries(entryCount, 1, 2)

	waitChan := raftGroup.messageArriveChan

	for _, entry := range entries {
		raftGroup.wait.Register(entry.Index)
	}

	notifyCh := make(chan struct{}, 1)
	apply := &apply{
		notifyc: notifyCh,
		entries: entries,
	}
	applyAll(apply, raftGroup)

	// check
	if raftGroup.applyIndex != uint64(entryCount) {
		t.Fatalf("apply index wrong, expect: %d, but actually: %d", entryCount, raftGroup.applyIndex)
	}

	if raftGroup.lastSaveApplyIndex != 0 {
		t.Fatalf("lastSaveApplyIndex index wrong, expect: %d, but actually: %d", 0, raftGroup.lastSaveApplyIndex)
	}

	// entry 1,2 should not be trigger because it's apply before, but 3 need trigger
	if !raftGroup.wait.IsRegistered(1) {
		t.Fatalf("entry[%d] should not be trigger with wait", 1)
	}
	if !raftGroup.wait.IsRegistered(2) {
		t.Fatalf("entry[%d] should not be trigger with wait", 2)
	}
	if raftGroup.wait.IsRegistered(3) {
		t.Fatalf("entry[%d] should be trigger with wait", 3)
	}

	// check TriggerMessageArriveChan
	timeout := time.After(1 * time.Second)
	select {
	case <-timeout:
		t.Fatalf("should trigger isTriggerMessageArriveChan")
	case <-waitChan:
	}
}

// case 5 : the begin apply index not next of raft.applyIndex
// TODO. because change to glog, glog use os.exit, cannot recover()
func TestApplyUnstable_ApplyNormalEntryError(t *testing.T) {

	defer func() {
		r := recover()
		t.Log(r)
		if r == nil {
			t.Fatal("should panic")
		}
	}()

	raftGroup := testCreateRaftGroupUsingApplyTest(t)
	raftGroup.applyIndex = 2
	entryCount := 3
	entries := testCreateMockNormalEntries(entryCount, 5, 2)

	notifyCh := make(chan struct{}, 1)
	apply := &apply{
		notifyc: notifyCh,
		entries: entries,
	}
	applyAll(apply, raftGroup)

}

// case 6 : test trigger save ApplyIndex by counting
func TestApply_SaveApplyIndexByCount(t *testing.T) {

	raftGroup := testCreateRaftGroupUsingApplyTest(t)

	// apply saveApplyIndexInterval - 1, should not save storage
	entries := testCreateMockNormalEntries(saveApplyIndexCount-1, 1, 2)
	notifyCh := make(chan struct{}, 1)
	apply0 := &apply{
		notifyc: notifyCh,
		entries: entries,
	}
	applyAll(apply0, raftGroup)

	if raftGroup.lastSaveApplyIndex != 0 {
		t.Fatalf("lastSaveApplyIndex index wrong, expect: %d, but actually: %d", 0, raftGroup.lastSaveApplyIndex)
	}

	// apply 1, should trigger save storage
	entries1 := testCreateMockNormalEntries(1, saveApplyIndexCount, 2)
	notifyCh1 := make(chan struct{}, 1)
	apply1 := &apply{
		notifyc: notifyCh1,
		entries: entries1,
	}
	applyAll(apply1, raftGroup)

	if raftGroup.lastSaveApplyIndex != uint64(saveApplyIndexCount) {
		t.Fatalf("lastSaveApplyIndex index wrong, expect: %d, but actually: %d", saveApplyIndexCount, raftGroup.lastSaveApplyIndex)
	}

	storageApplyIndex := raftGroup.raftGroupStableStore.MustGetApplyIndex(raftGroup.GroupConfig.GroupName)
	if storageApplyIndex != uint64(saveApplyIndexCount) {
		t.Fatalf("lastSaveApplyIndex index wrong, expect: %d, but actually: %d", saveApplyIndexCount, storageApplyIndex)
	}

	// apply 100 again , should not save storage
	entries2 := testCreateMockNormalEntries(100, saveApplyIndexCount+1, 2)
	notifyCh2 := make(chan struct{}, 1)
	apply2 := &apply{
		notifyc: notifyCh2,
		entries: entries2,
	}
	applyAll(apply2, raftGroup)

	if raftGroup.lastSaveApplyIndex != uint64(saveApplyIndexCount) {
		t.Fatalf("lastSaveApplyIndex index wrong, expect: %d, but actually: %d", saveApplyIndexCount, raftGroup.lastSaveApplyIndex)
	}

	storageApplyIndex = raftGroup.raftGroupStableStore.MustGetApplyIndex(raftGroup.GroupConfig.GroupName)
	if storageApplyIndex != uint64(saveApplyIndexCount) {
		t.Fatalf("lastSaveApplyIndex index wrong, expect: %d, but actually: %d", saveApplyIndexCount, storageApplyIndex)
	}

}

// case 7 : test trigger save ApplyIndex by timer
func TestApply_SaveApplyIndexByTime(t *testing.T) {

	raftGroup := testCreateRaftGroupUsingApplyTest(t)
	raftGroup.lastSaveApplyTime = time.Now()

	entries := testCreateMockNormalEntries(100, 1, 2)
	notifyCh := make(chan struct{}, 1)
	apply0 := &apply{
		notifyc: notifyCh,
		entries: entries,
	}
	applyAll(apply0, raftGroup)

	if raftGroup.lastSaveApplyIndex != 0 {
		t.Fatalf("lastSaveApplyIndex index wrong, expect: %d, but actually: %d", 0, raftGroup.lastSaveApplyIndex)
	}

	time.Sleep(saveApplyIndexDuration + time.Second)
	entries1 := testCreateMockNormalEntries(1, 101, 2)
	notifyCh1 := make(chan struct{}, 1)
	apply1 := &apply{
		notifyc: notifyCh1,
		entries: entries1,
	}
	applyAll(apply1, raftGroup)

	if raftGroup.lastSaveApplyIndex != 101 {
		t.Fatalf("lastSaveApplyIndex index wrong, expect: %d, but actually: %d", 101, raftGroup.lastSaveApplyIndex)
	}

}

func testCreateMockNormalEntries(count int, beginIndex uint64, term uint64) []raftpb.Entry {

	entries := make([]raftpb.Entry, 0)
	for i := 0; i < count; i++ {
		body := testRandomStringBytes(1024)
		b := make([]byte, len(body)+8)
		binary.BigEndian.PutUint64(b, beginIndex)
		copy(b[8:], body)
		entry := raftpb.Entry{
			Term:  term,
			Index: beginIndex,
			Type:  raftpb.EntryNormal,
			Data:  b,
		}
		entries = append(entries, entry)
		beginIndex++
	}
	return entries
}

func testCreateRaftGroupUsingApplyTest(t *testing.T) *RaftGroup {

	raftGroupConf := testCreateRaftGroupConfig(t, 1)
	raftGroup := testCreateRaftGroup(raftGroupConf)
	raftGroup.applyIndex = 0
	raftGroup.messageArriveChan = make(chan struct{})
	raftGroup.storage = raftstore.NewInmemStore()
	raftGroup.wait = wait.New()
	raftGroup.raftGroupStableStore = NewRaftGroupStableStore(raftGroup.basicStableStore)
	raftGroup.lastSaveApplyTime = time.Now()

	return raftGroup
}

func testDoApplyEntries(t *testing.T, entryCount int) {

	raftGroup := testCreateRaftGroupUsingApplyTest(t)
	beginIndex := 1

	// test 3 times
	for i := 0; i < 3; i++ {
		waitChan := raftGroup.messageArriveChan

		entries := testCreateMockNormalEntries(entryCount, uint64(beginIndex), 2)
		for _, entry := range entries {
			raftGroup.wait.Register(entry.Index)
		}

		notifyCh := make(chan struct{}, 1)
		applya := &apply{
			notifyc: notifyCh,
			entries: entries,
		}
		applyAll(applya, raftGroup)

		// check begin
		expectApplyIndex := beginIndex + entryCount - 1
		if raftGroup.applyIndex != uint64(expectApplyIndex) {
			t.Fatalf("apply index wrong, expect: %d, but actually: %d", expectApplyIndex, raftGroup.applyIndex)
		}

		if raftGroup.lastSaveApplyIndex != 0 {
			t.Fatalf("lastSaveApplyIndex index wrong, expect: %d, but actually: %d", 0, raftGroup.lastSaveApplyIndex)
		}

		// check wait notify
		for _, entry := range entries {
			if raftGroup.wait.IsRegistered(entry.Index) {
				t.Fatalf("entry[%d] should be trigger with wait", entry.Index)
			}
		}

		// sleep to wait TriggerMessageArriveChan
		// check TriggerMessageArriveChan
		timeout := time.After(1 * time.Second)
		select {
		case <-timeout:
			t.Fatalf("should trigger isTriggerMessageArriveChan")
		case <-waitChan:
		}
		beginIndex = beginIndex + entryCount
	}
}
