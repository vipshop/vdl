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

package raftstore

import (
	"errors"
	"fmt"
	"sync"

	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/logstream"
)

/**
InmemStore use to mock the storage, using memory. DO NOT use to production
*/

var (
	// ErrLogNotFound indicates a given log entry is not available.
	ErrLogNotFound = errors.New("log not found")
)

// InmemStore implements the LogStore and StableStore interface.
// It should NOT EVER be used for production. It is used only for
// unit tests. Use the MDBStore implementation instead.
type InmemStore struct {
	l         sync.RWMutex
	lowIndex  uint64
	highIndex uint64
	logs      map[uint64]raftpb.Entry
	kv        map[string][]byte
	kvInt     map[string]uint64
	hardState raftpb.HardState
	snapshot  raftpb.Snapshot
}

// NewInmemStore returns a new in-memory backend. Do not ever
// use for production. Only for testing.
func NewInmemStore() *InmemStore {
	i := &InmemStore{
		logs:      make(map[uint64]raftpb.Entry),
		kv:        make(map[string][]byte),
		kvInt:     make(map[string]uint64),
		lowIndex:  0,
		highIndex: 0,
		hardState: raftpb.HardState{},
		snapshot:  raftpb.Snapshot{},
	}
	return i
}

// FirstIndex implements the LogStore interface.
func (i *InmemStore) FirstIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.lowIndex, nil
}

// LastIndex implements the LogStore interface.
func (i *InmemStore) LastIndex() (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.highIndex, nil
}

// GetLog implements the LogStore interface.
func (i *InmemStore) GetEntry(index uint64) (*raftpb.Entry, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	l, ok := i.logs[index]
	if !ok {
		return nil, ErrLogNotFound
	}
	return &l, nil
}

// StoreLog implements the LogStore interface.
//func (i *InmemStore) StoreEntry(entry *raftpb.Entry) error {
//	return i.StoreEntries([]raftpb.Entry{*entry})
//}

// StoreLogs implements the LogStore interface.
func (i *InmemStore) StoreEntries(entries []raftpb.Entry) error {
	i.l.Lock()
	defer i.l.Unlock()
	for _, l := range entries {
		i.logs[l.Index] = l
		if i.lowIndex == 0 {
			i.lowIndex = l.Index
		}
		if l.Index > i.highIndex {
			i.highIndex = l.Index
		}
		fmt.Println("save entries,term:idx:", l.Term, l.Index, " len:", len(entries))
	}
	return nil
}

// DeleteRange implements the LogStore interface.
func (i *InmemStore) DeleteToEnd(start int64) error {
	i.l.Lock()
	defer i.l.Unlock()
	highIndex := i.highIndex
	for j := uint64(start); j <= highIndex; j++ {
		delete(i.logs, j)
	}
	i.highIndex = uint64(start)
	return nil
}

func (i *InmemStore) DeleteFiles(segmentNames []string) error {
	return nil
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (i *InmemStore) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {

	i.l.Lock()
	defer i.l.Unlock()

	maxSize = min(maxSize, i.highIndex-lo+1)
	hi = min(hi, lo+maxSize)
	hi = min(hi, i.highIndex+1)

	entries := make([]raftpb.Entry, hi-lo)

	var index uint64 = 0
	for j := lo; j < hi; j++ {
		entries[index] = i.logs[j]
		index++
	}
	return entries[:index], nil
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (i *InmemStore) Term(idx uint64) (uint64, error) {

	i.l.Lock()
	defer i.l.Unlock()

	return i.logs[idx].Term, nil
}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
func (i *InmemStore) Snapshot() (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, nil
}

func (i *InmemStore) IsNewStore() bool {
	return true
}

// Set implements the StableStore interface.
func (i *InmemStore) Set(key []byte, val []byte) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kv[string(key)] = val
	return nil
}

// Get implements the StableStore interface.
func (i *InmemStore) Get(key []byte) ([]byte, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.kv[string(key)], nil
}

// SetUint64 implements the StableStore interface.
func (i *InmemStore) SetUint64(key []byte, val uint64) error {
	i.l.Lock()
	defer i.l.Unlock()
	i.kvInt[string(key)] = val
	return nil
}

// GetUint64 implements the StableStore interface.
func (i *InmemStore) GetUint64(key []byte) (uint64, error) {
	i.l.RLock()
	defer i.l.RUnlock()
	return i.kvInt[string(key)], nil
}

func (i *InmemStore) DeleteRaftLog(rindex uint64) error {
	i.l.RLock()
	defer i.l.RUnlock()

	lastIndex := i.highIndex
	for j := rindex; j <= lastIndex; j++ {
		delete(i.logs, j)
	}
	lastIndex = rindex - 1
	return nil
}

func (i *InmemStore) Close() error {
	i.l.Lock()
	defer i.l.Unlock()

	i.logs = make(map[uint64]raftpb.Entry)
	i.kv = make(map[string][]byte)
	i.kvInt = make(map[string]uint64)
	i.lowIndex = 0
	i.highIndex = 0
	i.hardState = raftpb.HardState{}
	i.snapshot = raftpb.Snapshot{}

	return nil
}

// use to fetch the max offset index (kafka max offset currently)
// the return value should max log stream offset but <= maxOffset(param)
func (i *InmemStore) MaxVindex(maxRindex uint64) (int64, error) {
	return int64(maxRindex), nil
}

// use to fetch the min offset index(kafka min offset currently)
func (i *InmemStore) MinVindex() (int64, error) {
	return 0, nil
}

// fetch log stream messages, from startVindex, to raft log index is endRindex
// maxBytes means max bytes fetch.
// when there have entry from startVindex, return at least one entry event if the one entry large than maxBytes .
func (i *InmemStore) FetchLogStreamMessages(startVindex int64, endRindex uint64, maxBytes int32) ([]logstream.Entry, error, bool) {
	return nil, nil, false
}

//get vdl index from raft index
func (i *InmemStore) GetVindexByRindex(rindex uint64) (int64, error) {
	return int64(rindex), nil
}

func (i *InmemStore) CreateSnapshotMeta(applyIndex uint64) ([]*logstream.SegmentFile, error) {
	return nil, nil
}
