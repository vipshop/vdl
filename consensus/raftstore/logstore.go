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
	"github.com/vipshop/vdl/consensus/raft"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/logstream"
)

// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion.
type LogStore interface {
	raft.Storage

	// StoreLogs stores multiple log entries.
	StoreEntries(entries []raftpb.Entry) error

	// Delete log entries from the start index to the end.
	DeleteRaftLog(rindex uint64) error

	// Delete segment file by name.
	DeleteFiles(segmentNames []string) error

	// return whether is new store base on the dir
	IsNewStore() bool

	//get vdl index from raft index
	GetVindexByRindex(rindex uint64) (int64, error)

	// close store
	Close() error

	// use to fetch the max offset index (kafka max offset currently)
	// the return value should max log stream offset but <= maxOffset(param)
	MaxVindex(maxRindex uint64) (int64, error)

	// use to fetch the min offset index(kafka min offset currently)
	MinVindex() (int64, error)

	// fetch log stream messages, from startVindex, to raft log index is endRindex
	// maxBytes means max bytes fetch.
	// when there have entry from startVindex, return at least one entry event if the one entry large than maxBytes .
	// bool return whether read from cache
	FetchLogStreamMessages(startVindex int64, endRindex uint64, maxBytes int32) ([]logstream.Entry, error, bool)

	//create the metadata of segment and index file before applyIndex
	CreateSnapshotMeta(applyIndex uint64) ([]*logstream.SegmentFile, error)
}
