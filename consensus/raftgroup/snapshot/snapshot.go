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

package snapshot

import (
	"io"

	"github.com/coreos/etcd/pkg/ioutil"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/pkg/types"
)

//0 for VDL 1.0 currently
type SnapshotVersion int

// SnapshotMetadata for the raft group snapshot
// include all raft relative information
type SnapshotMetadata struct {

	// version for snapshot, 0 was use currently
	Version SnapshotVersion

	RaftGroupName string

	// Index and Term store when the snapshot was taken.
	// NOTE: Index should be last apply index when take snapshot
	Index uint64
	Term  uint64

	// latest members information , include own server
	// map[server name : string] URLs, URLs is the server listener address(es)
	MembersMap types.URLsMap

	//log files for index log and raft log
	LogFiles []LogFile
}

type LogFile struct {

	//the segment raft log name
	FileName string

	//the segment raft log size, use for NOT finish segment log (last writing segment)
	//when recover, need create a file with FileSize, and write the entries to this file
	FileSize uint64

	//raft log file : 0
	//index file : 1
	Type int

	//whether the segments is finish, should only have one writing file at a time
	// true: finish
	// false: writing
	IsFinish bool

	// checksum for finish file
	// for writing file, should check entry one by one
	Checksum string
}

/////////////////////////////
// temp for usage
type Snapshotter struct {
	dir string
}

func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	return nil
}

func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	return nil, nil
}

func (s *Snapshotter) SaveDBFrom(r io.Reader, id uint64) (int64, error) {
	return 0, nil
}

type Message struct {
	raftpb.Message
	ReadCloser io.ReadCloser
	TotalSize  int64
	closeC     chan bool
}

func NewMessage(rs raftpb.Message, rc io.ReadCloser, rcSize int64) *Message {
	return &Message{
		Message:    rs,
		ReadCloser: ioutil.NewExactReadCloser(rc, rcSize),
		TotalSize:  int64(rs.Size()) + rcSize,
		closeC:     make(chan bool, 1),
	}
}

// CloseNotify returns a channel that receives a single value
// when the message sent is finished. true indicates the sent
// is successful.
func (m Message) CloseNotify() <-chan bool {
	return m.closeC
}

func (m Message) CloseWithError(err error) {
	if cerr := m.ReadCloser.Close(); cerr != nil {
		err = cerr
	}
	if err == nil {
		m.closeC <- true
	} else {
		m.closeC <- false
	}
}
