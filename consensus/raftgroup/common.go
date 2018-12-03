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

//var plog = logutil.NewMergeLogger(capnslog.NewPackageLogger(conf.LOG_ROOT, "raft_group"))

type RaftGroupRunState int

const (
	//raftGroup state
	raftGroupStarting RaftGroupRunState = 0
	raftGroupStarted  RaftGroupRunState = 1
	raftGroupStopping RaftGroupRunState = 2
	raftGroupStopped  RaftGroupRunState = 3
)

var runStates = []string{
	"raftGroupStarting",
	"raftGroupStarted",
	"raftGroupStopping",
	"raftGroupStopped",
}

func (t RaftGroupRunState) String() string {
	return runStates[int(t)]
}

func IsStringEqualAny(a, b []string) bool {
	for _, aString := range a {
		for _, bString := range b {
			if aString == bString {
				return true
			}
		}
	}
	return false
}

//func init() {
//	plog.SetLevel(capnslog.INFO)
//}
