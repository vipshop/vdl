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

package apicommon

// use to find the vdl server info from the raft group peer
type VDLServerInfo struct {
	RaftNodeID uint64 `json:"RaftNodeID"`

	VDLServerID int32  `json:"VDLServerID"`
	Host        string `json:"Host"`
	Port        int32  `json:"Port"`
}
