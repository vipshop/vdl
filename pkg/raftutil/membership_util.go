// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raftutil

import (
	"github.com/vipshop/vdl/consensus/raftgroup/membership"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/server/adminapi/apipb"
)

// raft members struct to pb members
func MembersToProtoMembers(members []*membership.Member) []*apipb.Member {
	protoMembs := make([]*apipb.Member, len(members))
	for i := range members {
		protoMembs[i] = &apipb.Member{
			Name:     members[i].Name,
			ID:       uint64(members[i].ID),
			PeerURLs: members[i].PeerURLs,
		}
	}
	return protoMembs
}

// pb members to raft members struct
func ProtoMembersToMembers(protoMembs []*apipb.Member) []*membership.Member {
	members := make([]*membership.Member, len(protoMembs))
	for i := range protoMembs {
		members[i] = &membership.Member{
			Name:     protoMembs[i].Name,
			ID:       types.ID(protoMembs[i].ID),
			PeerURLs: protoMembs[i].PeerURLs,
		}
	}
	return members
}

// pb members to raft members struct
func ProtoMembersToMemberMap(protoMembs []*apipb.Member) map[types.ID]*membership.Member {
	memberMap := make(map[types.ID]*membership.Member)
	for i := range protoMembs {
		memberMap[types.ID(protoMembs[i].ID)] = &membership.Member{
			Name:     protoMembs[i].Name,
			ID:       types.ID(protoMembs[i].ID),
			PeerURLs: protoMembs[i].PeerURLs,
		}
	}
	return memberMap
}

func RemovedIDsToUint64Slice(removeIDs []types.ID) []uint64 {

	if removeIDs == nil || len(removeIDs) == 0 {
		return nil
	}

	ids := make([]uint64, len(removeIDs))
	for i, m := range removeIDs {
		ids[i] = uint64(m)
	}
	return ids
}

// pb removed member to raft removed struct
func ProtoRemovedMembersToRemovedMap(ids []uint64) map[types.ID]bool {
	removedMap := make(map[types.ID]bool)
	for value := range ids {
		removedMap[types.ID(value)] = true
	}
	return removedMap
}
