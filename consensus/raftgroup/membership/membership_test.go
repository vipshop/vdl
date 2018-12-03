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

package membership

import (
	"reflect"
	"testing"

	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/stablestore"
)

var (
	InitialPeerURLsMapSingle = "vdl0=http://127.0.0.1:2380,vdl1=http://127.0.0.1:2381,vdl2=http://127.0.0.1:2382"

	InitialPeerURLsMapMulti = "vdl0=http://127.0.0.1:2380,vdl0=http://127.0.0.1:2390," +
		"vdl1=http://127.0.0.1:2381," +
		"vdl2=http://127.0.0.1:2382,vdl2=http://127.0.0.1:2392,vdl2=http://127.0.0.1:2402"
)

func getExpectMembersSingle() map[string][]string {

	mapMembers := make(map[string][]string)
	mapMembers["vdl0"] = []string{"http://127.0.0.1:2380"}
	mapMembers["vdl1"] = []string{"http://127.0.0.1:2381"}
	mapMembers["vdl2"] = []string{"http://127.0.0.1:2382"}

	return mapMembers
}

func getExpectMembersMulti() map[string][]string {

	mapMembers := make(map[string][]string)
	mapMembers["vdl0"] = []string{"http://127.0.0.1:2380", "http://127.0.0.1:2390"}
	mapMembers["vdl1"] = []string{"http://127.0.0.1:2381"}
	mapMembers["vdl2"] = []string{"http://127.0.0.1:2382", "http://127.0.0.1:2392", "http://127.0.0.1:2402"}
	return mapMembers
}

// Case: Test InitialPeerURLsMap, url in map is single url
func TestNewMembershipFromInitialPeerURLsMap_Single(t *testing.T) {

	initialURLsMap, _ := types.NewURLsMap(InitialPeerURLsMapSingle)
	expectMembers := getExpectMembersSingle()
	doTestNewMembershipFromInitialPeerURLsMap(initialURLsMap, "raftgroup_multi", expectMembers, t)

}

// Case: Test InitialPeerURLsMap, url in map is multi url
func TestNewMembershipFromInitialPeerURLsMap_Multi(t *testing.T) {

	initialURLsMap, _ := types.NewURLsMap(InitialPeerURLsMapMulti)
	expectMembers := getExpectMembersMulti()
	doTestNewMembershipFromInitialPeerURLsMap(initialURLsMap, "raftgroup_multi", expectMembers, t)

}

// Case: Test add member and Persistence, use for test persistence data should be right
func TestAddMemberAndPersistence(t *testing.T) {

	raftGroupName := "TestAddMemberGroup"
	memStore := stablestore.NewInmemStableStore()

	initialURLsMap, _ := types.NewURLsMap(InitialPeerURLsMapMulti)
	membership, _ := NewMembershipFromInitialPeerURLsMap(initialURLsMap, raftGroupName, "vdl0", memStore)

	for _, member := range membership.MembersMap {
		membership.AddMemberAndPersistence(member)
	}

	persistentMembership := membership.store.GetMembers(raftGroupName)

	if len(persistentMembership.MembersMap) != 3 {
		t.Fatalf("persistentMembership MembersMap's len should be 3")
	}

	for _, persistentMember := range persistentMembership.MembersMap {
		if !reflect.DeepEqual(membership.MembersMap[persistentMember.ID].PeerURLs, persistentMember.PeerURLs) {
			t.Fatalf("TestAddMember : member %s have diff urls, persistent %s , mem %s",
				persistentMember.Name,
				persistentMember.PeerURLs,
				membership.MembersMap[persistentMember.ID].PeerURLs)
		}
	}

	for _, memMember := range membership.MembersMap {
		if !reflect.DeepEqual(persistentMembership.MembersMap[memMember.ID].PeerURLs, memMember.PeerURLs) {
			t.Fatalf("TestAddMember : member %s have diff urls, persistent %s , mem %s",
				persistentMembership.MembersMap[memMember.ID].Name,
				persistentMembership.MembersMap[memMember.ID].PeerURLs,
				memMember.PeerURLs)
		}
	}
}

// Case: Test remove member and Persistence, use for test persistence data should be right
func TestRemoveMemberAndPersistence(t *testing.T) {
	raftGroupName := "TestAddMemberGroup"
	memStore := stablestore.NewInmemStableStore()

	initialURLsMap, _ := types.NewURLsMap(InitialPeerURLsMapMulti)
	membership, _ := NewMembershipFromInitialPeerURLsMap(initialURLsMap, raftGroupName, "vdl0", memStore)

	var removeMemberID types.ID
	for _, member := range membership.MembersMap {
		membership.AddMemberAndPersistence(member)
		removeMemberID = member.ID
	}

	membership.RemoveMemberAndPersistence(removeMemberID)

	persistentMembership := membership.store.GetMembers(raftGroupName)

	if len(persistentMembership.MembersMap) != 2 {
		t.Fatalf("persistentMembership MembersMap's len should be 2")
	}

	if persistentMembership.MembersMap[removeMemberID] != nil {
		t.Fatalf("removeMemberID %d should be remove in MembersMap, current map is %v",
			removeMemberID, persistentMembership.MembersMap)
	}
}

// Case: Test update member and Persistence, use for test persistence data should be right
func TestUpdateMemberAndPersistence(t *testing.T) {

	raftGroupName := "TestAddMemberGroup"
	memStore := stablestore.NewInmemStableStore()

	initialURLsMap, _ := types.NewURLsMap(InitialPeerURLsMapMulti)
	membership, _ := NewMembershipFromInitialPeerURLsMap(initialURLsMap, raftGroupName, "vdl0", memStore)

	var updateMember *Member
	for _, member := range membership.MembersMap {
		membership.AddMemberAndPersistence(member)
		updateMember = member
	}

	updateMember.PeerURLs = []string{"http://127.0.0.1:9999"}
	membership.UpdateMemberAndPersistence(updateMember)

	persistentMembership := membership.store.GetMembers(raftGroupName)
	if len(persistentMembership.MembersMap) != 3 {
		t.Fatalf("persistentMembership MembersMap's len should be 3")
	}

	if !reflect.DeepEqual(persistentMembership.MembersMap[updateMember.ID].PeerURLs, updateMember.PeerURLs) {
		t.Fatalf("persistentMembership should update url, but currently not")
	}
}

func doTestNewMembershipFromInitialPeerURLsMap(initialPeerURLsMap types.URLsMap, raftGroupName string,
	expectMembers map[string][]string, t *testing.T) {

	memStore := stablestore.NewInmemStableStore()

	membership, err := NewMembershipFromInitialPeerURLsMap(initialPeerURLsMap, raftGroupName, "vdl0", memStore)
	if err != nil {
		t.Fatalf("failed to NewMembershipFromInitialPeerURLsMap", err)
	}

	for _, v := range membership.MembersMap {

		if expectMembers[v.Name] == nil {
			t.Fatalf("can not found %s in ExpectMembers", v.Name)
		}

		if !reflect.DeepEqual(expectMembers[v.Name], v.PeerURLs) {
			t.Fatalf("member %s have diff urls, expect %s , but actual %s", v.Name,
				expectMembers[v.Name], v.PeerURLs)
		}
	}
}
