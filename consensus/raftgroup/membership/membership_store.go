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
	"encoding/json"
	"sync"

	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/stablestore"
)

const (
	MembersKeyPrefix = "membership_membersKeyPrefix"
	MetaKeyPrefix    = "membership_metaKeyPrefix"
)

type MembershipStore struct {
	stableStore stablestore.StableStore

	// guards the db
	sync.Mutex
}

func NewMembershipStore(stableStore stablestore.StableStore) *MembershipStore {

	return &MembershipStore{
		stableStore: stableStore,
	}
}

func (s *MembershipStore) AddMember(member *Member, groupName string) {

	s.Lock()
	defer s.Unlock()

	members := s.getOrCreateMembers(groupName, true)
	members.MembersMap[member.ID] = member

	s.mustSaveMembers(members, groupName)
}

func (s *MembershipStore) DeleteMember(id types.ID, groupName string) {

	s.Lock()
	defer s.Unlock()

	members := s.getOrCreateMembers(groupName, true)
	delete(members.MembersMap, id)
	members.RemovedMap[id] = true

	s.mustSaveMembers(members, groupName)
}

func (s *MembershipStore) UpdateMember(member *Member, groupName string) {

	s.Lock()
	defer s.Unlock()

	members := s.getOrCreateMembers(groupName, true)
	members.MembersMap[member.ID] = member

	s.mustSaveMembers(members, groupName)
}

// when no members in store, return nil
func (s *MembershipStore) GetMembers(groupName string) *MembershipMembers {

	s.Lock()
	defer s.Unlock()

	return s.getOrCreateMembers(groupName, false)
}

func (s *MembershipStore) SaveMeta(meta *MembershipMeta, groupName string) {

	s.Lock()
	defer s.Unlock()

	s.mustSaveMeta(meta, groupName)
}

// when no member meta in store, return nil
func (s *MembershipStore) GetMeta(groupName string) *MembershipMeta {

	s.Lock()
	defer s.Unlock()

	return s.getOrCreateMeta(groupName, false)
}

// this method should not call outside of this file
// need protect by lock
func (s *MembershipStore) getOrCreateMembers(groupName string, create bool) *MembershipMembers {

	value, err := s.stableStore.Get(generateMembersKey(groupName))
	if err != nil {
		glog.Fatalf("get members from store should never fail: %v", err)
	}
	var members *MembershipMembers = nil
	if value == nil || len(value) == 0 {
		if create {
			members = &MembershipMembers{
				MembersMap: make(map[types.ID]*Member),
				RemovedMap: make(map[types.ID]bool),
			}
		}
	} else {
		members = &MembershipMembers{}
		if err = json.Unmarshal(value, members); err != nil {
			glog.Fatalf("Unmarshal Members from store should never fail: %v", err)
		}

		if members.MembersMap == nil {
			members.MembersMap = make(map[types.ID]*Member)
		}
		if members.RemovedMap == nil {
			members.RemovedMap = make(map[types.ID]bool)
		}
	}
	return members
}

// this method should not call outside of this file
// need protect by lock
func (s *MembershipStore) mustSaveMembers(members *MembershipMembers, groupName string) {

	b, err := json.Marshal(members)
	if err != nil {
		glog.Fatalf("marshal Members should never fail: %v", err)
	}
	err = s.stableStore.Put(generateMembersKey(groupName), b)
	if err != nil {
		glog.Fatalf("store Members should never fail: %v", err)
	}
}

// this method should not call outside of this file
// need protect by lock
func (s *MembershipStore) getOrCreateMeta(groupName string, create bool) *MembershipMeta {

	value, err := s.stableStore.Get(generateMetaKey(groupName))
	if err != nil {
		glog.Fatalf("get MembershipMeta from store should never fail: %v", err)
	}
	var meta *MembershipMeta = nil
	if value == nil || len(value) == 0 {
		if create {
			meta = &MembershipMeta{}
		}
	} else {
		meta = &MembershipMeta{}
		if err = json.Unmarshal(value, meta); err != nil {
			glog.Fatalf("Unmarshal MembershipMeta from store should never fail: %v", err)
		}
	}
	return meta
}

// this method should not call outside of this file
// need protect by lock
func (s *MembershipStore) mustSaveMeta(meta *MembershipMeta, groupName string) {

	b, err := json.Marshal(meta)
	if err != nil {
		glog.Fatalf("marshal MembershipMeta should never fail: %v", err)
	}
	err = s.stableStore.Put(generateMetaKey(groupName), b)
	if err != nil {
		glog.Fatalf("store MembershipMeta should never fail: %v", err)
	}
}

func generateMembersKey(groupName string) []byte {
	key := MembersKeyPrefix + groupName
	return []byte(key)
}

func generateMetaKey(groupName string) []byte {
	key := MetaKeyPrefix + groupName
	return []byte(key)
}
