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
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/vipshop/vdl/consensus/raft"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/netutil"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/stablestore"
	"golang.org/x/net/context"
)

var (
	ErrIDRemoved     = errors.New("membership: ID removed")
	ErrIDExists      = errors.New("membership: ID exists")
	ErrIDNotFound    = errors.New("membership: ID not found")
	ErrPeerURLexists = errors.New("membership: peerURL exists")
)

// 0 for VDL 1.0.0 currently
type MembershipVersion uint32

type Membership struct {
	MembershipMeta

	MembershipMembers

	// raft group name
	RaftGroupName string `json:"-"`

	//vdl server name
	ServerName string `json:"-"`

	store *MembershipStore

	// guards the fields below
	sync.Mutex
}

type MembershipMeta struct {

	// cluster ID
	// which generate by membersID which from initial-cluster when create new Raft Group
	// This ClusterID use to check whether the same cluster for cross-cluster-interaction
	ClusterID types.ID `json:"clusterID"`

	//version for membership
	Version MembershipVersion `json:"version"`

	//self memberid
	SelfMemberID types.ID `json:"name,omitempty"`
}

type MembershipMembers struct {
	MembersMap map[types.ID]*Member `json:"members"`

	// removed contains the ids of removed members in the cluster.
	// removed id cannot be reused.
	RemovedMap map[types.ID]bool `json:"removed"`
}

type Member struct {

	// Member ID, which generate by urls, raft group name and time(if exists)
	ID types.ID `json:"id"`

	// VDL server name
	Name string `json:"name,omitempty"`

	// peer urls
	PeerURLs []string `json:"peerURLs"`
}

func (c *Membership) Print() {
	glog.Infof("Membership Info -")
	glog.Infof(" |- ClusterID : %v", c.ClusterID)
	glog.Infof(" |- Version : %v", c.Version)
	glog.Infof(" |- SelfMemberID : %v", c.SelfMemberID)
	glog.Infof(" |- RaftGroupName : %v", c.RaftGroupName)
	glog.Infof(" |- ServerName : %v", c.ServerName)
	glog.Infof(" |- MembersMap --")
	c.Lock()
	defer c.Unlock()
	for k, v := range c.MembersMap {
		glog.Infof(" |- |- ID %s, ID in Member %s, Name: %s, PeerURLs: %v", k, v.ID, v.Name, v.PeerURLs)
	}
	glog.Infof(" |- RemovedMap --")
	for k, v := range c.RemovedMap {
		glog.Infof(" |- |- ID %s, bool %v", k, v)
	}

}

// use for start a new Raft Group
// it will use InitialPeerURLs on configuration
// NOTICE: it just load the InitialPeerURLs and put into memory struct, but didn't persistent
func NewMembershipFromInitialPeerURLsMap(initialPeerURLsMap types.URLsMap, raftGroupName string,
	vdlServerName string, stableStore stablestore.StableStore) (*Membership, error) {

	c := newMembership(raftGroupName, vdlServerName, stableStore)

	var foundSelfMemberID = false
	for serverName, urls := range initialPeerURLsMap {
		m := NewMember(serverName, urls, raftGroupName, nil)
		if _, ok := c.MembersMap[m.ID]; ok {
			return nil, fmt.Errorf("member exists with identical ID %v", m)
		}
		if uint64(m.ID) == raft.None {
			return nil, fmt.Errorf("cannot use %x as member id", raft.None)
		}
		c.MembersMap[m.ID] = m
		if serverName == vdlServerName {
			c.SelfMemberID = m.ID
			foundSelfMemberID = true
		}
	}
	if !foundSelfMemberID {
		glog.Infof("NewMembershipFromInitialPeerURLsMap, vdlServerName :%s", vdlServerName)
		c.Print()
		glog.Fatalf("NewMembershipFromInitialPeerURLsMap should found self member")
	}

	c.ClusterID = c.generateClusterID()
	return c, nil
}

func NewMembershipFromStore(raftGroupName string, stableStore stablestore.StableStore,
	vdlServerName string) *Membership {

	membershipStore := NewMembershipStore(stableStore)
	metaFromStore := membershipStore.GetMeta(raftGroupName)
	membersFromStore := membershipStore.GetMembers(raftGroupName)

	membership := &Membership{
		MembershipMeta:    *metaFromStore,
		MembershipMembers: *membersFromStore,
		ServerName:        vdlServerName,
		store:             membershipStore,
		RaftGroupName:     raftGroupName,
	}

	var foundSelfMemberID = false
	for _, m := range membership.MembersMap {
		if m.Name == vdlServerName {
			membership.SelfMemberID = m.ID
			foundSelfMemberID = true
		}
	}
	if !foundSelfMemberID {
		glog.Infof("NewMembershipFromStore from store, vdlServerName :%s", vdlServerName)
		membership.Print()
		glog.Warningf("NewMembershipFromStore should found self member")
		glog.Fatalf("cannot find my node in cluster information, Please check whether is removed from cluster")
	}

	return membership
}

func (c *Membership) MemberIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for _, m := range c.MembersMap {
		ids = append(ids, m.ID)
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}

func (c *Membership) RemovedIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for key := range c.RemovedMap {
		ids = append(ids, key)
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}

func (c *Membership) GetSelfMemberID() types.ID {
	return c.SelfMemberID
}

// MemberByName returns a Member with the given name if exists.
// If more than one member has the given name, it will panic.
func (c *Membership) MemberByName(name string) *Member {
	c.Lock()
	defer c.Unlock()
	var memb *Member
	for _, m := range c.MembersMap {
		if m.Name == name {
			if memb != nil {
				glog.Fatalf("two members with the given name %q exist", name)
			}
			memb = m
		}
	}
	return memb.Clone()
}

func (c *Membership) Member(id types.ID) *Member {
	c.Lock()
	defer c.Unlock()
	return c.MembersMap[id].Clone()
}

func (c *Membership) Members() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.MembersMap {
		ms = append(ms, m.Clone())
	}
	sort.Sort(ms)
	return []*Member(ms)
}

func (c *Membership) MemberCount() int {
	c.Lock()
	defer c.Unlock()
	return len(c.MembersMap)
}

func (c *Membership) IsIDRemoved(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	return c.RemovedMap[id]
}

func (c *Membership) IsIDInMembership(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	_, ok := c.MembersMap[id]
	return ok
}

// AddMember adds a new Member into the cluster, and saves the given member's
// raftAttributes into the store. The given member should have empty attributes.
// A Member with a matching id must not exist.
func (c *Membership) AddMemberAndPersistence(member *Member) {
	c.Lock()
	defer c.Unlock()

	c.MembersMap[member.ID] = member
	c.store.AddMember(member, c.RaftGroupName)

	glog.Infof("added member %s %v to cluster %s[%s]", member.ID, member.PeerURLs, c.RaftGroupName, c.ClusterID)
}

// RemoveMember removes a member from the store.
// The given id MUST exist, or the function panics.
func (c *Membership) RemoveMemberAndPersistence(id types.ID) {
	c.Lock()
	defer c.Unlock()

	delete(c.MembersMap, id)
	c.RemovedMap[id] = true
	c.store.DeleteMember(id, c.RaftGroupName)

	glog.Infof("removed member %s from cluster %s[%s]", id, c.RaftGroupName, c.ClusterID)
}

func (c *Membership) UpdateMemberAndPersistence(member *Member) {
	c.Lock()
	defer c.Unlock()

	c.MembersMap[member.ID] = member
	c.store.UpdateMember(member, c.RaftGroupName)

	//plog.Noticef("updated member %s %v in cluster %s[%s]", member.ID, member.PeerURLs, c.RaftGroupName, c.ClusterID)
	glog.Infof("updated member %s %v in cluster %s[%s]", member.ID, member.PeerURLs, c.RaftGroupName, c.ClusterID)
}

func (c *Membership) PersistenceMeta() {
	c.Lock()
	defer c.Unlock()

	c.store.SaveMeta(&c.MembershipMeta, c.RaftGroupName)
}

// check whether ID exists in store(after apply)
func (c *Membership) IsMemberIDExistsInStore(ID uint64) bool {
	members, _ := membersFromStore(c)
	id := types.ID(ID)
	if members[id] != nil {
		return true
	} else {
		return false
	}
}

func (c *Membership) GetMemberFromStore(ID uint64) *Member {
	members, _ := membersFromStore(c)
	id := types.ID(ID)
	return members[id]
}

// ValidateConfigurationChange takes a proposed ConfChange and
// ensures that it is still valid.
func (c *Membership) ValidateConfigurationChange(cc raftpb.ConfChange) error {
	members, removed := membersFromStore(c)
	id := types.ID(cc.NodeID)
	if removed[id] {
		return ErrIDRemoved
	}
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if members[id] != nil {
			return ErrIDExists
		}
		urls := make(map[string]bool)
		for _, m := range members {
			for _, u := range m.PeerURLs {
				urls[u] = true
			}
		}
		m := new(Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			glog.Fatalf("unmarshal member should never fail: %v", err)
		}
		for _, u := range m.PeerURLs {
			if urls[u] {
				return ErrPeerURLexists
			}
		}
	case raftpb.ConfChangeRemoveNode:
		if members[id] == nil {
			return ErrIDNotFound
		}
	case raftpb.ConfChangeUpdateNode:
		if members[id] == nil {
			return ErrIDNotFound
		}
		urls := make(map[string]bool)
		for _, m := range members {
			if m.ID == id {
				continue
			}
			for _, u := range m.PeerURLs {
				urls[u] = true
			}
		}
		m := new(Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			glog.Fatalf("unmarshal member should never fail: %v", err)
		}
		for _, u := range m.PeerURLs {
			if urls[u] {
				return ErrPeerURLexists
			}
		}
	default:
		glog.Fatalf("ConfChange type should be either AddNode, RemoveNode or UpdateNode")
	}
	return nil
}

func (c *Membership) generateClusterID() types.ID {
	mIDs := c.MemberIDs()
	b := make([]byte, 8*len(mIDs))
	for i, id := range mIDs {
		binary.BigEndian.PutUint64(b[8*i:], uint64(id))
	}
	hash := sha1.Sum(b)
	return types.ID(binary.BigEndian.Uint64(hash[:8]))
}

// ValidateClusterAndAssignIDs validates the local cluster by matching the PeerURLs
// with the existing cluster. If the validation succeeds, it assigns the IDs
// from the existing cluster to the local cluster.
// If the validation fails, an error will be returned.
func ValidateClusterAndAssignIDs(local *Membership, existing *Membership) error {
	ems := existing.Members()
	lms := local.Members()
	if len(ems) != len(lms) {
		return fmt.Errorf("member count is unequal")
	}
	sort.Sort(MembersByPeerURLs(ems))
	sort.Sort(MembersByPeerURLs(lms))

	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	var foundSelfMemberID = false
	for i := range ems {
		if !netutil.URLStringsEqual(ctx, ems[i].PeerURLs, lms[i].PeerURLs) {
			return fmt.Errorf("unmatched member while checking PeerURLs")
		}
		lms[i].ID = ems[i].ID
		if lms[i].Name == local.ServerName {
			local.SelfMemberID = lms[i].ID
			foundSelfMemberID = true
		}
	}
	if !foundSelfMemberID {
		glog.Infof("ValidateClusterAndAssignIDs, vdlServerName :%s", local.ServerName)
		existing.Print()
		glog.Warningf("ValidateClusterAndAssignIDs should found self member")
		glog.Fatalf("Cannot find member by my server name %s from cluster, "+
			"Please add member to cluster first and then start the new node", local.ServerName)
	}

	local.MembersMap = make(map[types.ID]*Member)
	for _, m := range lms {
		local.MembersMap[m.ID] = m
	}
	return nil
}

func membersFromStore(membership *Membership) (map[types.ID]*Member, map[types.ID]bool) {

	membersFromStore := membership.store.GetMembers(membership.RaftGroupName)
	if membersFromStore != nil {
		return membersFromStore.MembersMap, membersFromStore.RemovedMap
	} else {
		members := make(map[types.ID]*Member)
		removed := make(map[types.ID]bool)
		return members, removed
	}
}

// NewMember creates a Member without an ID and generates one based on the
// raft group name, peer URLs and time(if not nil). This is used for bootstrapping/adding new member.
func NewMember(serverName string, peerURLs types.URLs, groupName string, now *time.Time) *Member {
	m := &Member{
		PeerURLs: peerURLs.StringSlice(),
		Name:     serverName,
	}

	var b []byte
	sort.Strings(m.PeerURLs)
	for _, p := range m.PeerURLs {
		b = append(b, []byte(p)...)
	}

	b = append(b, []byte(groupName)...)
	if now != nil {
		b = append(b, []byte(fmt.Sprintf("%d", now.Unix()))...)
	}

	hash := sha1.Sum(b)
	m.ID = types.ID(binary.BigEndian.Uint64(hash[:8]))
	return m
}

func newMembership(raftGroupName string, serverName string, stableStore stablestore.StableStore) *Membership {

	meta := MembershipMeta{}
	members := MembershipMembers{
		MembersMap: make(map[types.ID]*Member),
		RemovedMap: make(map[types.ID]bool),
	}

	return &Membership{
		MembershipMeta:    meta,
		MembershipMembers: members,
		store:             NewMembershipStore(stableStore),
		RaftGroupName:     raftGroupName,
		ServerName:        serverName,
	}
}

func (m *Member) Clone() *Member {
	if m == nil {
		return nil
	}
	mm := &Member{
		ID:   m.ID,
		Name: m.Name,
	}
	if m.PeerURLs != nil {
		mm.PeerURLs = make([]string, len(m.PeerURLs))
		copy(mm.PeerURLs, m.PeerURLs)
	}
	return mm
}

// MembersByID implements sort by ID interface
type MembersByID []*Member

func (ms MembersByID) Len() int           { return len(ms) }
func (ms MembersByID) Less(i, j int) bool { return ms[i].ID < ms[j].ID }
func (ms MembersByID) Swap(i, j int)      { ms[i], ms[j] = ms[j], ms[i] }

// MembersByPeerURLs implements sort by peer urls interface
type MembersByPeerURLs []*Member

func (ms MembersByPeerURLs) Len() int { return len(ms) }
func (ms MembersByPeerURLs) Less(i, j int) bool {
	return ms[i].PeerURLs[0] < ms[j].PeerURLs[0]
}
func (ms MembersByPeerURLs) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
