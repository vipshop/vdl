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

package raftgroup

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vipshop/vdl/pkg/netutil"
	"github.com/vipshop/vdl/pkg/types"
	"golang.org/x/net/context"
)

const (
	defaultMaxSizePerMsg = 1 * 1024 * 1024 // 1M

	// NOTE: it's the max size once raft send append entries
	// Assuming the RTT is around 1ms, 512k max size is large enough. ( 512 * 1000 = 512M )
	defaultMaxSizePerAppendEntries = 512 * 1024

	// the inflight number for append entries request
	// NOTE: MaxInflightMsgs * MaxDataSizeSendOnce <= log store segment max size / 2
	defaultMaxInflightAppendEntriesRequest = 256

	//the default size of logstore memory cache
	defaultMemCacheSizeByte = 512 * 1024 * 1024

	//the default segment count of logstore must reserve
	defaultReserveSegmentCount = 4
)

type GroupConfig struct {

	// raft listener configuration
	ListenerUrls types.URLs

	// raft group name
	GroupName string

	// vdl server name, one vdl server have only one server name
	VDLServerName string

	// use int32 match to kafka protocol
	VDLServerID int32

	// Client listener Host
	VDLClientListenerHost string

	// Client listener Port
	VDLClientListenerPort int32

	// raft group initial peers
	// be map[vdl server name] -> []Urls
	InitialPeerURLsMap types.URLsMap

	//log persistent data dir
	DataDir string

	// Log store segment size, use default when =0
	LogStoreSegmentSize int64

	// Log store LRU size, use default when =0
	//LogStoreLRUSize int64

	// HeartbeatTick is 1, ElectionTicks is N times than HeartbeatTick
	ElectionTicks int

	// Millisecond for raft heartbeat
	HeartbeatMs time.Duration

	// true: initial-cluster-state=new , this use initial-cluster to init cluster
	// false: initial-cluster-state=existing, this use to add new member to a exists cluster
	IsInitialCluster bool

	// When join to a exists cluster, use this urls to fetch the exists cluster information
	ExistsClusterAdminUrls types.URLs

	// the max message size for StoreMessage method
	MaxSizePerMsg int

	//the logstore Memcache size
	MemCacheSizeByte uint64

	//the logstore must reserve segment counts
	ReserveSegmentCount int

	// strict check for member changes
	StrictMemberChangesCheck bool

	// max size once raft send append entries (per request)
	MaxSizePerAppendEntries uint64

	// the inflight number for append entries request
	MaxInflightAppendEntriesRequest int

	// If true, Raft runs an additional election phase
	// to check whether it would get enough votes to win
	// an election, thus minimizing disruptions.
	PreVote bool
}

// basic validate for configuration
func (conf *GroupConfig) Validate() error {

	if conf.MaxSizePerMsg == 0 {
		conf.MaxSizePerMsg = defaultMaxSizePerMsg
	}

	if conf.MaxSizePerAppendEntries == 0 {
		conf.MaxSizePerAppendEntries = defaultMaxSizePerAppendEntries
	}

	if conf.MaxInflightAppendEntriesRequest == 0 {
		conf.MaxInflightAppendEntriesRequest = defaultMaxInflightAppendEntriesRequest
	}

	if conf.MemCacheSizeByte == 0 {
		conf.MemCacheSizeByte = defaultMemCacheSizeByte
	}

	if conf.ReserveSegmentCount == 0 {
		conf.ReserveSegmentCount = defaultReserveSegmentCount
	}

	//TODO. log store/peer store/stable store/check

	return nil
}

// ValidateNewRaftGroup sanity-checks the initial config for #NEW RAFT GROUP# case
// and returns an error for things that should never happen.
func (conf *GroupConfig) ValidateNewRaftGroup() error {

	if conf.InitialPeerURLsMap.String() == "" {
		return fmt.Errorf("initial cluster unset")
	}

	if err := conf.hasLocalMember(); err != nil {
		return err
	}
	if err := conf.advertiseMatchesCluster(); err != nil {
		return err
	}
	if isDuplicate := conf.isDuplicateInitialPeerURLs(); isDuplicate {
		return fmt.Errorf("initial cluster %s has duplicate url", conf.InitialPeerURLsMap)
	}
	return nil

}

// hasLocalMember checks that the cluster at least contains the local server.
func (conf *GroupConfig) hasLocalMember() error {
	if urls := conf.InitialPeerURLsMap[conf.VDLServerName]; urls == nil {
		return fmt.Errorf("couldn't find local name %q in the initial cluster configuration", conf.VDLServerName)
	}
	return nil
}

// advertiseMatchesCluster confirms peer URLs match those in the cluster peer list.
func (conf *GroupConfig) advertiseMatchesCluster() error {
	urls, apurls := conf.InitialPeerURLsMap[conf.VDLServerName], conf.ListenerUrls.StringSlice()
	urls.Sort()
	sort.Strings(apurls)
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	if !netutil.URLStringsEqual(ctx, apurls, urls.StringSlice()) {
		umap := map[string]types.URLs{conf.VDLServerName: conf.ListenerUrls}
		return fmt.Errorf("--initial-cluster must include %s given --listen-peer-url=%s", types.URLsMap(umap).String(), strings.Join(apurls, ","))
	}
	return nil
}

func (conf *GroupConfig) isDuplicateInitialPeerURLs() bool {
	um := make(map[string]bool)
	for _, urls := range conf.InitialPeerURLsMap {
		for _, url := range urls {
			u := url.String()
			if um[u] {
				return true
			}
			um[u] = true
		}
	}
	return false
}

func (conf *GroupConfig) peerDialTimeout() time.Duration {
	// 1s for queue wait and system delay
	// + one RTT, which is smaller than 1/5 election timeout
	return time.Second + time.Duration(conf.ElectionTicks)*time.Duration(conf.HeartbeatMs)*time.Millisecond/5
}
