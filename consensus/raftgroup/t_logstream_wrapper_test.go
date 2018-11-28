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

// cases in this file will start 3 raft node
// cases refer to testcases.md

package raftgroup

import (
	"github.com/vipshop/vdl/consensus/raftgroup/api/apicommon"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// case 1
func TestLogStreamIT_TestGetVDLServerInfo(t *testing.T) {

	testInitAllDataFolder("TestLogStreamIT_TestGetVDLServerInfo")
	raftGroupNodes := testCreateThreeNodes(t, 1*1024*1024)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()
	for _, node := range raftGroupNodes {
		if node.vdlServerInfoCache != nil {
			t.Fatalf("vdlServerInfoCache should be nil when start, node:%v", node.GroupConfig.VDLServerName)
		}
	}

	vdlInfos, err := raftGroupNodes[0].GetVDLServerInfo()
	if err != nil {
		panic(err)
	}

	// check result
	t.Logf("get vdl server info from node[0] and check")
	testCheckFullVDLInfos(t, vdlInfos)
	t.Logf("check success")

	// check cache
	t.Logf("check node[1-3] cache")
	if &raftGroupNodes[0].vdlServerInfoCache[0] != &vdlInfos[0] {
		t.Fatalf("raftGroupNodes[0] haven't cache the result")
	}
	if raftGroupNodes[1].vdlServerInfoCache != nil {
		t.Fatalf("raftGroupNodes[1] vdlServerInfoCache should be nil when no fetch")
	}
	if raftGroupNodes[2].vdlServerInfoCache != nil {
		t.Fatalf("raftGroupNodes[2] vdlServerInfoCache should be nil when no fetch")
	}
	t.Logf("check success")

	// stop and get again
	t.Logf("stop node[1]")
	raftGroupNodes[1].Stop()

	t.Logf("fetch node[0] again, it should get from cache, do the check")
	vdlInfosAgain, err := raftGroupNodes[0].GetVDLServerInfo()
	// check result
	testCheckFullVDLInfos(t, vdlInfosAgain)
	t.Logf("check success")

	// get from node2
	t.Logf("fetch node[2]")
	node2VDLInfos, err := raftGroupNodes[2].GetVDLServerInfo()
	if len(node2VDLInfos) != 2 {
		t.Fatalf("GetVDLServerInfo from node 2 should get 2 VDL servers, but actually get: %d", len(node2VDLInfos))
	}

	for _, vdl := range node2VDLInfos {
		if vdl.VDLServerID == 1 {
			if vdl.Host != "127.0.0.1" || vdl.Port != 14001 {
				t.Fatalf("vdl server info wrong, the vdl.Host shoudl be 127.0.0.1 "+
					"and port should be 14001, but actually %v", vdl)
			}
		} else if vdl.VDLServerID == 3 {
			if vdl.Host != "127.0.0.1" || vdl.Port != 14003 {
				t.Fatalf("vdl server info wrong, the vdl.Host shoudl be 127.0.0.1 "+
					"and port should be 14003, but actually %v", vdl)
			}
		} else {
			t.Fatalf("vdl server info wrong, the VDLServerID shoudl be [1-3], but actually %d", vdl.VDLServerID)
		}
	}
	t.Logf("check success")

	// wait node 1 cache expire and fetch again
	t.Logf("expire node[0] cache, fetch again")
	raftGroupNodes[0].lastVDLServerInfoUpdateTime = raftGroupNodes[0].lastVDLServerInfoUpdateTime.Add(time.Nanosecond - vdlServerInfoCacheInterval)

	node1Again, err := raftGroupNodes[0].GetVDLServerInfo()
	if len(node1Again) != 2 {
		t.Fatalf("GetVDLServerInfo from node[0] should get 2 VDL servers, but actually get: %d", len(node1Again))
	}

	for _, vdl := range node1Again {
		if vdl.VDLServerID == 1 {
			if vdl.Host != "127.0.0.1" || vdl.Port != 14001 {
				t.Fatalf("vdl server info wrong, the vdl.Host shoudl be 127.0.0.1 "+
					"and port should be 14001, but actually %v", vdl)
			}
		} else if vdl.VDLServerID == 3 {
			if vdl.Host != "127.0.0.1" || vdl.Port != 14003 {
				t.Fatalf("vdl server info wrong, the vdl.Host shoudl be 127.0.0.1 "+
					"and port should be 14003, but actually %v", vdl)
			}
		} else {
			t.Fatalf("vdl server info wrong, the VDLServerID shoudl be [1-3], but actually %d", vdl.VDLServerID)
		}
	}
	t.Logf("check success")

}

// case 2
func TestLogStreamIT_TestGetVDLServerInfoConcurrent(t *testing.T) {

	testInitAllDataFolder("TestLogStreamIT_TestGetVDLServerInfoConcurrent")

	orginalInterval := vdlServerInfoCacheInterval
	vdlServerInfoCacheInterval = 1 * time.Millisecond

	raftGroupNodes := testCreateThreeNodes(t, 1*1024*1024)
	defer func() {
		testDestroyRaftGroups(raftGroupNodes)
	}()

	waitgroup := new(sync.WaitGroup)
	for i := 0; i < 100; i++ {
		waitgroup.Add(1)
		go func() {
			for i := 0; i < 50000; i++ {
				if i%200 == 0 {
					time.Sleep(10 * time.Nanosecond)
				}
				infos, err := raftGroupNodes[rand.Intn(3)].GetVDLServerInfo()
				if err != nil {
					panic(err)
				}
				testCheckFullVDLInfos(t, infos)
			}
			waitgroup.Done()
		}()
	}
	waitgroup.Wait()
	vdlServerInfoCacheInterval = orginalInterval
}

func testCheckFullVDLInfos(t *testing.T, vdlInfos []*apicommon.VDLServerInfo) {
	if len(vdlInfos) != 3 {
		t.Fatalf("GetVDLServerInfo should return 3 VDL servers, but actually get: %d", len(vdlInfos))
	}

	for _, vdl := range vdlInfos {
		if vdl.VDLServerID == 1 {
			if vdl.Host != "127.0.0.1" || vdl.Port != 14001 {
				t.Fatalf("vdl server info wrong, the vdl.Host shoudl be 127.0.0.1 "+
					"and port should be 14001, but actually %v", vdl)
			}
		} else if vdl.VDLServerID == 2 {
			if vdl.Host != "127.0.0.1" || vdl.Port != 14002 {
				t.Fatalf("vdl server info wrong, the vdl.Host shoudl be 127.0.0.1 "+
					"and port should be 14002, but actually %v", vdl)
			}
		} else if vdl.VDLServerID == 3 {
			if vdl.Host != "127.0.0.1" || vdl.Port != 14003 {
				t.Fatalf("vdl server info wrong, the vdl.Host shoudl be 127.0.0.1 "+
					"and port should be 14003, but actually %v", vdl)
			}
		} else {
			t.Fatalf("vdl server info wrong, the VDLServerID shoudl be [1-3], but actually %d", vdl.VDLServerID)
		}
	}
}
