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

// raft group test cases util

package raftgroup

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/vipshop/vdl/pkg/types"
	"gitlab.tools.vipshop.com/distributedstorage/fiu"
)

const testLetterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var (
	isJenkinsTest            *bool   = flag.Bool("jenkinsTest", false, "if in jenkins,please specify to true")
	jenkinsWorkspace         *string = flag.String("jenkinsWorkspace", "", "the jenkins workspace")
	testRaftGroupOneFolder   string
	testRaftGroupTwoFolder   string
	testRaftGroupThreeFolder string
	testRaftGroupFourFolder  string
)

func init() {

	fiu.CreateNonFIUEngine(nil)

	flag.Parse()

	// if not jenkins test, create just one folder to test
	if !*isJenkinsTest {
		var err error
		testRaftGroupOneFolder, err = ioutil.TempDir("", "vdl1")
		if err != nil {
			panic(err)
		}

		testRaftGroupTwoFolder, err = ioutil.TempDir("", "vdl2")
		if err != nil {
			panic(err)
		}

		testRaftGroupThreeFolder, err = ioutil.TempDir("", "vdl3")
		if err != nil {
			panic(err)
		}

		testRaftGroupFourFolder, err = ioutil.TempDir("", "vdl4")
		if err != nil {
			panic(err)
		}
	}

}

type TestMemStableStore struct {
	kvMap map[string]string
	l     sync.RWMutex
}

func testNewTestMemStableStore() *TestMemStableStore {
	return &TestMemStableStore{
		kvMap: make(map[string]string),
	}
}

func (s *TestMemStableStore) Put(key []byte, value []byte) error {
	s.l.Lock()
	defer s.l.Unlock()
	s.kvMap[string(key)] = string(value)
	return nil
}

func (s *TestMemStableStore) Get(key []byte) ([]byte, error) {
	s.l.RLock()
	defer s.l.RUnlock()
	return []byte(s.kvMap[string(key)]), nil
}

func (s *TestMemStableStore) GetBatch(keys []string) (map[string][]byte, error) {
	return nil, nil
}

func (s *TestMemStableStore) Close() error {
	s.l.Lock()
	defer s.l.Unlock()
	s.kvMap = nil
	return nil
}

// create size = n random string
func testRandStringBytes(n int) string {
	if n <= 0 {
		n = 1
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = testLetterBytes[rand.Intn(len(testLetterBytes))]
	}
	return string(b)
}

// create [1-n) random string
func testRandomStringBytes(n int) string {
	return testRandStringBytes(rand.Intn(n) + 1)
}

func testCreateRaftGroupConfig(t *testing.T, idx int) *GroupConfig {

	InitialPeerURLsMap, _ := types.NewURLsMap("vdl1=http://127.0.0.1:13001,vdl2=http://127.0.0.1:13002,vdl3=http://127.0.0.1:13003")
	return testCreateRaftGroupConfigWithURLsMap(t, idx, InitialPeerURLsMap)
}

func testCreateRaftGroupConfigWithURLsMap(t *testing.T, idx int, urlsMap types.URLsMap) *GroupConfig {

	listenerUrls, err := types.NewURLs([]string{"http://127.0.0.1:1300" + strconv.Itoa(idx)})
	if err != nil {
		panic(err)
	}

	dataFolder := testRaftGroupOneFolder
	if idx == 2 {
		dataFolder = testRaftGroupTwoFolder
	} else if idx == 3 {
		dataFolder = testRaftGroupThreeFolder
	}

	conf := &GroupConfig{
		ListenerUrls:             listenerUrls,
		GroupName:                "testGroup",
		VDLServerName:            "vdl" + strconv.Itoa(idx),
		VDLServerID:              int32(idx),
		VDLClientListenerHost:    "127.0.0.1",
		VDLClientListenerPort:    int32(14000 + idx),
		InitialPeerURLsMap:       urlsMap,
		DataDir:                  dataFolder,
		ElectionTicks:            20,
		HeartbeatMs:              200 * time.Millisecond,
		IsInitialCluster:         true,
		StrictMemberChangesCheck: true,
	}

	return conf
}

func testCreateRaftGroup(conf *GroupConfig) *RaftGroup {

	raftgroup, err := NewRaftGroup(conf, testNewTestMemStableStore())
	if err != nil {
		panic(err)
	}

	return raftgroup
}

func testInitAllDataFolder(caseName string) {

	fmt.Printf("start cases %s ........\n", caseName)

	// if jenkins Test, should keep data when need to check or replay, just create new dir to new test cases
	if *isJenkinsTest {
		var err error

		testRaftGroupOneFolder = path.Join(*jenkinsWorkspace, "data", "vdl1", caseName)
		err = os.MkdirAll(testRaftGroupOneFolder, 0700)
		if err != nil {
			panic(err)
		}

		testRaftGroupTwoFolder = path.Join(*jenkinsWorkspace, "data", "vdl2", caseName)
		err = os.MkdirAll(testRaftGroupTwoFolder, 0700)
		if err != nil {
			panic(err)
		}

		testRaftGroupThreeFolder = path.Join(*jenkinsWorkspace, "data", "vdl3", caseName)
		err = os.MkdirAll(testRaftGroupThreeFolder, 0700)
		if err != nil {
			panic(err)
		}

		testRaftGroupFourFolder = path.Join(*jenkinsWorkspace, "data", "vdl4", caseName)
		err = os.MkdirAll(testRaftGroupFourFolder, 0700)
		if err != nil {
			panic(err)
		}

	} else {
		os.RemoveAll(testRaftGroupOneFolder)
		os.RemoveAll(testRaftGroupTwoFolder)
		os.RemoveAll(testRaftGroupThreeFolder)
		os.RemoveAll(testRaftGroupFourFolder)
	}
}

func testGetLeaderNode(t *testing.T, groupNodes []*RaftGroup) *RaftGroup {

	// should elect a leader within 20 seconds
	for i := 0; i < 2000; i++ {
		for _, group := range groupNodes {
			if group.IsLeader() {
				return group
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader election")
	return nil
}

func testDestroyRaftGroups(groupNodes []*RaftGroup) {
	testCloseRaftGroups(groupNodes)
	fmt.Printf("finish cases ........\n")
}

func testCloseRaftGroups(groupNodes []*RaftGroup) {
	for _, group := range groupNodes {
		if group.runState == raftGroupStarted {
			err := group.Stop()
			if err != nil {
				panic(err)
			}
		}
	}
}
