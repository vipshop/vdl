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

package logstore

import (
	"testing"

	"bytes"

	"encoding/binary"

	"path"

	"os"

	"io"

	"math"

	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"gitlab.tools.vipshop.com/distributedstorage/fiu"
)

func init() {
	fiu.CreateNonFIUEngine(nil)
}

//only for test
func newLogStore() (*LogStore, error) {
	var err error
	cfg := &LogStoreConfig{
		Dir: TmpVdlDir,
		Meta: &FileMeta{
			VdlVersion: "1.0",
		},
		SegmentSizeBytes:    segmentSize,
		MaxSizePerMsg:       1024 * 1024,
		MemCacheSizeByte:    1024 * 1024 * 512,
		ReserveSegmentCount: 5,
	}
	s, err := NewLogStore(cfg)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func newLogStoreWithVersion(version string) (*LogStore, error) {
	var err error
	cfg := &LogStoreConfig{
		Dir: TmpVdlDir,
		Meta: &FileMeta{
			VdlVersion: version,
		},
		SegmentSizeBytes:    segmentSize,
		MemCacheSizeByte:    1024 * 1024 * 512,
		ReserveSegmentCount: 10,
		MaxSizePerMsg:       1024 * 1024,
	}
	s, err := NewLogStore(cfg)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func newEntries(start uint64, count int) []raftpb.Entry {
	entries := make([]raftpb.Entry, 0, 10)
	for i := 0; i < count; i++ {
		entry := raftpb.Entry{
			Term:  1,
			Index: start + uint64(i),
			Type:  raftpb.EntryNormal,
			Data:  make([]byte, 8),
		}
		binary.BigEndian.PutUint64(entry.Data, start+uint64(i)) //reqId equal Index
		entry.Data = append(entry.Data, []byte("hello")...)
		entries = append(entries, entry)
	}
	return entries
}

func newConfChangeEntries(start uint64, count int) []raftpb.Entry {
	entries := make([]raftpb.Entry, 0, 10)
	for i := 0; i < count; i++ {
		entry := raftpb.Entry{
			Term:  1,
			Index: start + uint64(i),
			Type:  raftpb.EntryConfChange,
			Data:  make([]byte, 8),
		}
		binary.BigEndian.PutUint64(entry.Data, start+uint64(i)) //reqId equal Index
		entry.Data = append(entry.Data, []byte("hello")...)
		entries = append(entries, entry)
	}
	return entries
}

//1
//new Logstore with open segment
func TestNewLogStoreWithNewSeg(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	//check logstore
	if len(s.Segments) != 1 {
		t.Fatalf("NewLogStore error:len(s.Segments)=%d", len(s.Segments))
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("LogStore.Close error:%s", err.Error())
	}
}

//2
func TestNewLogStoreWithOpenSeg(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	//save vdl log in segment[0],segment[1],segment[2]
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 100)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("logstore Close err=%s", err.Error())
	}
	//new logstore with open segments
	s, err = newLogStore()
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	if s.Segments[2].Status != SegmentRDWR {
		t.Fatalf("s.Segments[2].Status!=SegmentRDWR")
	}
	if s.Mc.FirstRindex != s.Segments[2].GetFirstRindex() || s.Mc.LastRindex != s.Segments[2].GetLastRindex() {
		t.Fatalf("s.Mc.FirstRindex=%d,s.Segments[2].FirstRindex=%d,s.Mc.LastRindex=%d,s.Segments[2].LastRindex=%d",
			s.Mc.FirstRindex, s.Segments[2].GetFirstRindex(), s.Mc.LastRindex, s.Segments[2].GetLastRindex())
	}
	if s.Mc.FirstVindex != s.Segments[2].GetFirstVindex() || s.Mc.LastVindex != s.Segments[2].GetLastVindex() {
		t.Fatalf("s.Mc.FirstRindex=%d,s.Segments[2].FirstRindex=%d,s.Mc.LastRindex=%d,s.Segments[2].LastRindex=%d",
			s.Mc.FirstRindex, s.Segments[2].GetFirstVindex(), s.Mc.LastRindex, s.Segments[2].GetLastVindex())
	}
}

//3
//test NewLogstore with open segment,but no segment file
func TestNewLogStoreWithOpenSegNoMetaFile(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	//save vdl log in segment[0],segment[1],segment[2]
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("logstore Close err=%s", err.Error())
	}
	//Remove segment meta file
	err = os.Remove(path.Join(s.Dir, segmentRangeFile))
	if err != nil {
		t.Fatalf("logstore Close err=%s", err.Error())
	}
	//new logstore with open segments
	s, err = newLogStore()
	if err != nil {
		t.Fatalf("newLogStore error=%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	if s.Segments[2].Status != SegmentRDWR {
		t.Fatalf("s.Segments[2].Status==SegmentRDWR")
	}

	segmentRange, err := s.RangeMetaFile.GetRangeInfo()
	if err != nil {
		t.Fatalf("GetRangeInfo error,err=%s", err.Error())
	}
	//segment0 log range
	logRange0 := segmentRange.NameToRange[s.Segments[0].GetName()]
	if logRange0.FirstRindex != s.Segments[0].GetFirstRindex() || logRange0.LastRindex != s.Segments[0].GetLastRindex() {
		t.Fatalf("logRange0.FirstRindex=%d,s.Segments[0].FirstRindex=%d,logRange0.LastRindex=%d,s.Segments[0].LastRindex=%d",
			logRange0.FirstRindex, s.Segments[0].GetFirstRindex(), logRange0.LastRindex, s.Segments[0].GetLastRindex())
	}
	if logRange0.FirstVindex != s.Segments[0].GetFirstVindex() || logRange0.LastVindex != s.Segments[0].GetLastVindex() {
		t.Fatalf("logRange0.FirstVindex=%d,s.Segments[0].FirstRindex=%d,logRange0.LastVindex=%d,s.Segments[0].LastVindex=%d",
			logRange0.FirstVindex, s.Segments[0].GetFirstVindex(), logRange0.LastVindex, s.Segments[0].GetLastVindex())
	}

	//segment1 log range
	logRange1 := segmentRange.NameToRange[s.Segments[1].GetName()]
	if logRange1.FirstRindex != s.Segments[1].GetFirstRindex() || logRange1.LastRindex != s.Segments[1].GetLastRindex() {
		t.Fatalf("logRange1.FirstRindex=%d,s.Segments[1].FirstRindex=%d,logRange1.LastRindex=%d,s.Segments[1].LastRindex=%d",
			logRange1.FirstRindex, s.Segments[1].GetFirstRindex(), logRange1.LastRindex, s.Segments[1].GetLastRindex())
	}
	if logRange1.FirstVindex != s.Segments[1].GetFirstVindex() || logRange1.LastVindex != s.Segments[1].GetLastVindex() {
		t.Fatalf("logRange1.FirstVindex=%d,s.Segments[1].FirstRindex=%d,logRange1.LastVindex=%d,s.Segments[1].LastVindex=%d",
			logRange1.FirstVindex, s.Segments[1].GetFirstVindex(), logRange1.LastVindex, s.Segments[1].GetLastVindex())
	}

}

//4
func TestNewLogStoreWithMultiVersion(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	//save vdl log in segment[0],segment[1],segment[2]
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("logstore Close err=%s", err.Error())
	}
	s, err = newLogStoreWithVersion("2.0")
	if err != nil {
		t.Fatalf("newLogStore error=%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	if s.Meta.VdlVersion != "2.0" {
		t.Fatalf("s.Meta.VdlVersion=%s,not equal 2.0", s.Meta.VdlVersion)
	}
	if len(s.Segments) != 3 {
		t.Fatalf("len(s.Segments)=%d,not equal 3", len(s.Segments))
	}
	if s.Segments[1].Status != SegmentReadOnly || s.Segments[2].Status != SegmentRDWR {
		t.Fatalf("s.Segments[2].Status=%v,s.Segments[3].Status=%v", s.Segments[2].Status,
			s.Segments[3].Status)
	}

}

//5
//only read memCache
func TestFetchLogStreamMessages1(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 50
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)
	//save vdl log in segment[0],segment[1],segment[2]
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	mcFirstVindex := s.Mc.FirstVindex
	mcLastVindex := s.Mc.LastVindex
	mcLastRindex := s.Mc.LastRindex
	t.Logf("mcFirstVindex=%d,mcLastVindex=%d,mcLastRindex=%d",
		mcFirstVindex, mcLastVindex, mcLastRindex)
	entries, err, _ := s.FetchLogStreamMessages(mcFirstVindex, mcLastRindex, noLimitVdlLog)
	if err != nil {
		t.Fatalf("FetchLogStreamMessages error:%s", err.Error())
	}

	if len(entries) != int(mcLastVindex-mcFirstVindex+1) {
		t.Fatalf("len(entries)=%d, endVindex-start+1=%d", len(entries), mcLastVindex-mcFirstVindex+1)
	}
}

//6
//read all segments data
func TestFetchLogStreamMessages2(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 50
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)
	//save vdl log in segment[0],segment[1],segment[2]
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	firstVindex, err := s.FirstVindex()
	if err != nil {
		t.Fatalf("FirstVindex error:%s", err.Error())
	}
	mcFirstVindex := s.Mc.FirstVindex
	mcLastVindex := s.Mc.LastVindex
	mcLastRindex := s.Mc.LastRindex
	t.Logf("mcFirstVindex=%d,mcLastVindex=%d,mcLastRindex=%d",
		mcFirstVindex, mcLastVindex, mcLastRindex)
	entries, err, _ := s.FetchLogStreamMessages(firstVindex, mcLastRindex, noLimitVdlLog)
	if err != nil {
		t.Fatalf("FetchLogStreamMessages error:%s", err.Error())
	}
	if len(entries) != int(mcLastVindex-firstVindex+1) {
		t.Fatalf("len(entries)=%d, endVindex-start+1=%d", len(entries), mcLastVindex-firstVindex+1)
	}
}

//7
//read multi segments,all in disk
func TestFetchLogStreamMessages3(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 50
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)
	//save vdl log in segment[0],segment[1]
	for len(s.Segments) < 2 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	//save config change log in segment[1],segment[2],segment[3]
	//and segment[2],segment[3] is all config change log
	for len(s.Segments) < 5 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newConfChangeEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	//save vdl log in segment[4],segment[5],segment[6]
	//and segment[5],segment[6] is vdl log
	for len(s.Segments) < 7 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}

	firstVindex, err := s.FirstVindex()
	if err != nil {
		t.Fatalf("FirstVindex error:%s", err.Error())
	}
	mcFirstVindex := s.Mc.FirstVindex
	mcFirstRindex := s.Mc.FirstRindex
	mcLastVindex := s.Mc.LastVindex
	mcLastRindex := s.Mc.LastRindex
	t.Logf("mcFirstVindex=%d,mcLastVindex=%d,mcLastRindex=%d",
		mcFirstVindex, mcLastVindex, mcLastRindex)
	//t.Logf("sig[0].FirstVindex=%d,sig[0].LastVindex=%d,sig[5].FirstVindex=%d,sig[5].LastVindex=%d",
	//	s.Segments[0].GetFirstVindex(), start, s.Segments[5].GetFirstVindex(), endVindex)
	entries, err, _ := s.FetchLogStreamMessages(firstVindex, mcFirstRindex-1, noLimitVdlLog)
	if err != nil {
		t.Fatalf("FetchLogStreamMessages error:%s", err.Error())
	}
	if len(entries) != int(mcFirstVindex-firstVindex) {
		t.Fatalf("len(entries)=%d, mcFirstVindex=%d,firstVindex=%d,"+
			"mcFirstVindex-firstVindex=%d", len(entries), mcFirstVindex, firstVindex,
			mcFirstVindex-firstVindex)
	}
	//消息大小为5，获取大小为1的消息，
	entries, err, _ = s.FetchLogStreamMessages(firstVindex, mcFirstRindex-1, 1)
	if err != nil {
		t.Fatalf("FetchLogStreamMessages error:%s", err.Error())
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries)=%d, mcFirstVindex=%d,firstVindex=%d,"+
			"mcFirstVindex-firstVindex=%d", len(entries), mcFirstVindex, firstVindex,
			mcFirstVindex-firstVindex)
	}
	//消息大小为5，获取消息总大小为11的消息
	entries, err, _ = s.FetchLogStreamMessages(firstVindex, mcFirstRindex-1, 11)
	if err != nil {
		t.Fatalf("FetchLogStreamMessages error:%s", err.Error())
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries)=%d, mcFirstVindex=%d,firstVindex=%d,"+
			"mcFirstVindex-firstVindex=%d", len(entries), mcFirstVindex, firstVindex,
			mcFirstVindex-firstVindex)
	}
}

//8
func TestGetVindexByRindex(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	//save vdl log in segment[0],segment[1],segment[2]
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	lastRindex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex error:%s", err.Error())
	}
	lastVindex, err := s.GetVindexByRindex(lastRindex)
	if err != nil {
		t.Fatalf("GetVindexByRindex error:%s", err.Error())
	}
	if lastVindex+1 != int64(lastRindex) {
		t.Fatalf("lastVindex=%d,lastRindex=%d", lastVindex, lastRindex)
	}
}

func checkEntry(leftEntry, rightEntry raftpb.Entry, t *testing.T) {
	if leftEntry.Type != rightEntry.Type || leftEntry.Term != rightEntry.Term ||
		leftEntry.Index != rightEntry.Index || bytes.Equal(leftEntry.Data, rightEntry.Data) == false {
		t.Fatalf("entry not equal,leftEntry=%v,rightEntry=%v", leftEntry, rightEntry)
	}
}

//9
func TestStoreAndReadEntries(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 50
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)
	lastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	start := lastIndex + 1
	entries := newEntries(start, 10)
	if err != nil {
		t.Fatalf("newEntries error:%s", err.Error())
	}
	err = s.StoreEntries(entries)
	if err != nil {
		t.Fatalf("newEntries error:%s", err.Error())
	}
	newLastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	if newLastIndex != start+9 {
		t.Fatalf("newLastIndex=%d,not equal %d", newLastIndex, start+9)
	}
	readEntries, err := s.Entries(start, start+10, noLimit)
	if err != nil {
		t.Fatalf("s.Entries error:%s", err.Error())
	}
	if len(entries) != len(readEntries) {
		t.Fatalf("len(entries)!=len(readEntries),len(entries)=%d,len(readEntries)=%d", len(entries), len(readEntries))
	}
	for i, entry := range readEntries {
		checkEntry(entry, entries[i], t)
	}
}

//10
//only in Memcache
func TestStoreAndReadEntries1(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 50
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)

	//save vdl log in segment[0],segment[1],segment[2]
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	mcFirstRindex := s.Mc.FirstRindex
	mcLastRindex := s.Mc.LastRindex
	t.Logf("mcFirstRindex=%d,mcLastRindex=%d",
		mcFirstRindex, mcLastRindex)
	readEntries, err := s.Entries(mcFirstRindex, mcLastRindex+1, noLimit)
	if err != nil {
		t.Fatalf("s.Entries error:%s", err.Error())
	}
	if len(readEntries) != int(mcLastRindex-mcFirstRindex+1) {
		t.Fatalf("len(entries)=%d,mcLastRindex-mcFirstRindex+1=%d",
			len(readEntries), mcLastRindex-mcFirstRindex+1)
	}
	if readEntries[0].Index != mcFirstRindex ||
		readEntries[len(readEntries)-1].Index != mcLastRindex {
		t.Fatalf("readEntries[0].Index=%d,mcFirstRindex=%d,"+
			"readEntries[len(readEntries)-1].Index=%d,"+
			"mcLastRindex=%d",
			readEntries[0].Index, mcFirstRindex,
			readEntries[len(readEntries)-1].Index, mcLastRindex)
	}
}

//11
//in disk + MemCache
func TestStoreAndReadEntries2(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 50
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)

	//save vdl log in segment[0],segment[1],segment[2]
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	firstRindex, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex error:%s", err.Error())
	}
	mcFirstRindex := s.Mc.FirstRindex
	mcLastRindex := s.Mc.LastRindex
	t.Logf("mcFirstRindex=%d,mcLastRindex=%d",
		mcFirstRindex, mcLastRindex)
	readEntries, err := s.Entries(firstRindex, mcLastRindex+1, noLimit)
	if err != nil {
		t.Fatalf("s.Entries error:%s", err.Error())
	}
	if len(readEntries) != int(mcLastRindex-firstRindex+1) {
		t.Fatalf("len(entries)=%d,mcLastRindex-mcFirstRindex+1=%d",
			len(readEntries), mcLastRindex-firstRindex+1)
	}
	if readEntries[0].Index != firstRindex ||
		readEntries[len(readEntries)-1].Index != mcLastRindex {
		t.Fatalf("readEntries[0].Index=%d,mcFirstRindex=%d,"+
			"readEntries[len(readEntries)-1].Index=%d,"+
			"mcLastRindex=%d",
			readEntries[0].Index, firstRindex,
			readEntries[len(readEntries)-1].Index, mcLastRindex)
	}
}

//12
//in disk
func TestStoreAndReadEntries3(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 50
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)

	//save vdl log in segment[0],segment[1],segment[2]
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	firstRindex, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex error:%s", err.Error())
	}
	mcFirstRindex := s.Mc.FirstRindex
	t.Logf("firstRindex=%d,mcFirstRindex=%d",
		firstRindex, mcFirstRindex)
	readEntries, err := s.Entries(firstRindex, mcFirstRindex, noLimit)
	if err != nil {
		t.Fatalf("s.Entries error:%s", err.Error())
	}
	if len(readEntries) != int(mcFirstRindex-firstRindex) {
		t.Fatalf("len(entries)=%d,mcLastRindex-mcFirstRindex+1=%d",
			len(readEntries), mcFirstRindex-firstRindex)
	}
	if readEntries[0].Index != firstRindex ||
		readEntries[len(readEntries)-1].Index != mcFirstRindex-1 {
		t.Fatalf("readEntries[0].Index=%d,mcFirstRindex=%d,"+
			"readEntries[len(readEntries)-1].Index=%d,"+
			"mcLastRindex=%d",
			readEntries[0].Index, firstRindex,
			readEntries[len(readEntries)-1].Index, mcFirstRindex-1)
	}
}

//13
func TestTerm(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 50
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)

	//save vdl log in segment[0],segment[1],segment[2]
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	lastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	readEntries, err := s.Entries(lastIndex, lastIndex+1, noLimit)
	if err != nil {
		t.Fatalf("s.Entries error:%s", err.Error())
	}
	term, err := s.Term(lastIndex)
	if err != nil {
		t.Fatalf("s.Term error:%s", err.Error())
	}
	if readEntries[0].Term != term {
		t.Fatalf("readEntries[0].Term=%d", readEntries[0].Term)
	}
}

//14
//with delete segment file
func TestDeleteRaftLog1(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	for len(s.Segments) <= 4 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	//have 5 segments
	start := s.Segments[2].GetFirstRindex()
	end := s.Segments[2].GetLastRindex()
	firstIndex, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex error:%s", err.Error())
	}
	rindex := (start + end) / 2
	err = s.DeleteRaftLog(rindex)
	if err != nil {
		t.Fatalf("DeleteRaftLog error:%s", err.Error())
	}
	//check write only
	if s.Segments[2].Status != SegmentRDWR {
		t.Fatalf("last segment status is not SegmentRDWR")
	}
	newLastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex error:%s", err.Error())
	}
	//check last rindex
	if newLastIndex != rindex-1 {
		t.Fatalf("newLastIndex[%d] is not equal rindex-1[%d]", newLastIndex, rindex-1)
	}
	//check MemCache last rindex
	if 0 != s.Mc.LastRindex {
		t.Fatalf("newLastIndex[%d] is not equal s.Mc.LastRindex[%d]", newLastIndex, 0)
	}
	newFirstIndex, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex error:%s", err.Error())
	}
	if firstIndex != newFirstIndex {
		t.Fatalf("firstIndex[%d] is not equal newFirstIndex[%d]", firstIndex, newFirstIndex)
	}
}

//15
//only delete in memCache
func TestDeleteRaftLog2(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 50
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)
	lastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	start := lastIndex + 1
	entries := newEntries(start, 10)
	if err != nil {
		t.Fatalf("newEntries error:%s", err.Error())
	}
	err = s.StoreEntries(entries)
	if err != nil {
		t.Fatalf("newEntries error:%s", err.Error())
	}
	//newLastIndex=start+9
	newLastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	//delete raft logs
	err = s.DeleteRaftLog(newLastIndex - 5)
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	//afterDeleteRaftIndex=start+3
	afterDeleteRaftIndex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	//
	if afterDeleteRaftIndex != newLastIndex-6 {
		t.Fatalf("newLastIndex=%d,not equal %d", newLastIndex, start+9)
	}
	readEntries, err := s.Entries(start, start+4, noLimit)
	if err != nil {
		t.Fatalf("s.Entries error:%s", err.Error())
	}
	if 4 != len(readEntries) {
		t.Fatalf("len(entries)!=len(readEntries),len(entries)=%d,len(readEntries)=%d", len(entries), len(readEntries))
	}
	for i, entry := range readEntries {
		checkEntry(entry, entries[i], t)
	}
}

//16
func TestMaxVindex(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()

	lastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	start := lastIndex + 1
	entries := newEntries(start, 10)
	if err != nil {
		t.Fatalf("newEntries error:%s", err.Error())
	}
	err = s.StoreEntries(entries)
	if err != nil {
		t.Fatalf("newEntries error:%s", err.Error())
	}
	//newLastIndex=start+9
	newLastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	maxVindex, err := s.MaxVindex(newLastIndex)
	if err != nil {
		t.Fatalf("s.MaxVindex error:%s", err.Error())
	}
	vindex, err := s.GetVindexByRindex(newLastIndex)
	if err != nil {
		t.Fatalf("s.MaxVindex error:%s", err.Error())
	}
	if maxVindex != vindex {
		t.Fatalf("maxVindex[%d] is not equal vindex[%d]", maxVindex, vindex)
	}
	//store raft log
	confChangeEntries := newConfChangeEntries(newLastIndex+1, 5)
	err = s.StoreEntries(confChangeEntries)
	if err != nil {
		t.Fatalf("s.StoreEntries error:%s", err.Error())
	}
	newLastIndex, err = s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	maxVindex, err = s.MaxVindex(newLastIndex)
	if err != nil {
		t.Fatalf("s.MaxVindex error:%s", err.Error())
	}
	if maxVindex != vindex {
		t.Fatalf("maxVindex[%d] is not equal vindex[%d]", maxVindex, vindex)
	}
}

//17
func TestMinVindex(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	firstVindex := s.Segments[0].GetFirstVindex()
	vindex, err := s.MinVindex()
	if err != nil {
		t.Fatalf("MinVindex error:%s", err.Error())
	}
	if firstVindex != vindex {
		t.Fatalf("firstVindex[%d] is not equal vindex[%d]", firstVindex, vindex)
	}
}

//18
func TestSetAndReadVdlLog(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	lastIndex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	start := lastIndex + 1
	entries := newEntries(start, 10)
	if err != nil {
		t.Fatalf("newEntries error:%s", err.Error())
	}

	err = s.StoreEntries(entries)
	if err != nil {
		t.Fatalf("StoreEntries error:%s", err.Error())
	}
	lastIndex, err = s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex error:%s", err.Error())
	}
	firstVindex, err := s.FirstVindex()
	if err != nil {
		t.Fatalf("s.FirstVindex error:%s", err.Error())
	}
	lastVindex, err := s.LastVindex()
	if err != nil {
		t.Fatalf("s.FirstVindex error:%s", err.Error())
	}
	vdlLogCount := int(lastVindex-firstVindex) + 1
	//t.Logf("firstVindex=%d,lastVindex=%d,vdlLogCount=%d",
	//	firstVindex, lastVindex, vdlLogCount)
	//get vdl log
	vdlLogs, err, _ := s.FetchLogStreamMessages(firstVindex, lastIndex, noLimitVdlLog)
	if err != nil {
		t.Fatalf("StoreVdlEntry error:%s", err.Error())
	}
	if len(vdlLogs) != vdlLogCount {
		t.Fatalf("len(vdlLogs) is %d not equal vdlLogCount1=%d",
			len(vdlLogs), vdlLogCount)
	}
	//vdl log is:"hello"
	vdlLogSize := 5
	vdlLogs, err, _ = s.FetchLogStreamMessages(firstVindex, lastIndex, int32(vdlLogSize*5+2))
	if err != nil {
		t.Fatalf("StoreVdlEntry error:%s", err.Error())
	}
	if len(vdlLogs) != 5 {
		t.Fatalf("len(vdlLogs) is %d not equal vdlLogCount1=%d",
			len(vdlLogs), vdlLogCount)
	}
	vdlLogs, err, _ = s.FetchLogStreamMessages(firstVindex, lastIndex, int32(vdlLogSize-1))
	if err != nil {
		t.Fatalf("StoreVdlEntry error:%s", err.Error())
	}
	if len(vdlLogs) != 1 {
		t.Fatalf("len(vdlLogs) is %d not equal vdlLogCount1=%d",
			len(vdlLogs), vdlLogCount)
	}
}

//19
func TestDeleteFiles(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	for len(s.Segments) <= 6 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 100)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}

	segmentNames := make([]string, 0, 10)
	segmentNames = append(segmentNames, path.Base(s.Segments[0].SegmentPath))

	if len(segmentNames) != 1 {
		t.Fatalf("len(segmentNames)=%d", len(segmentNames))
	}
	firstRindex := s.Segments[1].GetFirstRindex()
	lastRindex := s.Segments[len(s.Segments)-1].GetLastRindex()

	err = s.DeleteFiles(segmentNames)
	if err != nil {
		t.Fatalf("DeleteFiles error=%s", err.Error())
	}

	fisrtRindexLogStore, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex error=%s", err.Error())
	}

	lastRindexLogStore, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex error=%s", err.Error())
	}

	if fisrtRindexLogStore != firstRindex || lastRindexLogStore != lastRindex {
		t.Fatalf("s.FirstRindex=%d,firstRindex=%d,s.LastRindex=%d,lastRindex=%d",
			fisrtRindexLogStore, firstRindex, lastRindexLogStore, lastRindex)
	}
}

//20
func TestReadFromSegments(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	for len(s.Segments) < 2 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	startIndex := s.Segments[0].FirstRindex
	endIndex := s.Segments[1].LastRindex + 1
	AllEntries, err := s.Entries(startIndex, endIndex, noLimit)
	if len(AllEntries) != int(s.Segments[1].LastRindex-s.Segments[0].FirstRindex+1) {
		t.Logf("AllEntries[0].Index=%d,startIndex=%d,"+
			"AllEntries[%d].Index=%d,s.Segments[1].LastRindex=%d",
			AllEntries[0].Index, startIndex,
			len(AllEntries)-1, AllEntries[len(AllEntries)-1].Index,
			s.Segments[1].LastRindex)
		t.Fatalf("len(AllEntries)=%d,not equal %d", len(AllEntries), endIndex-startIndex+1)
	}
	if AllEntries[0].Index != startIndex ||
		AllEntries[len(AllEntries)-1].Index != s.Segments[1].LastRindex {
		t.Fatalf("AllEntries[0].Index=%d,startIndex=%d,"+
			"AllEntries[%d].Index=%d,s.Segments[1].LastRindex=%d",
			AllEntries[0].Index, startIndex,
			len(AllEntries)-1, AllEntries[len(AllEntries)-1].Index,
			s.Segments[1].LastRindex)
	}
}

//21
//read records from segments before last segment
func TestReadFromSegments2(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	startIndex := s.Segments[0].FirstRindex
	endIndex := s.Segments[1].LastRindex + 1
	AllEntries, err := s.Entries(startIndex, endIndex, noLimit)
	if len(AllEntries) != int(s.Segments[1].LastRindex-s.Segments[0].FirstRindex+1) {
		t.Logf("AllEntries[0].Index=%d,startIndex=%d,"+
			"AllEntries[%d].Index=%d,s.Segments[1].LastRindex=%d",
			AllEntries[0].Index, startIndex,
			len(AllEntries)-1, AllEntries[len(AllEntries)-1].Index,
			s.Segments[1].LastRindex)
		t.Fatalf("len(AllEntries)=%d,not equal %d", len(AllEntries), endIndex-startIndex+1)
	}
	if AllEntries[0].Index != startIndex ||
		AllEntries[len(AllEntries)-1].Index != s.Segments[1].LastRindex {
		t.Fatalf("AllEntries[0].Index=%d,startIndex=%d,"+
			"AllEntries[%d].Index=%d,s.Segments[1].LastRindex=%d",
			AllEntries[0].Index, startIndex,
			len(AllEntries)-1, AllEntries[len(AllEntries)-1].Index,
			s.Segments[1].LastRindex)
	}
}

//22
//test rebuild read only segment index
func TestRebuildReadSegmentIndex(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}

	//remove segment range meta file, and destroy the 1th segment index file
	rangeMetaPath := path.Join(s.Dir, segmentRangeFile)
	err = os.Remove(rangeMetaPath)
	if err != nil {
		t.Fatalf("Remove file %s,error:%s", rangeMetaPath, err.Error())
	}
	indexFile := s.Segments[0].IndexFile.IndexFile.File
	position, err := indexFile.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("indexFile Seek error:err=%s", err.Error())
	}
	//1th segment的index file 部分写
	err = indexFile.Truncate(position - 30)
	if err != nil {
		t.Fatalf("Truncate error:err=%s", err.Error())
	}
	//关闭logstore
	s.Close()
	//重新打开出错的logstore
	s, err = newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	//检查segment meta文件
	segmentRange, err := s.RangeMetaFile.GetRangeInfo()
	if err != nil {
		t.Fatalf("ParseRangeFile error:%s", err.Error())
	}
	//1th segment range
	rangeMeta := segmentRange.NameToRange[s.Segments[0].Log.Name]
	if rangeMeta.FirstRindex != s.Segments[0].FirstRindex ||
		rangeMeta.LastRindex != s.Segments[0].LastRindex ||
		rangeMeta.FirstVindex != s.Segments[0].FirstVindex ||
		rangeMeta.LastVindex != s.Segments[0].LastVindex {
		t.Fatalf("rangeMeta.FirstRindex=%d,rangeMeta.LastRindex=%d,"+
			"rangeMeta.FirstVindex=%d,rangeMeta.LastVindex=%d,"+
			"s.Segments[0].FirstRindex=%d,s.Segments[0].LastRindex=%d,"+
			"s.Segments[0].FirstVindex=%d,s.Segments[0].LastVindex=%d",
			rangeMeta.FirstRindex, rangeMeta.LastRindex, rangeMeta.FirstVindex, rangeMeta.LastVindex,
			s.Segments[0].FirstRindex, s.Segments[0].LastRindex, s.Segments[0].FirstVindex, s.Segments[0].LastVindex)
	}
}

//23
//test rebuild write segment(the last segment) index
func TestRebuildWriteSegmentIndex(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	lastSegFirstRindex := s.Segments[2].GetFirstRindex()
	lastSegLastRindex := s.Segments[2].GetLastRindex()
	lastSegFirstVindex := s.Segments[2].GetFirstVindex()
	lastSegLastVindex := s.Segments[2].GetLastVindex()

	//remove segment range meta file, and destroy the 3th segment index file
	rangeMetaPath := path.Join(s.Dir, segmentRangeFile)
	err = os.Remove(rangeMetaPath)
	if err != nil {
		t.Fatalf("Remove file %s,error:%s", rangeMetaPath, err.Error())
	}
	indexFile := s.Segments[2].IndexFile.IndexFile.File
	position, err := indexFile.Seek(0, io.SeekEnd)
	if err != nil {
		t.Fatalf("indexFile Seek error:err=%s", err.Error())
	}
	//the last segment的index file 部分写
	err = indexFile.Truncate(position - 30)
	if err != nil {
		t.Fatalf("Truncate error:err=%s", err.Error())
	}
	//关闭logstore
	s.Close()
	//重新打开出错的logstore
	s, err = newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}

	if s.Segments[2].GetFirstRindex() != lastSegFirstRindex ||
		s.Segments[2].GetLastRindex() != lastSegLastRindex ||
		s.Segments[2].GetFirstVindex() != lastSegFirstVindex ||
		s.Segments[2].GetLastVindex() != lastSegLastVindex {
		t.Fatalf("s.Segments[2].FirstRindex=%d,s.Segments[2].LastRindex=%d,"+
			"s.Segments[2].FirstVindex=%d,s.Segments[2].LastVindex=%d,"+
			"lastSegFirstRindex=%d,lastSegLastRindex=%d,"+
			"lastSegFirstVindex=%d,lastSegLastVindex=%d",
			s.Segments[2].GetFirstRindex(), s.Segments[2].GetLastRindex(),
			s.Segments[2].GetFirstVindex(), s.Segments[2].GetLastVindex(),
			lastSegFirstRindex, lastSegLastRindex, lastSegFirstVindex, lastSegLastVindex)
	}
}

//24
func TestEntriesWithMaxSize(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 10
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)
	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	//每个entry的大小：13
	entrySize := 13
	firsRindex, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("s.FirstIndex err:%s", err.Error())
	}
	lastRindex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("s.LastIndex err:%s", err.Error())
	}
	//在一个segment上读取
	entries, err := s.Entries(firsRindex, lastRindex, uint64(entrySize*4+1))
	if err != nil {
		t.Fatalf("s.Entries err:%s", err.Error())
	}
	if len(entries) != 4 {
		t.Fatalf("len(entries) should be 4, but is %d", len(entries))
	}

	entries, err = s.Entries(firsRindex, lastRindex, uint64(entrySize-1))
	if err != nil {
		t.Fatalf("s.Entries err:%s", err.Error())
	}
	if len(entries) != 1 {
		t.Fatalf("len(entries) should be 1, but is %d", len(entries))
	}
	//max
	entries, err = s.Entries(firsRindex, lastRindex, math.MaxUint64)
	if err != nil {
		t.Fatalf("s.Entries err:%s", err.Error())
	}
	if len(entries) != int(lastRindex-firsRindex) {
		t.Fatalf("len(entries) should be 1, but is %d", len(entries))
	}
}

func TestFetchLogStreamMessagesWithMaxSize(t *testing.T) {

}

//49
//不生成最后一个segment范围信息
func TestCreateSnapshotMeta(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 10
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)

	//
	start := uint64(100)
	entries := newEntries(start, 10)
	if err != nil {
		t.Fatalf("newEntries error:%s", err.Error())
	}
	err = s.StoreEntries(entries)
	if err != nil {
		t.Fatalf("newEntries error:%s", err.Error())
	}
	//out of range(100,109)
	_, err = s.CreateSnapshotMeta(99)
	if err != ErrArgsNotAvailable {
		t.Fatalf("CreateSnapshotMeta error:%v,want ErrArgsNotAvailable", err)
	}
	_, err = s.CreateSnapshotMeta(110)
	if err != ErrArgsNotAvailable {
		t.Fatalf("CreateSnapshotMeta error:%v,want ErrArgsNotAvailable", err)
	}

	if len(s.Segments) != 1 {
		t.Fatalf("len(s.Segments) is %d,want 1", len(s.Segments))
	}
	//one file
	segFiles, err := s.CreateSnapshotMeta(108)
	if err != nil {
		t.Fatalf("1:CreateSnapshotMeta error:%v,want nil", err)
	}
	if len(segFiles) != 1 {
		t.Fatalf("len(segFiles) want 1,but is %d", len(segFiles))
	}
	if segFiles[0].FirstRindex != 0 || segFiles[0].LastRindex != 0 ||
		segFiles[0].FirstVindex != -1 || segFiles[0].LastVindex != -1 ||
		segFiles[0].IsLastSegment != true {
		t.Fatalf("2:segmentFile[0] error,segFiles[0]=%v", *(segFiles[0]))
	}

	for len(s.Segments) < 3 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}

	applyIndex := s.Segments[1].LastRindex
	//applyIndex = segment[1].LastRindex
	segFiles, err = s.CreateSnapshotMeta(applyIndex)
	if err != nil {
		t.Fatalf("3:CreateSnapshotMeta error:%v,want nil", err)
	}
	if len(segFiles) != 2 {
		t.Fatalf("len(segFiles) want 2,but is %d", len(segFiles))
	}
	if segFiles[0].FirstRindex != s.Segments[0].FirstRindex || segFiles[0].LastRindex != s.Segments[0].LastRindex ||
		segFiles[0].FirstVindex != s.Segments[0].FirstVindex || segFiles[0].LastVindex != s.Segments[0].LastVindex ||
		segFiles[0].IsLastSegment != false {
		t.Fatalf("4:segmentFile[0] error,segFiles[0]=%v", *(segFiles[0]))
	}
	if segFiles[1].FirstRindex != 0 || segFiles[1].LastRindex != 0 ||
		segFiles[1].FirstVindex != -1 || segFiles[1].LastVindex != -1 ||
		segFiles[1].IsLastSegment != true {
		t.Fatalf("5:segmentFile[1] error,segFiles[1]=%v", *(segFiles[1]))
	}

	//segment[2].FirstRindex < applyIndex < segment[2].LastRindex
	applyIndex = s.Segments[2].LastRindex - 1
	//applyIndex = segment[1].LastRindex
	segFiles, err = s.CreateSnapshotMeta(applyIndex)
	if err != nil {
		t.Fatalf("3:CreateSnapshotMeta error:%v,want nil", err)
	}
	if len(segFiles) != 3 {
		t.Fatalf("len(segFiles) want 2,but is %d", len(segFiles))
	}
	if segFiles[0].FirstRindex != s.Segments[0].FirstRindex || segFiles[0].LastRindex != s.Segments[0].LastRindex ||
		segFiles[0].FirstVindex != s.Segments[0].FirstVindex || segFiles[0].LastVindex != s.Segments[0].LastVindex ||
		segFiles[0].IsLastSegment != false {
		t.Fatalf("4:segmentFile[0] error,segFiles[0]=%v", *(segFiles[0]))
	}
	if segFiles[1].FirstRindex != s.Segments[1].FirstRindex || segFiles[1].LastRindex != s.Segments[1].LastRindex ||
		segFiles[1].FirstVindex != s.Segments[1].FirstVindex || segFiles[1].LastVindex != s.Segments[1].LastVindex ||
		segFiles[1].IsLastSegment != false {
		t.Fatalf("5:segmentFile[1] error,segFiles[1]=%v", *(segFiles[1]))
	}

	if segFiles[2].FirstRindex != 0 || segFiles[2].LastRindex != 0 ||
		segFiles[2].FirstVindex != -1 || segFiles[2].LastVindex != -1 ||
		segFiles[2].IsLastSegment != true {
		t.Fatalf("5:segmentFile[1] error,segFiles[2]=%v", *(segFiles[2]))
	}
}

//50
func TestCheckAndRestoreDataDir(t *testing.T) {
	InitTmpVdlDir(t)
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	reserveCount := 10
	msgSize := int(ReserveRecordMemory) / reserveCount
	//reset MemCache
	s.Mc = NewMemCache(ReserveRecordMemory, msgSize)
	for len(s.Segments) < 7 {
		lastIndex, err := s.LastIndex()
		if err != nil {
			t.Fatalf("s.LastIndex error:%s", err.Error())
		}
		start := lastIndex + 1
		entries := newEntries(start, 10)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
		err = s.StoreEntries(entries)
		if err != nil {
			t.Fatalf("newEntries error:%s", err.Error())
		}
	}
	//正常情况:segment:0,1,2,3,4,5,6,7
	err = CheckAndRestoreDataDir(s.Dir)
	if err != nil {
		t.Fatalf("CheckAndRestoreDataDir case1 error:%s", err.Error())
	}
	//segment不存在，index和range存在
	//删除第一个segment文件
	deleteSegmentPath := path.Join(s.Dir, "0000000000000000.log")
	err = os.Remove(deleteSegmentPath)
	if err != nil {
		t.Fatalf("Remove error:%s,segmentPath=%s", err.Error(), deleteSegmentPath)
	}
	err = CheckAndRestoreDataDir(s.Dir)
	if err != nil {
		t.Fatalf("CheckAndRestoreDataDir case2 error:%s", err.Error())
	}
	//segment,index不存在，range存在
	deleteSegmentPath2 := path.Join(s.Dir, "0000000000000001.log")
	deleteIndexPath2 := path.Join(s.Dir, "0000000000000001.idx")
	err = os.Remove(deleteSegmentPath2)
	if err != nil {
		t.Fatalf("Remove error:%s,segmentPath2=%s", err.Error(), deleteSegmentPath)
	}
	err = os.Remove(deleteIndexPath2)
	if err != nil {
		t.Fatalf("Remove error:%s,indexPath2=%s", err.Error(), deleteIndexPath2)
	}
	err = CheckAndRestoreDataDir(s.Dir)
	if err != nil {
		t.Fatalf("CheckAndRestoreDataDir case3 error:%s", err.Error())
	}
	//现有segment:2,3,4,5,6,7
	//删除3，使得segment不连续
	deleteSegmentPath3 := path.Join(s.Dir, "0000000000000003.log")
	err = os.Remove(deleteSegmentPath3)
	if err != nil {
		t.Fatalf("Remove error:%s,segmentPath3=%s", err.Error(), deleteSegmentPath)
	}
	err = CheckAndRestoreDataDir(s.Dir)
	if err != ErrNotContinuous {
		t.Fatalf("CheckAndRestoreDataDir case4 want error:ErrNotContinuous,but %s", err.Error())
	}
}
