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

	"io"

	"os"
	"path"

	"github.com/vipshop/vdl/pkg/myos"
)

type NormalResult struct {
	firstRindex uint64
	lastRindex  uint64
	firstVindex int64
	lastVindex  int64
}

func newTornWriteLogstore(t *testing.T) *NormalResult {
	//normal new logstore
	s, err := newLogStore()
	if err != nil {
		t.Fatalf("NewLogStore error:%s", err.Error())
	}

	//write Record
	reserveCount := 10
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

	r := new(NormalResult)
	//
	r.firstRindex, err = s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex error,err=%s", err.Error())
	}
	r.lastRindex, err = s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex error,err=%s", err.Error())
	}
	r.firstVindex, err = s.FirstVindex()
	if err != nil {
		t.Fatalf("FirstVindex error,err=%s", err.Error())
	}
	r.lastVindex, err = s.LastVindex()
	if err != nil {
		t.Fatalf("LastVindex error,err=%s", err.Error())
	}

	lastSegment := s.Segments[2]
	//构造异常情况
	//segment[2]最后一个record异常，segment[2]对应的index去掉5项
	firstRindex2 := lastSegment.GetFirstRindex()
	lastRindex2 := lastSegment.GetLastRindex()
	if (lastRindex2 - firstRindex2 + 1) < 10 {
		t.Fatalf("lastRindex2-firstRindex2+1=%d,less than 10", lastRindex2-firstRindex2+1)
	}
	//no lock
	ies2, err := lastSegment.getIndexEntries(RaftIndexType, firstRindex2, lastRindex2+1, noLimit)
	if err != nil {
		t.Fatalf("")
	}

	lastIndexEntry2 := ies2[len(ies2)-1]
	lastPosition := lastIndexEntry2.Position
	zeroBuf := make([]byte, RecordHeaderSize)
	//设置lastIndexEntry2对应的Record的Record Header全部为0
	copy(lastSegment.Log.MapData[lastPosition:lastPosition+RecordHeaderSize], zeroBuf)
	err = myos.Syncfilerange(
		lastSegment.Log.File.Fd(),
		lastPosition,
		RecordHeaderSize,
		myos.SYNC_FILE_RANGE_WAIT_BEFORE|myos.SYNC_FILE_RANGE_WRITE|myos.SYNC_FILE_RANGE_WAIT_AFTER,
	)
	if err != nil {
		t.Fatalf("[segment.go-deleteRecordsByPosition]:Syncfilerange error,err=%s,position=%d,length=%d",
			err.Error(), lastPosition, RecordHeaderSize)
	}

	//截断segment[2]对应的index文件，去掉最后5项
	truncatePos := int64((len(ies2) - 5) * IndexEntrySize)
	err = lastSegment.IndexFile.IndexFile.Truncate(truncatePos)
	if err != nil {
		t.Fatalf("Truncate error ,err=%s,truncatePos=%d", err.Error(), truncatePos)
	}
	_, err = lastSegment.IndexFile.IndexFile.Seek(truncatePos, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek error ,err=%s,pos=%d", err.Error(), truncatePos)
	}
	s.Close()
	return r
}

//44
func TestNeedRecover(t *testing.T) {
	InitTmpVdlDir(t)
	//no exit flag file
	flagFile := NewFlagFile(TmpVdlDir)
	needRecover := flagFile.NeedRecover()
	if needRecover == false {
		t.Fatalf("NeedRecover should be true,not false")
	}
	//unnormal_exit
	flagFile.WriteExitFlag(UnnormalExitFlag)
	needRecover = flagFile.NeedRecover()
	if needRecover == false {
		t.Fatalf("NeedRecover should be true,not false")
	}
	//normal_exit
	flagFile.WriteExitFlag(NormalExitFlag)
	needRecover = flagFile.NeedRecover()
	if needRecover == true {
		t.Fatalf("NeedRecover should be false,not true")
	}
}

//45
func TestRecoverWithNoExitFlagFile(t *testing.T) {
	InitTmpVdlDir(t)
	r := newTornWriteLogstore(t)
	//remove exit flag file
	err := os.Remove(path.Join(TmpVdlDir, exitFlagFileName))
	if err != nil {
		t.Fatalf("Remove error,err=%s", err.Error())
	}

	s, err := newLogStore()
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	firstRindex, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex error,err=%s", err.Error())
	}
	lastRindex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex error,err=%s", err.Error())
	}
	firstVindex, err := s.FirstVindex()
	if err != nil {
		t.Fatalf("FirstVindex error,err=%s", err.Error())
	}
	lastVindex, err := s.LastVindex()
	if err != nil {
		t.Fatalf("LastVindex error,err=%s", err.Error())
	}

	if r.lastRindex != lastRindex+1 || r.lastVindex != lastVindex+1 {
		t.Fatalf("r.firstRindex=%d,r.lastRindex=%d,r.firstVindex=%d,r.lastVindex=%d,"+
			"firstRindex=%d,lastRindex=%d,firstVindex=%d,lastVindex=%d",
			r.firstRindex, r.lastRindex, r.firstVindex, r.lastVindex,
			firstRindex, lastRindex, firstVindex, lastVindex)
	}
}

//46
func TestRecoverWithUnnormalExit(t *testing.T) {
	InitTmpVdlDir(t)
	r := newTornWriteLogstore(t)
	//write UnnormalExitFlag
	flagFile := NewFlagFile(TmpVdlDir)
	flagFile.WriteExitFlag(UnnormalExitFlag)

	s, err := newLogStore()
	defer func() {
		err = s.Close()
		if err != nil {
			t.Fatalf("LogStore.Close error:%s", err.Error())
		}
	}()
	firstRindex, err := s.FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex error,err=%s", err.Error())
	}
	lastRindex, err := s.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex error,err=%s", err.Error())
	}
	firstVindex, err := s.FirstVindex()
	if err != nil {
		t.Fatalf("FirstVindex error,err=%s", err.Error())
	}
	lastVindex, err := s.LastVindex()
	if err != nil {
		t.Fatalf("LastVindex error,err=%s", err.Error())
	}

	if r.lastRindex != lastRindex+1 || r.lastVindex != lastVindex+1 {
		t.Fatalf("r.firstRindex=%d,r.lastRindex=%d,r.firstVindex=%d,r.lastVindex=%d,"+
			"firstRindex=%d,lastRindex=%d,firstVindex=%d,lastVindex=%d",
			r.firstRindex, r.lastRindex, r.firstVindex, r.lastVindex,
			firstRindex, lastRindex, firstVindex, lastVindex)
	}

}
