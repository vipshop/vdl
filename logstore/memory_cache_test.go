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

	"math"

	"github.com/vipshop/vdl/consensus/raft/raftpb"
)

//37
func TestMemCacheWriteAndRead(t *testing.T) {
	reserveCount := 1024
	msgSize := int(ReserveRecordMemory) / reserveCount
	m := NewMemCache(ReserveRecordMemory, msgSize)
	records := newRecords(1, 100, t)
	m.WriteRecords(records)
	if m.FirstRindex != 1 || m.LastRindex != 100 {
		t.Fatalf("m.FirstRindex=%d, m.LastRindex=%d", m.FirstRindex, m.LastRindex)
	}
	if m.FirstVindex != 1 || m.LastVindex != 100 {
		t.Fatalf("m.FirstVindex=%d, m.LastVindex=%d", m.FirstVindex, m.LastVindex)
	}
	if len(m.RindexToRecords) != int(reserveCount) &&
		len(m.VindexToRecords) != int(reserveCount) {
		t.Fatalf("len(m.RindexToRecords)=%d, len(m.RindexToRecords)=%d", len(m.RindexToRecords), len(m.RindexToRecords))
	}
	for i, record := range records {
		raftLogs, exist := m.GetRaftLogByRindex(records[i].Rindex, records[i].Rindex+1, math.MaxUint64)
		if exist == false {
			t.Fatalf("get rindex=%d record not exist", i)
		}
		if checkRaftEntry(raftLogs[0], record) == false {
			t.Fatalf("raftEntry=%v,newRecord=%v", raftLogs[0], record)
		}
	}
	m.close()
}

func checkRaftEntry(entry raftpb.Entry, record *Record) bool {
	return entry.Type == record.RaftType &&
		entry.Index == record.Rindex && entry.Term == record.Term &&
		bytes.Equal(entry.Data, record.Data)
}

//38
func TestLoadRecords(t *testing.T) {
	reserveCount := 100
	msgSize := int(ReserveRecordMemory) / reserveCount
	m := NewMemCache(ReserveRecordMemory, msgSize)
	records := newRecords(1, 200, t)
	m.LoadRecords(records)
	if m.FirstRindex != 101 || m.LastRindex != 200 {
		t.Fatalf("m.FirstRindex=%d, m.LastRindex=%d", m.FirstRindex, m.LastRindex)
	}
	if m.FirstVindex != 101 || m.LastVindex != 200 {
		t.Fatalf("m.FirstVindex=%d, m.LastVindex=%d", m.FirstVindex, m.LastVindex)
	}
	if len(m.RindexToRecords) != int(reserveCount) &&
		len(m.VindexToRecords) != int(reserveCount) {
		t.Fatalf("len(m.RindexToRecords)=%d, len(m.RindexToRecords)=%d", len(m.RindexToRecords), len(m.RindexToRecords))
	}
	for i := 100; i < 200; i++ {
		raftLogs, exist := m.GetRaftLogByRindex(records[i].Rindex, records[i].Rindex+1, math.MaxUint64)
		if exist == false {
			t.Fatalf("get rindex=%d record not exist", i)
		}
		if checkRaftEntry(raftLogs[0], records[i]) == false {
			t.Fatalf("raftEntry=%v,newRecord=%v", raftLogs[0], records[i])
		}
	}
	//insert new records
	records = newRecords(201, 150, t)
	m.WriteRecords(records)
	//check
	if m.FirstRindex != 251 || m.LastRindex != 350 {
		t.Fatalf("m.FirstRindex=%d, m.LastRindex=%d", m.FirstRindex, m.LastRindex)
	}
	if m.FirstVindex != 251 || m.LastVindex != 350 {
		t.Fatalf("m.FirstVindex=%d, m.LastVindex=%d", m.FirstVindex, m.LastVindex)
	}
	if len(m.RindexToRecords) != int(reserveCount) &&
		len(m.VindexToRecords) != int(reserveCount) {
		t.Fatalf("len(m.RindexToRecords)=%d, len(m.RindexToRecords)=%d", len(m.RindexToRecords), len(m.RindexToRecords))
	}
	//check the new records
	for i := 50; i < 150; i++ {
		raftLogs, exist := m.GetRaftLogByRindex(records[i].Rindex, records[i].Rindex+1, math.MaxUint64)
		if exist == false {
			t.Fatalf("get rindex=%d record not exist", i)
		}
		if checkRaftEntry(raftLogs[0], records[i]) == false {
			t.Fatalf("raftEntry=%v,newRecord=%v", raftLogs[0], records[i])
		}
	}
	m.close()
}

//39
func TestDeleteRecordMemCache(t *testing.T) {
	reserveCount := 100
	msgSize := int(ReserveRecordMemory) / reserveCount
	m := NewMemCache(ReserveRecordMemory, msgSize)
	records := newRecords(1, 150, t)
	m.LoadRecords(records)
	if m.FirstRindex != 51 || m.LastRindex != 150 {
		t.Fatalf("m.FirstRindex=%d, m.LastRindex=%d", m.FirstRindex, m.LastRindex)
	}
	if m.FirstVindex != 51 || m.LastVindex != 150 {
		t.Fatalf("m.FirstVindex=%d, m.LastVindex=%d", m.FirstVindex, m.LastVindex)
	}
	//delete record from rindex is 51
	m.DeleteRecordsByRindex(120)
	if m.FirstRindex != 0 || m.LastRindex != 0 {
		t.Fatalf("m.FirstRindex=%d, m.LastRindex=%d", m.FirstRindex, m.LastRindex)
	}
	if m.FirstVindex != -1 || m.LastVindex != -1 {
		t.Fatalf("m.FirstVindex=%d, m.LastVindex=%d", m.FirstVindex, m.LastVindex)
	}
	m.close()
}
