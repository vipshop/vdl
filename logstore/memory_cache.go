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
	"sync"

	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/pkg/glog"

	"github.com/vipshop/vdl/logstream"
)

var (
	//512MB
	ReserveRecordMemory uint64 = 1024 * 1024 * 512 //reserve memory for tail records.
)

type MemCache struct {
	Mu              sync.RWMutex //the last segment and logstore will operate the MemCache, so need a lock
	VindexToRecords []*Record    //the last segment file records which cache in memeory
	RindexToRecords []*Record
	CacheSize       uint64
	FirstRindex     uint64
	LastRindex      uint64
	FirstVindex     int64
	LastVindex      int64
}

//创建MemCache,CacheSize取决为：保留内存/单条消息最大Size
func NewMemCache(MemCacheSizeByte uint64, maxSizePerMsg int) *MemCache {
	mc := new(MemCache)

	if MemCacheSizeByte < ReserveRecordMemory {
		glog.Fatalf("[memory_cache.go-NewMemCache]:MemCacheSizeByte(%d) is too little,must large than %d",
			MemCacheSizeByte, ReserveRecordMemory)
	}
	if MemCacheSizeByte < uint64(maxSizePerMsg) {
		glog.Fatalf("[memory_cache.go-NewMemCache]:maxSizePerMsg(%d) is too large,must less than %d",
			maxSizePerMsg, ReserveRecordMemory)
	}

	mc.CacheSize = MemCacheSizeByte / uint64(maxSizePerMsg)
	mc.VindexToRecords = make([]*Record, mc.CacheSize)
	mc.RindexToRecords = make([]*Record, mc.CacheSize)
	mc.FirstRindex = 0
	mc.LastRindex = 0
	mc.FirstVindex = -1
	mc.LastVindex = -1
	return mc
}

//仅用于测试
//only for performance test,don't use in production
func (m *MemCache) GetRecords(count int) []*Record {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	var i uint64
	records := make([]*Record, 0, count)
	//deep copy
	for i = 0; i < uint64(count); i++ {
		pos := (i + m.FirstRindex) % m.CacheSize
		record := &Record{
			Crc:        m.RindexToRecords[pos].Crc,
			RecordType: m.RindexToRecords[pos].RecordType,
			DataLen:    m.RindexToRecords[pos].DataLen,
			Vindex:     m.RindexToRecords[pos].Vindex,
			Term:       m.RindexToRecords[pos].Term,
			Rindex:     m.RindexToRecords[pos].Rindex,
			RaftType:   m.RindexToRecords[pos].RaftType,
			Data:       m.RindexToRecords[pos].Data,
		}
		records = append(records, record)
	}
	return records
}

//根据范围和消息总大小限制获取raft entry
//范围:[start,end)
func (m *MemCache) GetRaftLogByRindex(start, end uint64, maxSize uint64) ([]raftpb.Entry, bool) {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	var raftLogSize uint64
	//范围是否匹配
	if start < m.FirstRindex || end > m.LastRindex+1 {
		//debug info
		if glog.V(1) {
			glog.Infof("D:[memory_cache.go-GetRecordsByRindex]:Get records by rindex from [start,end) is [%d,%d) im MemCache, "+
				"but [m.FirstRindex,m.LastRindex) is [%d,%d)", start, end, m.FirstRindex, m.LastRindex)
		}
		return nil, false
	} else {
		var cacheRecord *Record
		raftLogs := createRaftEntriesSlice(end - start)
		for i := start; i < end; i++ {

			pos := i % m.CacheSize
			cacheRecord = m.RindexToRecords[pos]
			if cacheRecord.Rindex != i {
				glog.Fatalf("record in memcache is not correct,rindex should be:%d, but rindex is %d,"+
					"m.FirstRindex=%d,m.LastRindex=%d,m.FirstVindex=%d,m.LastVindex=%d",
					i, cacheRecord.Rindex, m.FirstRindex, m.LastRindex, m.FirstVindex, m.LastVindex)
			}
			entry := raftpb.Entry{
				Term:  cacheRecord.Term,
				Index: cacheRecord.Rindex,
				Type:  cacheRecord.RaftType,
				//Shallow Copy
				Data: cacheRecord.Data,
			}

			raftLogs = append(raftLogs, entry)
			raftLogSize += uint64(len(entry.Data))
			if raftLogSize >= maxSize {
				break
			}
		}
		return raftLogs, true
	}
}

//获取从startVindex到endRindex之间的VDL Log，包括endRindex所在的记录
//startVindex是VDL log index
//endRindex是raft log index
func (m *MemCache) GetVdlLogByVindex(startVindex int64, endRindex uint64, maxSize int32) ([]logstream.Entry, bool) {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	var vdlLogSize int64
	if startVindex < m.FirstVindex || startVindex > m.LastVindex {
		//debug info
		if glog.V(1) {
			glog.Infof("D:[memory_cache.go-GetRecordsByVindex]:Get records by vindex from startVindex=%d in MemCache, "+
				"but [m.FirstRindex,m.LastRindex] is [%d,%d)", startVindex,
				m.FirstVindex, m.LastVindex)
		}
		return nil, false
	} else {
		vdlLogs := createLogStreamEntriesSlice(100)
		for i := startVindex; i <= m.LastVindex; i++ {
			pos := uint64(i) % m.CacheSize

			cacheRecord := m.VindexToRecords[pos]
			//check the vindex
			if cacheRecord.Vindex != i {
				glog.Fatalf("record in memcache is not correct,vindex should be:%d, but vindex is %d,"+
					"m.FirstRindex=%d,m.LastRindex=%d,m.FirstVindex=%d,m.LastVindex=%d",
					i, cacheRecord.Vindex, m.FirstRindex, m.LastRindex, m.FirstVindex, m.LastVindex)
			}
			e := logstream.Entry{
				Offset: cacheRecord.Vindex,
				// Remove the 8 bytes in the head of data, the field is reqId
				// Shallow Copy
				Data: cacheRecord.Data[8:],
			}
			if endRindex < cacheRecord.Rindex {
				break
			}
			vdlLogs = append(vdlLogs, e)
			//vdlLogSize为用户数据大小之和
			vdlLogSize += int64(len(e.Data))
			if vdlLogSize >= int64(maxSize) {
				break
			}
		}
		return vdlLogs, true
	}
}

func (m *MemCache) WriteRecords(records []*Record) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if len(records) == 0 {
		return
	}
	if 0 < m.LastRindex && m.LastRindex+1 != records[0].Rindex {
		glog.Fatalf("[memory_cache.go-WriteRecord]: the records are not sequential, s.LastRindex=%d, "+
			"the record raft index should be %d, but record.Rindex=%d", m.LastRindex, m.LastRindex+1, records[0].Rindex)
	}
	for i := 0; i < len(records); i++ {
		m.writeRecord(records[i])
	}
}

func (m *MemCache) writeRecord(r *Record) {
	rpos := r.Rindex % m.CacheSize
	m.RindexToRecords[rpos] = r
	if m.FirstRindex == 0 {
		m.FirstRindex = r.Rindex
	}
	if m.LastRindex < r.Rindex {
		m.LastRindex = r.Rindex
	}
	//当最大rindex和最小rindex超过CacheSize时，更新FirstRindex
	if m.CacheSize < m.LastRindex-m.FirstRindex+1 {
		m.FirstRindex = m.LastRindex - m.CacheSize + 1
	}

	if 0 <= r.Vindex {
		vpos := r.Vindex % int64(m.CacheSize)
		m.VindexToRecords[vpos] = r
		if m.FirstVindex == -1 {
			m.FirstVindex = r.Vindex
		}
		if m.LastVindex < r.Vindex {
			m.LastVindex = r.Vindex
		}
		//当最大vindex和最小vindex超过CacheSize时，更新FirstVindex
		if int64(m.CacheSize) < m.LastVindex-m.FirstVindex+1 {
			m.FirstVindex = m.LastVindex - int64(m.CacheSize) + 1
		}
	}

	//debug info
	if glog.V(1) {
		glog.Infof("D:[memory_cache.go-WriteRecord]:write record[rindex=%d,vindex=%d] in MemCache,"+
			"m.FirstRindex=%d, m.LastRindex=%d,m.FirstVindex=%d, m.LastVindex=%d",
			r.Rindex, r.Vindex, m.FirstRindex, m.LastRindex, m.FirstVindex, m.LastVindex)
	}
}

//将MemCache中的数据全部清空
func (m *MemCache) DeleteRecordsByRindex(rindex uint64) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	glog.Infof("[memory_cache.go-DeleteRecordsByRindex]:before DeleteRecordsByRindex,rindex=%d,"+
		"m.FirstRindex=%d,m.LastRindex=%d,"+
		"m.FirstVindex=%d,m.LastVindex=%d", rindex, m.FirstRindex, m.LastRindex,
		m.FirstVindex, m.LastVindex)

	m.FirstRindex = 0
	m.LastRindex = 0
	m.FirstVindex = -1
	m.LastVindex = -1
}

//将records加载到MemCache中
func (m *MemCache) LoadRecords(records []*Record) {
	if len(records) == 0 {
		return
	}

	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.FirstRindex = 0
	m.LastRindex = 0
	m.FirstVindex = -1
	m.LastVindex = -1

	for i := 0; i < len(records); i++ {
		rpos := (records[i].Rindex) % m.CacheSize
		m.RindexToRecords[rpos] = records[i]
		if m.FirstRindex == 0 {
			m.FirstRindex = records[i].Rindex
		}
		if m.LastRindex < records[i].Rindex {
			m.LastRindex = records[i].Rindex
		}
		//reset firstRindex
		if m.CacheSize < m.LastRindex-m.FirstRindex+1 {
			m.FirstRindex = m.LastRindex - m.CacheSize + 1
		}

		if 0 <= records[i].Vindex {
			vpos := (records[i].Vindex) % int64(m.CacheSize)
			m.VindexToRecords[vpos] = records[i]
			if m.FirstVindex == -1 {
				m.FirstVindex = records[i].Vindex
			}
			if m.LastVindex < records[i].Vindex {
				m.LastVindex = records[i].Vindex
			}
			//reset firstVindex
			if int64(m.CacheSize) < m.LastVindex-m.FirstVindex+1 {
				m.FirstVindex = m.LastVindex - int64(m.CacheSize) + 1
			}
		}
	}
	//debug info
	if glog.V(1) {
		glog.Infof("D:[memory_cache.go-LoadRecords]:MemCache loadRecords,m.FirstRindex=%d,m.LastRindex=%d,"+
			"m.FirstVindex=%d,m.LastVindex=%d", m.FirstRindex, m.LastRindex, m.FirstVindex, m.LastVindex)
	}
}

func (m *MemCache) close() {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.FirstRindex = 0
	m.LastRindex = 0
	m.FirstVindex = -1
	m.LastVindex = -1
}

func (m *MemCache) GetFirstRindex() uint64 {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.FirstRindex
}

func (m *MemCache) GetLastRindex() uint64 {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.LastRindex
}

func (m *MemCache) GetFirstVindex() int64 {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.FirstVindex
}

func (m *MemCache) GetLastVindex() int64 {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.LastVindex
}
