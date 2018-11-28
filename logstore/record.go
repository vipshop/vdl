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
	"hash/crc32"

	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/logstream"
	"github.com/vipshop/vdl/pkg/glog"
)

const (
	MetaDataType int32 = iota
	RaftLogType
	VdlLogType
)

//Record store order
//Crc|RecordType|DataLen|Vindex|Term|Rindex|RaftType|Data
type Record struct {
	Crc        uint32 // crc for the remainder field
	RecordType int32  //
	DataLen    int64  //data length
	Vindex     int64

	Term     uint64
	Rindex   uint64
	RaftType raftpb.EntryType
	Data     []byte
}

func binaryToRecord(b []byte) (*Record, error) {
	record := new(Record)
	record.Crc = Encoding.Uint32(b[:4])
	newCrc := crc32.ChecksumIEEE(b[4:])
	if record.Crc != newCrc {
		return nil, ErrCrcNotMatch
	}

	record.RecordType = int32(Encoding.Uint32(b[4:8]))
	record.DataLen = int64(Encoding.Uint64(b[8:16]))
	record.Vindex = int64(Encoding.Uint64(b[16:24]))

	record.Term = Encoding.Uint64(b[24:32])
	record.Rindex = Encoding.Uint64(b[32:40])
	record.RaftType = raftpb.EntryType(Encoding.Uint32(b[40:44]))
	record.Data = b[44:]

	return record, nil
}

//func binaryToLogstreamEntry(b []byte) logstream.Entry {
//	var entry logstream.Entry
//	//var rindex uint64
//	//Remove CRC
//	//recordCrc := Encoding.Uint32(b[:4])
//	//newCrc := crc32.ChecksumIEEE(b[4:])
//	//if recordCrc != newCrc {
//	//	return entry, 0, ErrCrcNotMatch
//	//}
//
//	//entry.Offset = int64(Encoding.Uint64(b[16:24]))
//	//rindex = Encoding.Uint64(b[32:40])
//	//44-52 is reqId
//	//entry.Data = make([]byte, len(b[52:]))
//	//copy(entry.Data, b[52:])
//	entry.Data = b[52:]
//	return entry
//}

func recordBinaryToTerm(b []byte) uint64 {
	return Encoding.Uint64(b[24:32])
}

func recordBinaryToIndex(b []byte) uint64 {
	return Encoding.Uint64(b[32:40])
}

func recordBinaryToDataLen(b []byte) int64 {
	return int64(Encoding.Uint64(b[8:16]))
}

func binaryToRaftEntry(b []byte) (raftpb.Entry, error) {
	var entry raftpb.Entry
	//recordCrc := Encoding.Uint32(b[:4])
	//newCrc := crc32.ChecksumIEEE(b[4:])
	//if recordCrc != newCrc {
	//	return entry, ErrCrcNotMatch
	//}
	entry.Type = raftpb.EntryType(Encoding.Uint32(b[40:44]))
	entry.Term = Encoding.Uint64(b[24:32])
	entry.Index = Encoding.Uint64(b[32:40])
	//shallow copy
	entry.Data = b[44:]
	return entry, nil
}

func recordToBinary(r *Record) []byte {
	b := make([]byte, RecordHeaderSize)

	Encoding.PutUint32(b[4:8], uint32(r.RecordType))
	Encoding.PutUint64(b[8:16], uint64(r.DataLen))
	Encoding.PutUint64(b[16:24], uint64(r.Vindex))
	Encoding.PutUint64(b[24:32], r.Term)
	Encoding.PutUint64(b[32:40], r.Rindex)
	Encoding.PutUint32(b[40:44], uint32(r.RaftType))
	b = append(b, r.Data...)
	r.Crc = crc32.ChecksumIEEE(b[4:])
	Encoding.PutUint32(b[:4], r.Crc)
	return b
}

func generateRecordCrc(r *Record) uint32 {
	b := make([]byte, RecordHeaderSize)

	Encoding.PutUint32(b[4:8], uint32(r.RecordType))
	Encoding.PutUint64(b[8:16], uint64(r.DataLen))
	Encoding.PutUint64(b[16:24], uint64(r.Vindex))
	Encoding.PutUint64(b[24:32], r.Term)
	Encoding.PutUint64(b[32:40], r.Rindex)
	Encoding.PutUint32(b[40:44], uint32(r.RaftType))
	b = append(b, r.Data...)

	return crc32.ChecksumIEEE(b[4:])
}

func recordsToBinary(records []*Record) []byte {
	buf := make([]byte, 0, 128)
	for i := 0; i < len(records); i++ {
		b := recordToBinary(records[i])
		buf = append(buf, b...)
	}
	return buf
}

func RecordsToVdlLog(records []*Record, maxSize int32) []logstream.Entry {
	if len(records) == 0 {
		return nil
	}
	var vdlLogSize int64
	vdlLogs := make([]logstream.Entry, 0, 100)
	//check log Consistency
	defer func() {
		count := len(vdlLogs)
		if 1 < count && vdlLogs[0].Offset+int64(count-1) != vdlLogs[count-1].Offset {
			glog.Fatalf("[record.go-RecordsToRaftLog]:raft log is not continuous,"+
				"firstVindex=%d,lastVindex=%d,count=%d", vdlLogs[0].Offset, vdlLogs[count-1].Offset,
				count)
		}
	}()

	for _, record := range records {
		e := logstream.Entry{
			Offset: record.Vindex,
			Data:   record.Data[8:], // Remove the 8 bytes in the head of data, the field is reqId
		}
		vdlLogs = append(vdlLogs, e)
		vdlLogSize += record.DataLen
		if vdlLogSize >= int64(maxSize) && len(vdlLogs) == 1 {
			//debug info
			if glog.V(1) {
				glog.Infof("D:[util.go-RecordsToRaftLog]:the size of first raft log is large than maxsize,raftLogSize=%d,maxSize=%d,"+
					"only return first raft log", vdlLogSize, maxSize)
			}
			return vdlLogs
		}
		if vdlLogSize > int64(maxSize) && 1 < len(vdlLogs) {
			//debug info
			if glog.V(1) {
				glog.Infof("D:[util.go-RecordsToRaftLog]:the size of raft logs is large than maxsize,raftLogSize=%d,maxSize=%d",
					vdlLogSize, maxSize)
			}
			return vdlLogs[:len(vdlLogs)-1]
		}
	}

	return vdlLogs
}

//仅用于性能测试
//only for performance test,don't use in production
func RecordsToRaftLog(records []*Record, maxSize uint64) []raftpb.Entry {
	if len(records) == 0 {
		return nil
	}
	var raftLogSize int64
	raftLogs := make([]raftpb.Entry, 0, 100)
	//check log Consistency
	defer func() {
		count := len(raftLogs)
		if 1 < count && raftLogs[0].Index+uint64(count-1) != raftLogs[count-1].Index {
			glog.Fatalf("[record.go-RecordsToRaftLog]:raft log is not continuous,"+
				"firstIndex=%d,lastIndex=%d,count=%d", raftLogs[0].Index, raftLogs[count-1].Index,
				count)
		}
	}()

	for _, record := range records {
		var entry raftpb.Entry
		entry.Term = record.Term
		entry.Type = record.RaftType
		entry.Index = record.Rindex
		entry.Data = record.Data
		raftLogs = append(raftLogs, entry)
		raftLogSize = raftLogSize + record.DataLen + TermSize + RindexSize + RaftTypeSize
		if uint64(raftLogSize) >= maxSize && len(raftLogs) == 1 {
			//debug info
			if glog.V(1) {
				glog.Infof("D:[util.go-RecordsToRaftLog]:the size of first raft log is large than maxsize,raftLogSize=%d,maxSize=%d,"+
					"only return first raft log", raftLogSize, maxSize)
			}
			return raftLogs
		}
		if uint64(raftLogSize) > maxSize && 1 < len(raftLogs) {
			//debug info
			if glog.V(1) {
				glog.Infof("D:[util.go-RecordsToRaftLog]:the size of raft logs is large than maxsize,raftLogSize=%d,maxSize=%d,"+
					"only return raft log[%d,%d]", raftLogSize, maxSize, raftLogs[0].Index, raftLogs[len(raftLogs)-2].Index)
			}
			return raftLogs[:len(raftLogs)-1]
		}
	}
	return raftLogs
}
