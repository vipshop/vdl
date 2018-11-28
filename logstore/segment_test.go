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
	"encoding/binary"
	"path"
	"testing"

	"github.com/coreos/etcd/pkg/fileutil"

	"github.com/vipshop/vdl/consensus/raft/raftpb"

	"os"

	"flag"
	"math"
)

var (
	mc               *MemCache
	meta             *FileMeta
	segmentSize      int64 = 10000
	TmpVdlDir        string
	jenkinsWorkspace *string = flag.String("jenkinsWorkspace", "/Users/flike/vdl", "the jenkins workspace")
)

func init() {
	flag.Parse()
	TmpVdlDir = *jenkinsWorkspace
	ReserveRecordsCount := 1024
	msgSize := int(ReserveRecordMemory) / ReserveRecordsCount
	mc = NewMemCache(ReserveRecordMemory, msgSize)
	meta = &FileMeta{
		VdlVersion: "1.0",
	}
}

func InitTmpVdlDir(t *testing.T) {
	if IsFileExist(TmpVdlDir) {
		err := os.RemoveAll(TmpVdlDir)
		if err != nil {
			t.Fatalf("Remove All error:%s", err.Error())
		}
	}
	err := os.MkdirAll(TmpVdlDir, fileutil.PrivateDirMode)
	if err != nil {
		t.Fatalf("Remove All error:%s", err.Error())
	}
}

func newSegment(t *testing.T) (*Segment, error) {
	cfg := &NewSegmentConfig{
		Dir:      TmpVdlDir,
		Name:     segmentFileName(0),
		MaxBytes: segmentSize,
		Mc:       mc,
		Meta:     meta,
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}
	return segment, nil
}

//24
func TestNewSegment(t *testing.T) {
	InitTmpVdlDir(t)
	cfg := &NewSegmentConfig{
		Dir:           TmpVdlDir,
		Name:          segmentFileName(0),
		MaxBytes:      segmentSize,
		Mc:            mc,
		Meta:          meta,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}
	if segment.Log.Name != segmentFileName(0) {
		t.Fatalf("segment.Name not equal,segment.Name=%s", segment.Log.Name)
	}
	if segment.SegmentPath != path.Join(cfg.Dir, segment.Log.Name) {
		t.Fatalf("segment.SegmentPath not equal,segment.SegmentPath=%s", segment.SegmentPath)
	}
	if segment.FirstRindex != 0 || segment.LastRindex != 0 || segment.FirstVindex != -1 || segment.LastVindex != -1 {
		t.Fatalf("segment index not equal,FirstRindex=%d,LastRindex=%d,FirstVindex=%d,LastVindex=%d",
			segment.FirstRindex, segment.LastRindex, segment.FirstVindex, segment.LastVindex)
	}

	if segment.Mc != mc {
		t.Fatalf("segment.Mc or segment.IndexCache not equal")
	}
	if segment.IndexFile.Name != indexFileName(0) {
		t.Fatalf("segment.Index.Name not equal,segment.Index.Name=%s", segment.IndexFile.Name)
	}
	if segment.Status != SegmentRDWR || segment.Log.WritePosition != 47 {
		t.Fatalf("segment.Status=%v,segment.Position=%d", segment.Status, segment.Log.WritePosition)
	}
	segment.Remove()
}

//25
func TestSetAndGetMeta(t *testing.T) {
	InitTmpVdlDir(t)
	cfg := &NewSegmentConfig{
		Dir:      TmpVdlDir,
		Name:     segmentFileName(0),
		MaxBytes: segmentSize,
		Mc:       mc,
		Meta:     meta,
		//Ic:            ic,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}
	buf, err := segment.getSegmentFileMeta()
	if err != nil {
		t.Fatalf("getSegmentFileMeta error:%s", err.Error())
	}
	m, err := binaryToMeta(buf)
	if m.VdlVersion != meta.VdlVersion {
		t.Fatalf("meta not equal, m.VdlVersion=%s,meta.VdlVersion=%s", m.VdlVersion, meta.VdlVersion)
	}
}

//26
func TestReadRecordsByVindex(t *testing.T) {
	InitTmpVdlDir(t)
	cfg := &NewSegmentConfig{
		Dir:           TmpVdlDir,
		Name:          segmentFileName(0),
		MaxBytes:      segmentSize,
		Mc:            mc,
		Meta:          meta,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}

	records := newRecords(1, 10, t)
	err = segment.WriteRecords(records)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}

	records = newRaftRecords(11, 10, t)
	err = segment.WriteRecords(records)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	//segment Close
	segment.Close()
	//open with read
	readCfg := &OpenSegmentConfig{
		Dir:             TmpVdlDir,
		Name:            segmentFileName(0),
		MaxBytes:        segmentSize,
		OpenSegmentMode: ReadOnlyMode,
		Range:           nil,
		Mc:              mc,
		RangeMetaFile:   NewRangeFile(TmpVdlDir),
	}
	readSeg, err := OpenSegmentWithRead(readCfg)
	if err != nil {
		t.Fatalf("OpenSegmentWithRead error:%s", err.Error())
	}

	lastRindex := readSeg.GetLastRindex()

	vdlResult, err := readSeg.ReadVdlLogsByVindex(0, lastRindex, math.MaxInt32)
	if err != nil {
		t.Fatalf("readRecordsByRindex error:%s", err.Error())
	}
	if len(vdlResult.entries) != 10 {
		t.Fatalf("len(records)!=10")
	}
	raftLogs, _, err := readSeg.ReadRaftLogsByRindex(1, lastRindex+1, math.MaxUint64)
	if err != nil {
		t.Fatalf("readRecordsByRindex error:%s", err.Error())
	}
	if len(raftLogs) != 20 {
		t.Fatalf("len(records)=%d,not equal 20", len(raftLogs))
	}
}

//27
func TestReadRecordsByRindex(t *testing.T) {
	ReserveRecordsCount := 1024
	msgSize := int(ReserveRecordMemory) / ReserveRecordsCount
	InitTmpVdlDir(t)
	cfg := &NewSegmentConfig{
		Dir:           TmpVdlDir,
		Name:          segmentFileName(0),
		MaxBytes:      segmentSize,
		Mc:            NewMemCache(ReserveRecordMemory, msgSize),
		Meta:          meta,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}

	records := newRecords(1, 1, t)
	err = segment.WriteRecords(records)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	//segment Close
	segment.Close()
	//open with read
	readCfg := &OpenSegmentConfig{
		Dir:             TmpVdlDir,
		Name:            segmentFileName(0),
		MaxBytes:        segmentSize,
		OpenSegmentMode: ReadOnlyMode,
		Range:           nil,
		Mc:              mc,
		RangeMetaFile:   NewRangeFile(TmpVdlDir),
	}
	readSeg, err := OpenSegmentWithRead(readCfg)
	if err != nil {
		t.Fatalf("OpenSegmentWithRead error:%s", err.Error())
	}
	entries, _, err := readSeg.ReadRaftLogsByRindex(1, 2, math.MaxInt64)
	if err != nil {
		t.Fatalf("readRecordsByRindex error:%s", err.Error())
	}
	if len(entries) != 1 {
		t.Fatalf("len(records)!=1")
	}
}

//28
func TestDeleteRecordsByRindex(t *testing.T) {
	InitTmpVdlDir(t)
	ReserveRecordsCount := 1024
	msgSize := int(ReserveRecordMemory) / ReserveRecordsCount
	cfg := &NewSegmentConfig{
		Dir:      TmpVdlDir,
		Name:     segmentFileName(0),
		MaxBytes: segmentSize,
		Mc:       NewMemCache(ReserveRecordMemory, msgSize),
		Meta:     meta,
		//Ic:            ic,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}

	vdlRecords := newRecords(100, 20, t)
	err = segment.WriteRecords(vdlRecords)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	//segment Close
	segment.Close()
	//open with write
	writeCfg := &OpenSegmentConfig{
		Dir:             TmpVdlDir,
		Name:            segmentFileName(0),
		MaxBytes:        segmentSize,
		OpenSegmentMode: ReadWriteMode,
		Range:           nil,
		Mc:              mc,
		RangeMetaFile:   NewRangeFile(TmpVdlDir),
	}
	writeSeg, err := OpenSegmentWithWrite(writeCfg)
	if err != nil {
		t.Fatalf("OpenSegmentWithRead error:%s", err.Error())
	}
	err = writeSeg.DeleteRecordsByRindex(113)
	if err != nil {
		t.Fatalf("OpenSegmentWithRead error:%s", err.Error())
	}
	if writeSeg.LastRindex != 112 {
		t.Fatalf("writeSeg.LastRindex is %d, not equal 12", writeSeg.LastRindex)
	}
	if writeSeg.LastVindex != 12 {
		t.Fatalf("writeSeg.LastVindex is %d, not equal 11", writeSeg.LastVindex)
	}
	if writeSeg.Mc.LastRindex != 0 {
		t.Fatalf("writeSeg.Mc.LastRindex is %d, not equal 12", 0)
	}
	//writeSeg.Remove()
}

func newRecords(start uint64, count int, t *testing.T) []*Record {
	records := make([]*Record, 0, count)
	for i := start; i < start+uint64(count); i++ {
		s := "hello,world"
		r := new(Record)
		r.RecordType = VdlLogType
		r.RaftType = raftpb.EntryNormal
		r.Rindex = i
		r.Vindex = int64(i)
		r.Term = 1
		r.Data = make([]byte, 8)
		binary.BigEndian.PutUint64(r.Data, i) //reqId equal Index
		r.Data = append(r.Data, []byte(s)...)
		r.DataLen = int64(len(s)) + 8
		r.Crc = generateRecordCrc(r)

		records = append(records, r)
	}
	return records
}

func newRaftRecords(start uint64, count int, t *testing.T) []*Record {
	records := make([]*Record, 0, count)
	for i := start; i < start+uint64(count); i++ {
		s := "hello,world"
		r := new(Record)
		r.RecordType = RaftLogType
		r.RaftType = raftpb.EntryConfChange
		r.Rindex = i
		r.Vindex = -1
		r.Term = 1
		r.Data = make([]byte, 8)
		binary.BigEndian.PutUint64(r.Data, i) //reqId equal Index
		r.Data = append(r.Data, []byte(s)...)
		r.DataLen = int64(len(s)) + 8
		r.Crc = generateRecordCrc(r)
		records = append(records, r)
	}
	return records
}

//29
func TestGetIndexEntriesWithLRU(t *testing.T) {
	InitTmpVdlDir(t)
	ReserveRecordsCount := 1024
	msgSize := int(ReserveRecordMemory) / ReserveRecordsCount
	cfg := &NewSegmentConfig{
		Dir:           TmpVdlDir,
		Name:          segmentFileName(0),
		MaxBytes:      segmentSize,
		Mc:            NewMemCache(ReserveRecordMemory, msgSize),
		Meta:          meta,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}
	totalRecords := make([]*Record, 0, 40)
	//vdl log
	vdlRecords := newRecords(1, 10, t)
	err = segment.WriteRecords(vdlRecords)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	totalRecords = append(totalRecords, vdlRecords...)
	//raft log
	raftRecords := newRaftRecords(11, 10, t)
	err = segment.WriteRecords(raftRecords)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	totalRecords = append(totalRecords, raftRecords...)
	//vdl log 2
	vdlRecords2 := newRecords(21, 10, t)
	err = segment.WriteRecords(vdlRecords2)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	totalRecords = append(totalRecords, vdlRecords2...)
	//vdl log
	raftRecords2 := newRaftRecords(31, 10, t)
	err = segment.WriteRecords(raftRecords2)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	totalRecords = append(totalRecords, raftRecords2...)
	//1-10:vdl log,11-20:raft log
	//21-30:vdl log,31-40:raft log
	//[1,41)
	indexEntries, err := segment.getIndexEntries(RaftIndexType, 1, 41, math.MaxUint64)
	if err != nil {
		t.Fatalf("getIndexEntries error:%s", err.Error())
	}
	if len(indexEntries) != 40 {
		t.Fatalf("len(indexEntries) is %d, not 40", len(indexEntries))
	}
	for i := 0; i < len(indexEntries); i++ {
		checkIndexEntry(indexEntries[i], totalRecords[i])
	}
	//[0,19)
	vdlIndexEntries, err := segment.getIndexEntries(VdlIndexType, 0, 20, math.MaxUint64)
	if err != nil {
		t.Fatalf("getIndexEntries error:%s", err.Error())
	}
	if len(vdlIndexEntries) != 20 {
		t.Fatalf("len(indexEntries) is %d, not 20", len(vdlIndexEntries))
	}
	for i := 0; i < 10; i++ {
		checkIndexEntry(vdlIndexEntries[i], totalRecords[i])
	}
	for i := 10; i < 20; i++ {
		checkIndexEntry(vdlIndexEntries[i], totalRecords[20+i])
	}
}

//30
func TestGetIndexEntriesFromFile(t *testing.T) {
	InitTmpVdlDir(t)
	ReserveRecordsCount := 1024
	msgSize := int(ReserveRecordMemory) / ReserveRecordsCount
	cfg := &NewSegmentConfig{
		Dir:           TmpVdlDir,
		Name:          segmentFileName(0),
		MaxBytes:      segmentSize,
		Mc:            NewMemCache(ReserveRecordMemory, msgSize),
		Meta:          meta,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}
	totalRecords := make([]*Record, 0, 40)
	//vdl log
	vdlRecords := newRecords(1, 10, t)
	err = segment.WriteRecords(vdlRecords)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	totalRecords = append(totalRecords, vdlRecords...)
	//raft log
	raftRecords := newRaftRecords(11, 10, t)
	err = segment.WriteRecords(raftRecords)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	totalRecords = append(totalRecords, raftRecords...)
	//vdl log 2
	vdlRecords2 := newRecords(21, 10, t)
	err = segment.WriteRecords(vdlRecords2)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	totalRecords = append(totalRecords, vdlRecords2...)
	//vdl log
	raftRecords2 := newRaftRecords(31, 10, t)
	err = segment.WriteRecords(raftRecords2)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	totalRecords = append(totalRecords, raftRecords2...)
	//1-10:vdl log,11-20:raft log
	//21-30:vdl log,31-40:raft log
	//[1,41)
	indexEntries, err := segment.getIndexEntriesFromFile(RaftIndexType, 1, 41, math.MaxUint64)
	if err != nil {
		t.Fatalf("getIndexEntries error:%s", err.Error())
	}
	if len(indexEntries) != 40 {
		t.Fatalf("len(indexEntries) is %d, not 40", len(indexEntries))
	}
	for i := 0; i < len(indexEntries); i++ {
		checkIndexEntry(indexEntries[i], totalRecords[i])
	}
	//[0,19)
	vdlIndexEntries, err := segment.getIndexEntriesFromFile(VdlIndexType, 0, 20, math.MaxUint64)
	if err != nil {
		t.Fatalf("getIndexEntries error:%s", err.Error())
	}
	if len(vdlIndexEntries) != 20 {
		t.Fatalf("len(indexEntries) is %d, not 20", len(vdlIndexEntries))
	}
	for i := 0; i < 10; i++ {
		checkIndexEntry(vdlIndexEntries[i], totalRecords[i])
	}
	for i := 10; i < 20; i++ {
		checkIndexEntry(vdlIndexEntries[i], totalRecords[20+i])
	}
}

//31
//test torn write in read_write index, and rebuild index
func TestRebuildWritingIndex(t *testing.T) {
	InitTmpVdlDir(t)
	ReserveRecordsCount := 1024
	msgSize := int(ReserveRecordMemory) / ReserveRecordsCount
	cfg := &NewSegmentConfig{
		Dir:           TmpVdlDir,
		Name:          segmentFileName(0),
		MaxBytes:      segmentSize,
		Mc:            NewMemCache(ReserveRecordMemory, msgSize),
		Meta:          meta,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}
	//vdl log
	vdlRecords := newRecords(1, 10, t)
	err = segment.WriteRecords(vdlRecords)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	//模拟损坏index文件
	newPos := segment.IndexFile.Position - 2
	segment.IndexFile.IndexFile.Truncate(newPos)
	segment.Close()
	//open with write
	writeCfg := &OpenSegmentConfig{
		Dir:             TmpVdlDir,
		Name:            segmentFileName(0),
		MaxBytes:        segmentSize,
		OpenSegmentMode: ReadWriteMode,
		Range:           nil,
		Mc:              mc,
		RangeMetaFile:   NewRangeFile(TmpVdlDir),
	}
	writeSeg, err := OpenSegment(writeCfg)
	if err != nil {
		t.Fatalf("OpenSegment error:%s", err.Error())
	}
	if writeSeg.FirstRindex != 1 || writeSeg.LastRindex != 10 {
		t.Fatalf("writeSeg.FirstRindex=%d,writeSeg.LastRindex=%d",
			writeSeg.FirstRindex, writeSeg.LastRindex)
	}
}

//32
//测试删除Segment中Record
func TestTruncateSegment(t *testing.T) {
	InitTmpVdlDir(t)
	ReserveRecordsCount := 1024
	msgSize := int(ReserveRecordMemory) / ReserveRecordsCount
	cfg := &NewSegmentConfig{
		Dir:           TmpVdlDir,
		Name:          segmentFileName(0),
		MaxBytes:      segmentSize,
		Mc:            NewMemCache(ReserveRecordMemory, msgSize),
		Meta:          meta,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}
	//vdl log
	vdlRecords := newRecords(1, 10, t)
	err = segment.WriteRecords(vdlRecords)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}
	//删除5及以后的全部raft log
	err = segment.DeleteRecordsByRindex(5)
	if err != nil {
		t.Fatalf("DeleteRecordsByRindex error,err=%s", err.Error())
	}
	//解析segment文件，并重建索引
	pr, err := segment.RebuildIndexFile()
	if err != nil {
		t.Fatalf("RebuildIndexFile error,err=%s", err.Error())
	}
	if len(pr.Entries) != 4 || pr.FirstRindex != 1 || pr.LastRindex != 4 ||
		pr.FirstVindex != 0 || pr.LastVindex != 3 {
		t.Fatalf("len(pr.Entries)=%d,pr.FirstRindex=%d, pr.LastRindex=%d,pr.FirstVindex=%d,pr.LastVindex=%d",
			len(pr.Entries), pr.FirstRindex, pr.LastRindex, pr.FirstVindex, pr.LastVindex)
	}
}

//47,测试读取rindex在segment和index中对应记录的结束位置
func TestGetReadSizeByRindex(t *testing.T) {
	InitTmpVdlDir(t)
	ReserveRecordsCount := 1024
	msgSize := int(ReserveRecordMemory) / ReserveRecordsCount
	cfg := &NewSegmentConfig{
		Dir:           TmpVdlDir,
		Name:          segmentFileName(0),
		MaxBytes:      segmentSize,
		Mc:            NewMemCache(ReserveRecordMemory, msgSize),
		Meta:          meta,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}

	//raft log:5-14
	vdlRecords := newRecords(5, 10, t)
	err = segment.WriteRecords(vdlRecords)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}

	//out of range
	_, _, err = segment.GetEndPositionByRindex(15)
	if err != ErrArgsNotAvailable {
		t.Fatalf("want err=%v,but err=%v", ErrArgsNotAvailable, err)
	}
	_, _, err = segment.GetEndPositionByRindex(4)
	if err != ErrArgsNotAvailable {
		t.Fatalf("want err=%v,but err=%v", ErrArgsNotAvailable, err)
	}

	//first one
	a, b, err := segment.GetEndPositionByRindex(5)
	if err != nil {
		t.Fatalf("want err=%v,but err=%v", ErrArgsNotAvailable, err)
	}
	//vdl metadata
	metaLen := int64(RecordHeaderSize + len("1.0"))
	recordLen := vdlRecords[0].DataLen + int64(RecordHeaderSize)
	indexLen := int64(IndexEntrySize)
	if a != metaLen+recordLen-1 || b != indexLen-1 {
		t.Fatalf("want a=%d,b=%d,but a=%d,b=%d", metaLen+recordLen-1, indexLen-1, a, b)
	}

	//last one
	a, b, err = segment.GetEndPositionByRindex(14)
	if err != nil {
		t.Fatalf("want err=%v,but err=%v", ErrArgsNotAvailable, err)
	}
	if a != metaLen+recordLen*10-1 || b != indexLen*10-1 {
		t.Fatalf("want a=%d,b=%d,but a=%d,b=%d", metaLen+recordLen*10-1, indexLen*10-1, a, b)
	}

	//middle one
	a, b, err = segment.GetEndPositionByRindex(9)
	if err != nil {
		t.Fatalf("want err=%v,but err=%v", ErrArgsNotAvailable, err)
	}
	if a != metaLen+recordLen*5-1 || b != indexLen*5-1 {
		t.Fatalf("want a=%d,b=%d,but a=%d,b=%d", metaLen+recordLen*5-1, indexLen*5-1, a, b)
	}
}

//48,获取rindex对应的最大vindex
func TestGetMaxVindexByRindex(t *testing.T) {
	InitTmpVdlDir(t)
	ReserveRecordsCount := 1024
	msgSize := int(ReserveRecordMemory) / ReserveRecordsCount
	cfg := &NewSegmentConfig{
		Dir:           TmpVdlDir,
		Name:          segmentFileName(0),
		MaxBytes:      segmentSize,
		Mc:            NewMemCache(ReserveRecordMemory, msgSize),
		Meta:          meta,
		RangeMetaFile: NewRangeFile(TmpVdlDir),
	}
	segment, err := NewSegment(cfg)
	if err != nil {
		t.Fatalf("NewSegment error:%s", err.Error())
	}

	//强行设置InitFirstVindex为100
	segment.InitFirstVindex = 100

	//new raft log
	//raft log:10-19
	raftRecords := newRaftRecords(10, 10, t)
	err = segment.WriteRecords(raftRecords)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}

	//raft log:20-29
	//vdl log:100-109
	vdlRecords := newRecords(20, 10, t)
	err = segment.WriteRecords(vdlRecords)
	if err != nil {
		t.Fatalf("writeRecords error:%s", err.Error())
	}

	//out of range(range is:10-29)
	_, err = segment.GetMaxVindexByRindex(9)
	if err != ErrArgsNotAvailable {
		t.Fatalf("1:want err=ErrArgsNotAvailable,but err=%v", err)
	}
	_, err = segment.GetMaxVindexByRindex(30)
	if err != ErrArgsNotAvailable {
		t.Fatalf("2:want err=ErrArgsNotAvailable,but err=%v", err)
	}

	//before first one
	a, err := segment.GetMaxVindexByRindex(15)
	if err != nil {
		t.Fatalf("3:GetMaxVindexByRindex err=%v", err)
	}
	if a != 99 {
		t.Fatalf("4:want a=99,but a=%d", a)
	}

	//first one
	a, err = segment.GetMaxVindexByRindex(20)
	if err != nil {
		t.Fatalf("3:GetMaxVindexByRindex err=%v", err)
	}
	if a != 100 {
		t.Fatalf("4:want a=0,but a=%d", a)
	}

	//last one
	a, err = segment.GetMaxVindexByRindex(29)
	if err != nil {
		t.Fatalf("3:GetMaxVindexByRindex err=%v", err)
	}
	if a != 109 {
		t.Fatalf("4:want a=0,but a=%d", a)
	}
}
