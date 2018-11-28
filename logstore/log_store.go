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
	"io/ioutil"
	"math"
	"os"
	"path"
	"strings"

	"sync"

	"sort"

	"time"

	"errors"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/vipshop/vdl/consensus/raft"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/logstream"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/vdlfiu"
	"gitlab.tools.vipshop.com/distributedstorage/fiu"
)

var (
	MaxSegmentSize           int64         = 512 * 1024 * 1024
	noLimit                  uint64        = math.MaxUint64
	noLimitVdlLog            int32         = math.MaxInt32
	nonexistentVindex        int64         = -1
	reserveSegmentCount      int           = 4
	maxSliceSize             uint64        = 10000
	delayDeleteFilesDuration time.Duration = 3 * time.Minute

	warningStoreTimeout = 100 //ms
	warningReadTimeout  = 100 //ms

	//追查写latency大的metrics信息
	getLastSegDuration time.Duration
	copyDuration       time.Duration
	syncDuration       time.Duration
	writeIndexDuration time.Duration
	writeMemDuration   time.Duration
	//rolling segment
	rollingSegmentDuration time.Duration

	//追查读latency大的metrics信息
	readSegmentDuration       time.Duration
	getIndexDuration          time.Duration //getIndexDuration 包含 readLRUDuration,readIndexFileDuration
	readIndexFileDuration     time.Duration
	GetRecordDuration         time.Duration
	checkIndexDuration        time.Duration
	returnIesFromFileDuration time.Duration
	returnIesFromFileBegin    time.Time
)

type LogStore struct {
	Dir                 string    // the living directory of the underlay files
	Meta                *FileMeta // metadata recorded at the head of each WAL
	SegmentSizeBytes    int64     //the max size of segment file
	ReserveSegmentCount int
	IsNew               bool

	Mu       sync.RWMutex
	Segments []*Segment //log segment files,the last segment is for write only, others for read only

	Mc            *MemCache //memory cache for The latest piece of records which including all the records in last segment
	RangeMetaFile *RangeFile
	ExitFlagFile  *FlagFile
}

type LogStoreConfig struct {
	Dir                 string
	Meta                *FileMeta //the segment metadata
	SegmentSizeBytes    int64
	MaxSizePerMsg       int
	MemCacheSizeByte    uint64
	ReserveSegmentCount int
}

type FileMeta struct {
	VdlVersion string
}

//创建logstore
func NewLogStore(cfg *LogStoreConfig) (*LogStore, error) {
	var logStore *LogStore
	var err error

	if cfg.SegmentSizeBytes <= 0 || len(cfg.Dir) == 0 || len(cfg.Meta.VdlVersion) == 0 {
		return nil, ErrArgsNotAvailable
	}
	dir := path.Clean(cfg.Dir)
	if existFile(dir) {
		logStore, err = newLogStoreWithOpenSegments(cfg)
		if err != nil {
			return nil, err
		}
	} else {
		//if not exist files
		logStore, err = newLogStoreWithNewSegment(cfg)
		if err != nil {
			return nil, err
		}
	}

	return logStore, nil
}

func newLogStoreWithOpenSegments(cfg *LogStoreConfig) (*LogStore, error) {
	var err error

	store := new(LogStore)
	store.Dir = path.Clean(cfg.Dir)

	err = CheckAndRestoreDataDir(store.Dir)
	if err != nil {
		glog.Fatalf("[log_store.go-newLogStoreWithOpenSegments]:CheckAndRestoreDataDir error:%s,dir=%s",
			err.Error(), store.Dir)
	}

	store.Meta = cfg.Meta
	store.SegmentSizeBytes = cfg.SegmentSizeBytes
	//最少保留4个segment文件
	if cfg.ReserveSegmentCount < reserveSegmentCount {
		glog.Fatalf("[log_store.go-newLogStoreWithOpenSegments]:cfg.ReserveSegmentCount[%d] is too little,must"+
			"large than %d", cfg.ReserveSegmentCount, reserveSegmentCount)
	}
	store.ReserveSegmentCount = cfg.ReserveSegmentCount

	//new memory cache
	store.Mc = NewMemCache(cfg.MemCacheSizeByte, cfg.MaxSizePerMsg)
	store.RangeMetaFile = NewRangeFile(store.Dir)
	store.ExitFlagFile = NewFlagFile(store.Dir)

	openSegmentsCfg := new(OpenSegmentsConfig)
	openSegmentsCfg.Dir = store.Dir
	openSegmentsCfg.MaxBytes = store.SegmentSizeBytes
	openSegmentsCfg.Mc = store.Mc
	openSegmentsCfg.Meta = store.Meta
	openSegmentsCfg.RangeMetaFile = store.RangeMetaFile
	openSegmentsCfg.Ranges, err = store.RangeMetaFile.GetRangeInfo()
	if err != nil {
		glog.Errorf("[log_store.go-newLogStoreWithOpenSegments]:GetRangeInfo error:err=%s", err.Error())
		return nil, err
	}
	openSegmentsCfg.NeedRecover = store.ExitFlagFile.NeedRecover()

	store.Segments, err = OpenSegments(openSegmentsCfg)
	if err != nil {
		glog.Errorf("[log_store.go-newLogStoreWithOpenSegments]:openSegments in newLogStoreWithOpenSegments error,err=%s", err.Error())
		return nil, err
	}

	//如果最后一个segment不包含数据，设置InitFirstVindex
	lastSegment := store.Segments[len(store.Segments)-1]
	//if the last segment exactly has no records, using the previous segment LastVindex set the InitFirstVindex
	if lastSegment.GetFirstVindex() == -1 {
		maxVindex := getMaxVindex(store.Segments)
		lastSegment.InitFirstVindex = maxVindex + 1
	}
	store.IsNew = false
	//写入非正常退出标识
	store.ExitFlagFile.WriteExitFlag(UnnormalExitFlag)
	glog.Infof("create log store success")
	//debug
	if glog.V(1) {
		glog.Infof("D:s.FirstVindex=%d,s.LastVindex=%d,s.FirstRindex=%d,s.LastRindex=%d",
			store.GetFirstSegment().GetFirstVindex(),
			store.GetLastSegment().GetLastVindex(),
			store.GetFirstSegment().GetFirstRindex(),
			store.GetLastSegment().GetLastRindex())
	}
	return store, nil
}

//issue #119
//检查数据目录下的segment,index和range的一致性,并删除多余的信息或文件
func CheckAndRestoreDataDir(dir string) error {
	segmentNameMap := make(map[string]bool)
	segmentNameSlice := make([]string, 0, 128)
	indexNames := make([]string, 0, 128)

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		glog.Errorf("[log_store.go-CheckAndRestoreDataDir]:read dir error in CheckAndRestoreDataDir,err=%s", err.Error())
		return err
	}

	for i := 0; i < len(files); i++ {
		//构造segment文件列表
		if strings.HasSuffix(files[i].Name(), LogFileSuffix) {
			segmentName := files[i].Name()
			segmentNameMap[segmentName] = true
			segmentNameSlice = append(segmentNameSlice, segmentName)
		}
		//构造index文件列表
		if strings.HasSuffix(files[i].Name(), IndexFileSuffix) {
			indexNames = append(indexNames, files[i].Name())
		}
	}

	//检查segment文件列表的连续性
	for i := 0; i < len(segmentNameSlice); i++ {
		if 0 < i {
			nextNum, err := parseSegmentName(segmentNameSlice[i])
			if err != nil {
				glog.Errorf("[log_store.go-CheckAndRestoreDataDir]:parseSegmentName1 error,err=%s,fileName=%s",
					err.Error(), segmentNameSlice[i])
				return err
			}
			prevNum, err := parseSegmentName(segmentNameSlice[i-1])
			if err != nil {
				glog.Errorf("[log_store.go-CheckAndRestoreDataDir]:parseSegmentName2 error,err=%s,fileName=%s",
					err.Error(), segmentNameSlice[i-1])
				return err
			}
			if prevNum+1 != nextNum {
				glog.Errorf("[log_store.go-CheckAndRestoreDataDir]:segment number is not continuous,pre=%d,next=%d",
					prevNum, nextNum)
				return ErrNotContinuous
			}
		}
	}

	//删除不在segment列表中的index文件
	for i := 0; i < len(indexNames); i++ {
		segmentName := getSegmentByIndexName(indexNames[i])
		if _, exist := segmentNameMap[segmentName]; exist == false {
			//remove index file
			deleteIndexPath := path.Join(dir, indexNames[i])
			err := os.Remove(deleteIndexPath)
			if err != nil {
				glog.Errorf("[log_store.go-CheckAndRestoreDataDir]:remove redundant index file error,err=%s,index_file_name=%s",
					err.Error(), indexNames[i])
				return err
			}
			glog.Infof("[log_store.go-CheckAndRestoreDataDir]:remove redundant index file,index_file_name=%s", indexNames[i])
		}
	}

	//构造range信息，删除不在segment列表中的范围元信息
	rangeFile := NewRangeFile(dir)
	rangeInfo, err := rangeFile.GetRangeInfo()
	if err != nil {
		glog.Errorf("[log_store.go-CheckAndRestoreDataDir]:GetRangeInfo error,err=%s,filePath=%s",
			err.Error(), rangeFile.rangeFilePath)
		return err
	}
	for name, _ := range rangeInfo.NameToRange {
		if _, exist := segmentNameMap[name]; exist == false {
			segments := []string{name}
			err := rangeFile.DeleteLogRanges(segments)
			if err != nil {
				glog.Errorf("[log_store.go-CheckAndRestoreDataDir]:DeleteLogRanges error,err=%s,segments=%v,rangeFile=%s",
					err.Error(), segments, rangeFile.rangeFilePath)
				return err
			}
		}
	}

	glog.Infof("[log_store.go-CheckAndRestoreDataDir]: check data dir success")
	return nil
}

func newLogStoreWithNewSegment(cfg *LogStoreConfig) (*LogStore, error) {
	var err error
	store := new(LogStore)
	store.Dir = path.Clean(cfg.Dir)
	store.Meta = cfg.Meta
	store.SegmentSizeBytes = cfg.SegmentSizeBytes
	//最少保留4个segment
	if cfg.ReserveSegmentCount < reserveSegmentCount {
		glog.Fatalf("[log_store.go-newLogStoreWithNewSegment]:cfg.ReserveSegmentCount[%d] is too little,must"+
			"large than %d", cfg.ReserveSegmentCount, reserveSegmentCount)
	}
	store.ReserveSegmentCount = cfg.ReserveSegmentCount

	//new memory cache
	store.Mc = NewMemCache(cfg.MemCacheSizeByte, cfg.MaxSizePerMsg)
	store.RangeMetaFile = NewRangeFile(store.Dir)
	//Remove path
	err = os.RemoveAll(store.Dir)
	if err != nil {
		glog.Errorf("RemoveAll error in newLogStoreWithNewSegment,err=%s", err.Error())
		return nil, err
	}
	err = os.MkdirAll(cfg.Dir, fileutil.PrivateDirMode)
	if err != nil {
		glog.Errorf("[log_store.go-NewLogStore]:MkdirAll error in NewLogStore,err=%s", err.Error())
		return nil, err
	}
	segmentCfg := &NewSegmentConfig{
		Dir:           store.Dir,
		Name:          segmentFileName(0),
		MaxBytes:      store.SegmentSizeBytes,
		Mc:            store.Mc,
		Meta:          store.Meta,
		RangeMetaFile: store.RangeMetaFile,
	}
	segment, err := NewSegment(segmentCfg)
	if err != nil {
		glog.Errorf("[log_store.go-newLogStoreWithNewSegment]:NewSegment error in newLogStoreWithNewSegment,err=%s", err.Error())
		return nil, err
	}
	store.Segments = append(store.Segments, segment)
	store.IsNew = true

	store.ExitFlagFile = NewFlagFile(store.Dir)
	//写入非正常退出标识
	store.ExitFlagFile.WriteExitFlag(UnnormalExitFlag)
	glog.Infof("[log_store.go-newLogStoreWithNewSegment]:create log store success")
	return store, nil
}

//根据rindex获取vindex
func (s *LogStore) GetVindexByRindex(rindex uint64) (int64, error) {

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStoreGetVindexByRindexError) {
		return 0, errors.New("Mock FiuStoreGetVindexByRindexError")
	}

	var i int
	var targetSegment *Segment
	//判断rindex的有效性
	firstRindex, err := s.FirstIndex()
	if err != nil {
		return -1, err
	}
	lastRindex, err := s.LastIndex()
	if err != nil {
		return -1, err
	}
	if rindex < firstRindex || lastRindex < rindex {
		return -1, ErrEntryNotExist
	}

	//找到rindex对应的segment
	s.Mu.RLock()
	for i = len(s.Segments) - 1; 0 <= i; i-- {
		if s.Segments[i].GetFirstRindex() <= rindex && rindex <= s.Segments[i].GetLastRindex() {
			break
		}
	}
	if i < 0 {
		s.Mu.RUnlock()
		return -1, ErrEntryNotExist
	}
	targetSegment = s.Segments[i]
	s.Mu.RUnlock()

	//在对应的segment内查找rindex对应的vindex
	return targetSegment.GetVindexByRindex(rindex)
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (s *LogStore) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStoreEntriesError) {
		return nil, errors.New("Mock FiuStoreEntriesError")
	}

	//FIU test[1],模拟调用超时，超过election-timeout(2s)
	if fiu.IsSyncPointExist(vdlfiu.FiuEntriesTimeout) {
		glog.Warningf("in IsSyncPointExist Sleep 3 second")
		time.Sleep(time.Second * 3)
	}
	var raftLogs []raftpb.Entry
	//对lo,hi进行有效性检查
	err := s.perCheckForEntries(lo, hi)
	if err != nil {
		return nil, err
	}

	// read entries
	// NOTE: 在ReadRaftLogByRindex中发现lo不存在，返回ErrCompacted
	readStart := time.Now()
	raftLogs, err = s.ReadRaftLogByRindex(lo, hi, maxSize)
	if err != nil {
		glog.Errorf("[log_store.go-Entries]:ReadRecordsByRindex error,lo=%d,hi=%d,err=%s",
			lo, hi, err.Error())
		return nil, err
	}
	if len(raftLogs) == 0 {
		glog.Warningf("[log_store.go-Entries]:no raft log in index[%d,%d),maxSize=%d", lo, hi, maxSize)
		return nil, raft.ErrUnavailable
	}
	raftLogs = s.limitRaftLogSize(raftLogs, maxSize)

	readDuration := time.Now().Sub(readStart)
	LogstoreReadTps.Add(float64(len(raftLogs)))
	LogstoreReadLatency.Observe(float64(readDuration / time.Millisecond))
	//超过warningReadTimeout(ms)输出日志
	if readDuration > time.Duration(warningReadTimeout)*time.Millisecond {
		glog.Warningf("[log_store.go-Entries]:ReadRaftLogByRindex cost %v,entries count is %d, maxSize is %d Byte,"+
			"readSegmentDuration=%v,getIndexDuration=%v,readIndexFileDuration=%v,GetRecordDuration=%v",
			readDuration, len(raftLogs), maxSize,
			readSegmentDuration, getIndexDuration, readIndexFileDuration, GetRecordDuration)
	}

	//检查raft log连续性
	for i := 0; i < len(raftLogs)-1; i++ {
		if raftLogs[i].Index+1 != raftLogs[i+1].Index {
			for j := 0; j < len(raftLogs); j++ {
				glog.Infof("[log_store.go-Entries]:j=%d,raftlog[j].Term=%d,raftlog[j].Index=%d,"+
					"raftlog[j].Type=%d,raftlog[j].Data=%v", j, raftLogs[j].Term, raftLogs[j].Index,
					raftLogs[j].Type, raftLogs[j].Data)
			}
			glog.Fatalf("[log_store.go-Entries]:read raft log from logstore is not continuous,i=%d", i)
		}
	}
	//定位#78,#80问题的临时日志
	//returnIesFromFileDuration有问题
	if hi-lo != uint64(len(raftLogs)) {
		glog.Infof("[log_store.go-Entries]:lo=%d,hi=%d,len(raftLogs)=%d,maxSize=%d,"+
			"readDuration=%v,readSegmentDuration=%v,getIndexDuration=%v,"+
			"readIndexFileDuration=%v,GetRecordDuration=%v,checkIndexDuration=%v,returnIesFromFileDuration=%v",
			lo, hi, len(raftLogs), maxSize, readDuration, readSegmentDuration, getIndexDuration,
			readIndexFileDuration, GetRecordDuration, checkIndexDuration, returnIesFromFileDuration)
	}

	return raftLogs, nil
}

func (s *LogStore) limitRaftLogSize(ents []raftpb.Entry, maxSize uint64) []raftpb.Entry {
	if len(ents) == 0 {
		return ents
	}
	size := len(ents[0].Data)
	var limit int
	for limit = 1; limit < len(ents); limit++ {
		size += len(ents[limit].Data)
		if uint64(size) > maxSize {
			break
		}
	}
	return ents[:limit]
}

func (s *LogStore) limitLogStreamEntrySize(ents []logstream.Entry, maxSize int32) []logstream.Entry {
	if len(ents) == 0 {
		return ents
	}
	size := len(ents[0].Data)
	var limit int
	for limit = 1; limit < len(ents); limit++ {
		size += len(ents[limit].Data)
		if int32(size) > maxSize {
			break
		}
	}
	return ents[:limit]
}

// per check for Entries() method
func (s *LogStore) perCheckForEntries(lo, hi uint64) error {

	firstRindex, err := s.FirstIndex()
	if err != nil {
		return err
	}
	lastRindex, err := s.LastIndex()
	if err != nil {
		return err
	}

	if lo < firstRindex {
		return raft.ErrCompacted
	}
	//raft lib上层调用保证了hi的正确性，如果hi超出范围，说明存在BUG
	if hi > lastRindex+1 {
		glog.Fatalf("[log_store.go-Entries]:entries' hi(%d) is out of bound lastindex(%d)",
			hi, lastRindex)
	}
	return nil
}

//仅用于测试使用，勿用于生产环境
//only for performance test,don't use in production
func (s *LogStore) GetEntriesInMemCache(count int) ([]raftpb.Entry, error) {
	records := s.Mc.GetRecords(count)
	if len(records) == 0 {
		return nil, raft.ErrUnavailable
	}
	raftLogs := RecordsToRaftLog(records, math.MaxUint64)
	return raftLogs, nil
}

// raft lib调用StoreEntries存储entries到logstore中
func (s *LogStore) StoreEntries(entries []raftpb.Entry) error {

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStoreStoreEntriesError) {
		return errors.New("Mock FiuStoreStoreEntriesError")
	}

	//Fiu test[2],模拟调用超时，超过election-timeout(2s)
	if fiu.IsSyncPointExist(vdlfiu.FiuLeaderStoreEntries) {
		glog.Warningf("fiu test in StoreEntries,sleep 3 second")
		time.Sleep(time.Second * 3)
	}

	//FIU test[13],模拟Follower调用该函数随机超时2s-4s
	if fiu.IsSyncPointExist(vdlfiu.FiuFollowerStoreEntries) {
		vdlfiu.SleepRandom(2000, 4000)
	}

	if len(entries) == 0 {
		return nil
	}

	//根据entries生成Record slice
	records, recordSize := s.generateRecords(entries)
	if s.SegmentSizeBytes < recordSize {
		glog.Fatalf("[log_store.go-StoreEntries]:recordSize[%d] is large than s.SegmentSizeBytes[%d]", recordSize, s.SegmentSizeBytes)
	}
	writeStart := time.Now()
	err := s.WriteRecords(records, recordSize)
	if err != nil {
		return err
	}
	storeDuration := time.Now().Sub(writeStart)
	//metric
	LogstoreWriteTps.Add(float64(len(records)))
	LogstoreWriteLatency.Observe(float64(storeDuration / time.Millisecond))
	//WriteRecords调用超过warningStoreTimeout (ms)，输出相关时间监控信息
	if storeDuration > time.Duration(warningStoreTimeout)*time.Millisecond {
		glog.Warningf("[log_store.go-StoreEntries]:"+
			"store records cost %v,records count is %d, record size is %d Byte,"+
			"getLastSegment cost %v,cutSegmentDuration cost %v,Memcopy cost %v,syncFileRange cost %v,writeIndex cost %v,writeMemCache cost %v",
			storeDuration, len(records), recordSize,
			getLastSegDuration, rollingSegmentDuration, copyDuration, syncDuration, writeIndexDuration, writeMemDuration)
	}

	return nil
}

//根据entries生成Record slice
func (s *LogStore) generateRecords(entries []raftpb.Entry) ([]*Record, int64) {

	records := make([]*Record, 0, len(entries))
	var recordSize int64 = 0

	for i := 0; i < len(entries); i++ {
		//check the continuity of raft log
		if i < len(entries)-1 && entries[i].Index+1 != entries[i+1].Index {
			for i := 0; i < len(entries); i++ {
				glog.Infof("[log_store.go-StoreEntries]:i:%d,entries[i].Term=%d,entries[i].Index=%d,entries[i].Type=%d,entries[i].Data=%v",
					i, entries[i].Term, entries[i].Index, entries[i].Type, entries[i].Data)
			}
			glog.Fatalf("[log_store.go-StoreEntries]:raft logs is not continuous,entries[%d].Index=%d,entries[%d].Index=%d，"+
				"len(entries)=%d",
				i, entries[i].Index, i+1, entries[i+1].Index, len(entries))
		}
		record := new(Record)
		if entries[i].Type == raftpb.EntryNormal && 0 < len(entries[i].Data) {
			record.RecordType = VdlLogType
		} else {
			record.RecordType = RaftLogType
		}
		record.Vindex = -1 //set in segment
		record.DataLen = int64(len(entries[i].Data))
		record.Term = entries[i].Term
		record.Rindex = entries[i].Index
		record.RaftType = entries[i].Type
		record.Data = entries[i].Data
		records = append(records, record)
		recordSize += RecordHeaderSize + record.DataLen
	}

	return records, recordSize
}

//根据index(i)获取对应的Term
func (s *LogStore) Term(i uint64) (uint64, error) {

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStoreTermError) {
		return 0, errors.New("Mock FiuStoreTermError")
	}

	if i == 0 {
		return 0, nil
	}
	//检查有效性
	firstRindex, err := s.FirstIndex()
	if err != nil {
		return 0, err
	}
	lastRindex, err := s.LastIndex()
	if err != nil {
		return 0, err
	}
	if i < firstRindex {
		return 0, raft.ErrCompacted
	}

	//i处于Logstore范围内时，调用Entries获取i对应的raft entry
	if firstRindex <= i && i <= lastRindex {
		entries, err := s.Entries(i, i+1, noLimit)
		if err != nil {
			return 0, err
		}
		if len(entries) == 0 {
			glog.Errorf("[log_store.go-Term]: get term by index %d occur ErrUnavailable", i)
			return 0, raft.ErrUnavailable
		} else {
			return entries[0].Term, nil
		}
	}
	glog.Errorf("[log_store.go-Term]: get term by index %d occur ErrUnavailable, "+
		"not in (firstRindex <= i && i <= lastRindex), firstRindex:%v,  lastRindex:%v", i, firstRindex, lastRindex)
	return 0, raft.ErrUnavailable
}

//获取不超过maxRindex，对应的最大vindex
func (s *LogStore) MaxVindex(maxRindex uint64) (int64, error) {

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStoreStoreMaxVindexError) {
		return 0, errors.New("Mock FiuStoreStoreMaxVindexError")
	}

	var vindex int64
	var err error
	vindex = -1
	//根据rindex，从后往前找，找到第一个不为-1的vindex，则返回
	for ; 0 < maxRindex; maxRindex-- {
		vindex, err = s.GetVindexByRindex(maxRindex)
		if err != nil {
			glog.Fatalf("[log_store.go-MaxVindex]:GetVindexByRindex error,error=%s,maxRindex=%d,vindex=%d",
				err.Error(), maxRindex, vindex)
		}
		if 0 <= vindex {
			break
		}
	}
	return vindex, nil
}

//kafka接口调用该函数获取vdl log
//startVindex:表示获取vdl log的开始位置
//endRindex:表示获取vdl log的最大rindex，小于等于该值
//maxBytes:该次获取的vdl log总大小不能超过maxBytes，如果第一条vdl log就超过了maxBytes，则返回第一条
//bool return whether read from cache
func (s *LogStore) FetchLogStreamMessages(startVindex int64, endRindex uint64, maxBytes int32) ([]logstream.Entry, error, bool) {
	//FIU test[17],模拟Follower调用该函数随机超时1s-3s
	if fiu.IsSyncPointExist(vdlfiu.FiuFetchVdlLog) {
		vdlfiu.SleepRandom(1000, 3000)
	}

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStoreStoreFetchStreamMsgError) {
		return nil, errors.New("Mock FiuStoreStoreFetchStreamMsgError"), false
	}

	firstVindex, err := s.FirstVindex()
	if err != nil {
		glog.Errorf("[log_store.go-FetchLogStreamMessages]:call FirstVindex err,err=%s",
			err.Error())
		return nil, err, false
	}
	//maxVindex is -1, means no vdl log
	maxVindex, err := s.MaxVindex(endRindex)
	if err != nil {
		glog.Errorf("[log_store.go-FetchLogStreamMessages]:call MaxVindex err,err=%s,rindex=%s",
			err.Error(), endRindex)
		return nil, err, false
	}
	//以下情况返回ErrOutOfRange:
	//startVindex 为-1
	//如果开始消费的位置小于logstore中最小的偏移
	//如果开始消费位置大于logstore中最大的vindex的下一个
	if startVindex == nonexistentVindex || startVindex < firstVindex || startVindex > maxVindex+1 {
		return nil, ErrOutOfRange, false
	}
	//如果logstore不存在数据，或者消费位置为最后一个vindex的下一个，返回空
	if maxVindex == nonexistentVindex || startVindex == maxVindex+1 {
		return nil, nil, false
	}

	//读取vdl log
	readStart := time.Now()
	vdlLogs, err, isReadFromCache := s.ReadRecordsByVindex(startVindex, endRindex, maxBytes)
	if err != nil {
		glog.Errorf("[log_store.go-FetchLogStreamMessages]:ReadRecordsByVindex error,err=%s,startVindex=%d,"+
			"endRindex=%d,maxBytes=%d", err.Error(), startVindex, endRindex, maxBytes)
		return nil, err, isReadFromCache
	}
	vdlLogs = s.limitLogStreamEntrySize(vdlLogs, maxBytes)

	readDuration := time.Now().Sub(readStart)
	LogstoreReadTps.Add(float64(len(vdlLogs)))
	LogstoreReadLatency.Observe(float64(readDuration / time.Millisecond))
	//读取时间超过warningReadTimeout (ms),则输出相关统计信息
	if readDuration > time.Duration(warningReadTimeout)*time.Millisecond {
		glog.Warningf("[log_store.go-Entries]:ReadRecordsByVindex cost %v,entries count is %d, maxSize is %d Byte",
			readDuration, len(vdlLogs), maxBytes)
	}
	return vdlLogs, nil, isReadFromCache
}

//输出segment文件和对应的index 文件
//只能从开始位置的segment开始顺序删除
func (s *LogStore) DeleteFiles(segmentNames []string) error {
	if len(segmentNames) == 0 {
		return nil
	}
	for i := 0; i < len(segmentNames); i++ {
		if len(segmentNames[i]) == 0 {
			return ErrArgsNotAvailable
		}
	}
	count := s.SegmentCount()
	// logstore at least keep s.ReserveSegmentCount segments, do not allow to delete
	if count <= s.ReserveSegmentCount {
		glog.Errorf("[log_store.go-DeleteFiles]:log store at least keep %d segments,now logstore has %d segments",
			s.ReserveSegmentCount, s.SegmentCount())
		return ErrNotAllowDelete
	}
	sort.Strings(segmentNames)

	// 获取待删除的segment，并从logstore的segments数组中移除
	// 注:
	// 这里先从segments数组中移除, 然后再延时物理删除segment
	// 从segments数组中移除后, 这些数据对于后续的请求不再可见。
	// 延时物理删除Segment则保证正在处理的请求,还能正常处理,
	// 我们认为,当延时时间的窗口过后,再不会有请求读到要删除的文件。
	s.Mu.Lock()
	for i := 0; i < len(segmentNames); i++ {
		if segmentNames[i] != s.Segments[i].GetName() {
			glog.Errorf("[log_store.go-DeleteFiles]:delete segments not in order,s.Segments[%d].Name=%s,segmentNames[%d]=%s",
				i, s.Segments[i].GetName(), i, segmentNames[i])
			s.Mu.Unlock()
			return ErrArgsNotAvailable
		}
	}
	deleteSegments := s.Segments[:len(segmentNames)]
	s.Segments = s.Segments[len(segmentNames):]
	s.Mu.Unlock()

	go s.doDeleteFilesWithDelay(deleteSegments, segmentNames, delayDeleteFilesDuration)

	return nil
}

func (s *LogStore) doDeleteFilesWithDelay(deleteSegments []*Segment, segmentNames []string, d time.Duration) {

	<-time.After(d)

	//同步方式删除segment文件和range metadata
	deleteStart := time.Now()
	//sync delete
	for i := 0; i < len(deleteSegments); i++ {
		err := deleteSegments[i].Remove()
		if err != nil {
			glog.Fatalf("[log_store.go-DeleteFiles]:Remove segment error,err=%s,segmentName=%s",
				err.Error(), deleteSegments[i].Log.Name)
		}
	}
	err := s.RangeMetaFile.DeleteLogRanges(segmentNames)
	if err != nil {
		glog.Errorf("[log_store.go-DeleteFiles]:DeleteLogRange error:err=%s,segmentNames=%v", err.Error(), segmentNames)
	}
	LogstoreDeleteSegmentLatency.Observe(float64(time.Now().Sub(deleteStart) / time.Millisecond))
}

//获取[start,end)范围的raft entry，并且总大小不超过maxSize。
//范围和大小两个限制，有一个突破，则立即返回
//如果第一条raft entry大小就超过maxSize,则返回第一条raft entry
func (s *LogStore) ReadRaftLogByRindex(start, end uint64, maxSize uint64) ([]raftpb.Entry, error) {
	var raftLogs []raftpb.Entry
	var err error
	var exist bool

	//从MemCache中读取
	raftLogs, exist = s.Mc.GetRaftLogByRindex(start, end, maxSize)
	if exist {
		return raftLogs, nil
	}

	startReadSegment := time.Now()
	//如果不在MemCache中，从文件中读
	raftLogs, err = s.ReadRaftLogFromSegments(start, end, maxSize)
	if err != nil {
		glog.Errorf("[logstore.go-ReadRaftLogByRindex]:ReadRaftLogFromSegments error,start=%d,end=%d,err=%s",
			start, end, err.Error())
		return nil, err
	}
	readSegmentDuration = time.Now().Sub(startReadSegment)
	//metric
	SegmentReadTps.Add(float64(len(raftLogs)))
	return raftLogs, nil
}

//根据vindex获取对应的Entry
//startVindex:对应的vindex开始位置
//endRindex:获取Entry的rindex，不超过endRindex，小于等于该值
//maxSize:获取的Entry总大小不超过该值
//范围和大小两个限制，有一个突破，则立即返回
//如果第一条entry大小就超过maxSize,则返回第一条entry
//bool return whether read from cache
func (s *LogStore) ReadRecordsByVindex(startVindex int64, endRindex uint64,
	maxSize int32) ([]logstream.Entry, error, bool) {

	var vdlLogs []logstream.Entry
	var err error
	var exist bool
	//从MemCache中读取
	vdlLogs, exist = s.Mc.GetVdlLogByVindex(startVindex, endRindex, maxSize)
	if exist {
		return vdlLogs, nil, true
	}
	//不在MemCache中，从文件中读取
	vdlLogs, err = s.ReadVdlLogFromSegments(startVindex, endRindex, maxSize)
	if err != nil {
		glog.Errorf("[logstore.go-ReadRecordsByVindex]:ReadVdlLogFromSegments error,"+
			"startVindex=%d,endRindex=%d,maxSize=%d,err=%s",
			startVindex, endRindex, maxSize, err.Error())
		return nil, err, false
	}
	SegmentReadTps.Add(float64(len(vdlLogs)))
	return vdlLogs, nil, false
}

//logstore调用该函数写Record
//recordSize为records的总大小
func (s *LogStore) WriteRecords(records []*Record, recordSize int64) error {
	start := time.Now()
	writingSegment := s.GetLastSegment()
	lastSegment := writingSegment
	isNewSegment := false
	getLastSegDuration = time.Now().Sub(start)

	//如果最后一个Segment空间不足，创建新的segment，写入数据，并加入segment数组
	if writingSegment.isFull(recordSize) {

		writingSegment = s.rollingSegment(writingSegment, records)
		isNewSegment = true
		// metrics
		SegmentCutCounter.Inc()
		rollingSegmentDuration = time.Now().Sub(start) - getLastSegDuration

	}
	err := writingSegment.WriteRecords(records)
	if err != nil {
		return err
	}

	if isNewSegment {
		//对logstore加写锁，进行以下步骤：
		//1.改变最后一个Segment的读写状态
		//2.将包含数据的新segment添加进logstore中
		s.Mu.Lock()
		lastSegment.SetReadOnly()
		s.Segments = append(s.Segments, writingSegment)
		s.Mu.Unlock()
	}
	return nil
}

//从文件读取，根据vindex获取对应的Entry
//startVindex:对应的vindex开始位置
//endRindex:获取Entry的rindex，不超过endRindex，小于等于该值
//maxSize:获取的Entry总大小不超过该值
//范围和大小两个限制，有一个突破，则立即返回
//如果第一条entry大小就超过maxSize,则返回第一条entry
func (s *LogStore) ReadVdlLogFromSegments(startVindex int64, endRindex uint64,
	maxSize int32) ([]logstream.Entry, error) {

	//var targetSegment *Segment
	totalVdlLogs := make([]logstream.Entry, 0, 100)

	for {

		//根据startVindex找到对应的segment
		targetSegment, isLastSegment := s.GetStartSegmentByVindex(startVindex)
		//没有找到对应的segment
		if targetSegment == nil {
			return nil, ErrOutOfRange
		}

		//找到startVindex对应的Segment，然后从该Segment读取数据（有可能是部分数据）
		//hasRead := len(totalVdlLogs)
		vdlResult, err := targetSegment.ReadVdlLogsByVindex(startVindex, endRindex, maxSize)
		if err != nil {
			glog.Errorf("[log_store.go-ReadVdlLogFromSegments]:ReadVdlLogsByVindex error,err=%s,"+
				"startVindex=%d,endRindex=%d,maxSize=%d",
				err.Error(), startVindex, endRindex, maxSize)
			return nil, err
		}

		totalVdlLogs = append(totalVdlLogs, vdlResult.entries...)

		//在下面两种情况下，需要返回：
		//读到满足要求的日志了
		//或者没有完全读够，但已经到了最后的segment，没有日志可读了
		if vdlResult.nextVindex == -1 || isLastSegment {
			return totalVdlLogs, nil
		}
		//read in next segment
		if 0 < vdlResult.nextVindex {
			startVindex = vdlResult.nextVindex
			maxSize = vdlResult.leftSize
		}
	}

	glog.Fatalf("[log_store.go-ReadVdlLogFromSegments]:can't be here,startVindex=%d,"+
		"endRindex=%d,maxSize=%d", startVindex, endRindex, maxSize)
	return nil, nil
}

//根据startVindex获得对应的segment结构，同时返回改segment是否是最后一个segment
func (s *LogStore) GetStartSegmentByVindex(startVindex int64) (*Segment, bool) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	var targetSegment *Segment = nil
	var found = false
	var isLastSegment = false

	for i := len(s.Segments) - 1; 0 <= i; i-- {
		firstVindex := s.Segments[i].GetFirstVindex()
		lastVindex := s.Segments[i].GetLastVindex()
		if firstVindex <= startVindex && startVindex <= lastVindex {
			found = true
			if i == len(s.Segments)-1 {
				isLastSegment = true
			}
			targetSegment = s.Segments[i]
			break
		}
	}
	if found == false {
		return nil, false
	}
	return targetSegment, isLastSegment
}

//从文件读取，获取[start,end)范围的raft entry，并且总大小不超过maxSize。
//范围和大小两个限制，有一个突破，则立即返回
//如果第一条raft entry大小就超过maxSize,则返回第一条raft entry
func (s *LogStore) ReadRaftLogFromSegments(start, end uint64, maxSize uint64) ([]raftpb.Entry, error) {

	var i int
	startSegmentIndex := -1
	endSegmentIndex := -1
	segments := make([]*Segment, 0, 4)

	//找到start对应的segment，end对应的segment
	//并将这个范围内的segment，放入一个Segment数组，这样使得后续读取不再需要对logstore的segment数组加锁
	s.Mu.RLock()
	count := len(s.Segments)

	//find the startSegmentIndex and endSegmentIndex
	for i = count - 1; 0 <= i; i-- {
		firstRindex := s.Segments[i].GetFirstRindex()
		lastRindex := s.Segments[i].GetLastRindex()
		if firstRindex <= uint64(start) && uint64(start) <= lastRindex {
			startSegmentIndex = i
		}
		if firstRindex < uint64(end) && uint64(end) <= lastRindex+1 {
			endSegmentIndex = i
		}
		if startSegmentIndex != -1 && endSegmentIndex != -1 {
			break
		}
	}
	//the start raft log not exist,return compact error
	if startSegmentIndex == -1 {
		s.Mu.RUnlock()
		glog.Errorf("[log_store.go-ReadFromSegments]:error,req.Start=%d,req.End=%d,"+
			"startSegmentIndex=%d,endSegmentIndex=%d",
			start, end, startSegmentIndex, endSegmentIndex)
		return nil, raft.ErrCompacted
	}

	if endSegmentIndex == -1 || startSegmentIndex > endSegmentIndex {
		s.Mu.RUnlock()
		glog.Errorf("[log_store.go-ReadFromSegments]:error,req.Start=%d,req.End=%d,"+
			"startSegmentIndex=%d,endSegmentIndex=%d",
			start, end, startSegmentIndex, endSegmentIndex)
		return nil, ErrEntryNotExist
	}
	for i := startSegmentIndex; i <= endSegmentIndex; i++ {
		segments = append(segments, s.Segments[i])
	}
	s.Mu.RUnlock()

	return s.readInSegments(segments, start, end, maxSize)
}

//从segment数组中循环读取
func (s *LogStore) readInSegments(segments []*Segment, startIndex, endIndex uint64, maxSize uint64) ([]raftpb.Entry, error) {
	//[startIndex,endIndex) 可能非常大，所以我们不能预分配太大的空间
	totalRaftLogs := createRaftEntriesSlice(endIndex - startIndex)
	canReadSize := maxSize

	//循环从segments读取数据，范围和大小两个条件，有一个条件达到上限则直接返回
	for i := 0; i < len(segments); i++ {
		var begin, end uint64

		firstRindex := segments[i].GetFirstRindex()
		lastRindex := segments[i].GetLastRindex()

		if firstRindex < startIndex {
			begin = startIndex
		} else {
			begin = firstRindex
		}

		if lastRindex < endIndex {
			end = lastRindex + 1
		} else {
			end = endIndex
		}

		raftLogs, leftSize, err := segments[i].ReadRaftLogsByRindex(begin, end, canReadSize)
		if err != nil {
			return nil, err
		}
		totalRaftLogs = append(totalRaftLogs, raftLogs...)
		//更新需要读取的raftlog size
		canReadSize = leftSize
		//已经读取到了maxSize大小的raft log，直接退出
		if canReadSize == 0 {
			break
		}
	}
	//检查log连续性，在Entries会Fatal，这里只打印参数
	for i := 0; i < len(totalRaftLogs)-1; i++ {
		if totalRaftLogs[i].Index+1 != totalRaftLogs[i+1].Index {
			glog.Infof("[log_store.go-readInSegments]:len(segments)=%d,startIndex=%d,endIndex=%d,maxSize=%d",
				len(segments), startIndex, endIndex, maxSize)
			for j := 0; j < len(segments); j++ {
				glog.Infof("[log_store.go-readInSegments]:j=%d,segment[j].FirstRindex=%d,segment[j].LastRindex=%d",
					segments[j].GetFirstRindex(), segments[j].GetLastRindex())
			}
		}
	}
	return totalRaftLogs, nil
}

//删除rindex及之后的raft log，包含rindex
func (s *LogStore) DeleteRaftLog(rindex uint64) error {
	var i int
	var err error

	//范围检查
	firstRindex, err := s.FirstIndex()
	if err != nil {
		return err
	}
	lastRindex, err := s.LastIndex()
	if err != nil {
		return err
	}
	if rindex < firstRindex || lastRindex < rindex {
		glog.Errorf("[log_store.go-DeleteRaftLog]:rindex is not in the range of logstore,"+
			"firstRindex=%d,lastRindex=%d,rindex=%d", firstRindex, lastRindex, rindex)
		return ErrArgsNotAvailable
	}

	s.Mu.Lock()
	var found = false
	//获取rindex对应的segment，并将之后的segment全部删除
	count := len(s.Segments)
	//find from last segment to first segment
	for i = count - 1; 0 <= i; i-- {
		firstRindex := s.Segments[i].GetFirstRindex()
		lastRindex := s.Segments[i].GetLastRindex()
		if firstRindex <= rindex && rindex <= lastRindex {
			found = true
			break
		}
	}

	if found == false {
		s.Mu.Unlock()
		return nil
	}
	//需要完整删除i后面的segment
	if i < count-1 {
		deleteSegmentNames := make([]string, 0, 4)

		deleteSegments := s.Segments[i+1:]
		for k := 0; k < len(deleteSegments); k++ {
			err = deleteSegments[k].Remove()
			if err != nil {
				glog.Fatalf("[log_store.go-DeleteRaftLog]:Remove segment file in DeleteRaftLog error:%s,"+
					"segmentName=%s", err.Error(), deleteSegments[k].GetName())
			}
			deleteSegmentNames = append(deleteSegmentNames, deleteSegments[k].GetName())
		}

		//reopen the last segment with write
		err = s.Segments[i].ReOpenWithWrite()
		if err != nil {
			glog.Fatalf("[log_store.go-DeleteRaftLog]:ReOpenWithWrite error,err=%s,segmentName=%s",
				s.Segments[i].GetName())
		}
		deleteSegmentNames = append(deleteSegmentNames, s.Segments[i].GetName())

		//detele the last segment range metadata
		err = s.RangeMetaFile.DeleteLogRanges(deleteSegmentNames)
		if err != nil {
			glog.Fatalf("[log_store.go-DeleteRaftLog]:DeleteLogRanges error,err=%s,deleteSegmentNames=%v",
				err.Error(), deleteSegmentNames)
		}
		s.Segments = s.Segments[:i+1]
	}
	lastSegment := s.Segments[len(s.Segments)-1]
	s.Mu.Unlock()

	//删除rindex对应的segment中，在rindex及之后的全部log
	return lastSegment.DeleteRecordsByRindex(rindex)
}

//segment切换
//raft lib保证了写操作串行，所以cutSegment也会串行执行
func (s *LogStore) rollingSegment(lastSegment *Segment, records []*Record) *Segment {

	startTime := time.Now()

	// finish last segment --step1
	// 创建新的segment时，需要保证之前的segment index file中的数据已经全部落盘
	// 这样在数据恢复时，只可能出现最后一个segment index file数据不完整的情况。
	err := lastSegment.SyncIndexFile()
	if err != nil {
		glog.Fatalf("[log_store.go-rollingSegment]: lastSegment.SyncIndexFile error, lastSegment:[%s], error:[%v]",
			lastSegment.GetName(), err)
	}

	syncTime := time.Now()

	err = s.writeRangeMetadata(lastSegment)
	if err != nil {
		glog.Fatalf("[log_store.go-writeRangeMetadata]: writeRangeMetadata error, lastSegment:[%s], error:[%v]",
			lastSegment.GetName(), err)
	}

	writeRangeTime := time.Now()

	// new segment --step2
	// 根据lastSegment生成一个新的Segment Name
	newSegmentName := generateNextSegmentName(lastSegment.GetName())
	newCfg := &NewSegmentConfig{
		Dir:      path.Dir(lastSegment.SegmentPath),
		Name:     newSegmentName,
		MaxBytes: lastSegment.GetMaxBytes(),
		Mc:       s.Mc,
		Meta:     s.Meta,
		//		Ic:            s.IndexLRU,
		RangeMetaFile: s.RangeMetaFile,
	}
	newSegment, err := NewSegment(newCfg)
	if err != nil {
		glog.Fatalf("[log_store.go-rollingSegment]: NewSegment error, NewSegment cfg:[%v], error:[%v]",
			newCfg, err)
	}

	newSegmentTime := time.Now()

	// set the newSegment initVindex, just for generate vindex
	// lastSegment.LastVindex != -1 means lastSegment have user log(kafka log)
	// otherwise should found user log index from the previous segments
	if lastSegment.LastVindex != -1 {
		newSegment.InitFirstVindex = lastSegment.LastVindex + 1
	} else {
		newSegment.InitFirstVindex = s.getMaxVindexWithLock() + 1
	}

	glog.Warningf("[log_store.go-rollingSegment]: rolling segment, newSegmentName=%s"+
		"sync index used %v, writeRange used %v "+
		"newSegment used %v, newSegmentCreateFileTime %v, newSegmentPreallocateTime %v, newSegmentMmapTime %v, "+
		"newSegmentMetaTime %v, newSegmentNewIndexTime %v",
		newSegmentName, syncTime.Sub(startTime), writeRangeTime.Sub(syncTime), newSegmentTime.Sub(writeRangeTime),
		newSegmentCreateFileTime.Sub(newSegmentBeginTime), newSegmentPreallocateTime.Sub(newSegmentCreateFileTime),
		newSegmentMmapTime.Sub(newSegmentPreallocateTime), newSegmentMetaTime.Sub(newSegmentMmapTime),
		newSegmentNewIndexTime.Sub(newSegmentMetaTime))

	return newSegment
}

func (s *LogStore) getMaxVindexWithLock() int64 {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	var maxVindex, lastVindex int64
	maxVindex = -1
	for _, segment := range s.Segments {
		lastVindex = segment.GetLastVindex()
		if maxVindex < lastVindex {
			maxVindex = lastVindex
		}
	}
	return maxVindex
}

func (s *LogStore) writeRangeMetadata(lastSegment *Segment) error {

	logRange := &LogRange{
		FirstRindex: lastSegment.GetFirstRindex(),
		LastRindex:  lastSegment.GetLastRindex(),
		FirstVindex: lastSegment.GetFirstVindex(),
		LastVindex:  lastSegment.GetLastVindex(),
	}

	err := s.RangeMetaFile.AppendLogRange(lastSegment.GetName(), logRange)
	if err != nil {
		return err
	}

	return nil
}

func (s *LogStore) Close() error {
	var err error
	s.Mu.Lock()
	for i := 0; i < len(s.Segments); i++ {
		err = s.Segments[i].Close()
		if err != nil {
			s.Mu.Unlock()
			glog.Errorf("[log_store.go-Close]:log store Close segment error:segment=%v,err=%s", s.Segments[i], err.Error())
			return err
		}
	}
	s.Mu.Unlock()
	s.Mc.close()
	//写入正常退出标识
	s.ExitFlagFile.WriteExitFlag(NormalExitFlag)
	return nil
}

func (s *LogStore) GetFirstSegment() *Segment {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.Segments[0]
}

func (s *LogStore) GetLastSegment() *Segment {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.Segments[len(s.Segments)-1]
}

func (s *LogStore) IsNewStore() bool {
	return s.IsNew
}

// FirstIndex returns the first index written. 0 for no entries.
func (s *LogStore) FirstIndex() (uint64, error) {

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStoreFirstIndexError) {
		return 0, errors.New("Mock FiuStoreFirstIndexError")
	}

	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.Segments[0].GetFirstRindex(), nil
}

// LastIndex returns the last index written. 0 for no entries.
func (s *LogStore) LastIndex() (uint64, error) {

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStoreLastIndexError) {
		return 0, errors.New("Mock FiuStoreLastIndexError")
	}

	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.Segments[len(s.Segments)-1].GetLastRindex(), nil
}

// FirstVIndex returns the first index written. -1 for no entries.
func (s *LogStore) FirstVindex() (int64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	minVindex := getMinVindex(s.Segments)
	return minVindex, nil
}

// LastIndex returns the last index written. -1 for no entries.
func (s *LogStore) LastVindex() (int64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	maxVindex := getMaxVindex(s.Segments)
	return maxVindex, nil
}

func (s *LogStore) SegmentCount() int {
	s.Mu.RLock()
	s.Mu.RUnlock()
	return len(s.Segments)
}

func (s *LogStore) MinVindex() (int64, error) {

	// mock error
	if fiu.IsSyncPointExist(vdlfiu.FiuStoreStoreMinVindexError) {
		return 0, errors.New("Mock FiuStoreStoreMinVindexError")
	}

	return s.FirstVindex()
}

func (s *LogStore) Snapshot() (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, nil
}

func (s *LogStore) CreateSnapshotMeta(applyIndex uint64) ([]*logstream.SegmentFile, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	var i int
	segmentFiles := make([]*logstream.SegmentFile, 0, len(s.Segments))
	//检查参数有效性
	firstRindex := s.Segments[0].GetFirstRindex()
	lastRindex := s.Segments[len(s.Segments)-1].GetLastRindex()
	if applyIndex < firstRindex || applyIndex > lastRindex {
		glog.Errorf("applyIndex is out of range,applyIndex=%d,firstRindex=%d,lastRindex=%d",
			applyIndex, firstRindex, lastRindex)
		return nil, ErrArgsNotAvailable
	}

	//找到applyIndex落在哪个segment，将之前的segmentFile追加进数组
	for i = 0; i < len(s.Segments); i++ {
		segmentLastRindex := s.Segments[i].GetLastRindex()
		if applyIndex <= segmentLastRindex {
			break
		} else {
			segmentName := s.Segments[i].GetName()
			segmentFile := &logstream.SegmentFile{
				SegmentName:      segmentName,
				SegmentSizeBytes: MaxSegmentSize,
				IsLastSegment:    false,
				EndPos:           0,
				FirstVindex:      s.Segments[i].GetFirstVindex(),
				LastVindex:       s.Segments[i].GetLastVindex(),
				FirstRindex:      s.Segments[i].GetFirstRindex(),
				LastRindex:       s.Segments[i].GetLastRindex(),
			}
			indexFile := &logstream.IndexFile{
				IndexName: getIndexNameBySegmentName(segmentName),
				EndPos:    0,
			}
			segmentFile.Index = indexFile
			segmentFiles = append(segmentFiles, segmentFile)
		}
	}

	//set last segment file
	lastSegmentName := s.Segments[i].GetName()
	segmentEndPos, indexEndPos, err := s.Segments[i].GetEndPositionByRindex(applyIndex)
	if err != nil {
		glog.Errorf("GetEndPositionByRindex error,err=%s,segment=%v,applyIndex=%d",
			err.Error(), *(s.Segments[i]), applyIndex)
		return nil, err
	}

	//不生成最后一个segment范围信息
	//因为snapshot中的restore_agent不会将最后一个segment范围信息写入logstore的range metadata文件
	lastSegmentFile := &logstream.SegmentFile{
		SegmentName:      lastSegmentName,
		SegmentSizeBytes: MaxSegmentSize,
		IsLastSegment:    true,
		EndPos:           segmentEndPos,
		FirstRindex:      0,
		FirstVindex:      -1,
		LastRindex:       0,
		LastVindex:       -1,
	}
	lastIndexFile := &logstream.IndexFile{
		IndexName: getIndexNameBySegmentName(lastSegmentName),
		EndPos:    indexEndPos,
	}
	lastSegmentFile.Index = lastIndexFile
	segmentFiles = append(segmentFiles, lastSegmentFile)

	return segmentFiles, nil
}
