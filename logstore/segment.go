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
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"sync"

	"syscall"

	"time"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/vipshop/vdl/consensus/raft"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/logstream"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/myos"
	"github.com/vipshop/vdl/vdlfiu"
	"gitlab.tools.vipshop.com/distributedstorage/fiu"
)

var Encoding = binary.BigEndian

type RequestType int8

const (
	RaftIndexType RequestType = iota
	VdlIndexType
)

type SegmentStatus int8

const (
	SegmentReadOnly SegmentStatus = iota
	SegmentRDWR
	SegmentClosed
)

type OpenMode int8

const (
	ReadWriteMode OpenMode = iota
	ReadOnlyMode
	ReadWriteWithRecoverMode
)

//The recording fields are arranged in the following order:
//crc|record_type|data_length|vindex|term|rindex|raft_type|data
const (
	TermSize          = 8
	RindexSize        = 8
	RaftTypeSize      = 4
	RaftLogHeaderSize = 20
	RecordHeaderSize  = 44
)

type LogFile struct {
	File          *fileutil.LockedFile
	Name          string
	MapData       []byte
	WritePosition int64
	SyncPosition  int64
	MaxBytes      int64
}

type Segment struct {
	Mu              sync.RWMutex
	SegmentPath     string //file path
	FirstVindex     int64
	FirstRindex     uint64
	LastVindex      int64  //the last vdl log index
	LastRindex      uint64 // the last raft log index
	InitFirstVindex int64  //if the segment has record, InitFirstVindex is equal FirstVindex

	Log       *LogFile
	IndexFile *Index // the index for this segment
	Status    SegmentStatus

	Mc            *MemCache
	RangeMetaFile *RangeFile
}

type NewSegmentConfig struct {
	Dir           string
	Name          string
	MaxBytes      int64
	Mc            *MemCache
	Meta          *FileMeta
	RangeMetaFile *RangeFile
}

var (
	newSegmentBeginTime       time.Time
	newSegmentCreateFileTime  time.Time
	newSegmentPreallocateTime time.Time
	newSegmentMmapTime        time.Time
	newSegmentMetaTime        time.Time
	newSegmentNewIndexTime    time.Time
)

//创建segment和index
//segment file name like this :"0000000000000000.log"
func NewSegment(cfg *NewSegmentConfig) (*Segment, error) {

	newSegmentBeginTime = time.Now()

	if cfg.MaxBytes == 0 || len(cfg.Dir) == 0 || len(cfg.Name) == 0 ||
		len(cfg.Meta.VdlVersion) == 0 || cfg.Mc == nil || cfg.RangeMetaFile == nil {
		glog.Errorf("[segment.go-NewSegment]:args are not available:cfg.MaxBytes =%d,"+
			"cfg.Dir=%s,cfg.Name=%s,cfg.Meta=%v,cfg.RangeMetaFile=%v",
			cfg.MaxBytes, cfg.Dir, cfg.Name, cfg.Meta, cfg.RangeMetaFile)
		return nil, ErrArgsNotAvailable
	}

	cfg.Dir = filepath.Clean(cfg.Dir)
	//create log segment file
	logPath := filepath.Join(cfg.Dir, cfg.Name)
	if IsFileExist(logPath) {
		glog.Errorf("[segment.go-NewSegment]:segment file:%v already exists", logPath)
		return nil, ErrFileExist
	}

	//create file
	logFile, err := fileutil.TryLockFile(logPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, fileutil.PrivateFileMode)
	if err != nil {
		return nil, err
	}

	newSegmentCreateFileTime = time.Now()

	//pre allocate maxBytes size space for this file
	if err = fileutil.Preallocate(logFile.File, cfg.MaxBytes, true); err != nil {
		glog.Errorf("[segment.go-NewSegment]:failed to allocate space when creating new segment file (%v)", err)
		logFile.Close()
		return nil, err
	}

	newSegmentPreallocateTime = time.Now()

	log := &LogFile{
		File:          logFile,
		Name:          cfg.Name,
		WritePosition: 0,
		SyncPosition:  0,
		MaxBytes:      cfg.MaxBytes,
	}
	log.MapData, err = syscall.Mmap(int(logFile.File.Fd()), 0, int(cfg.MaxBytes),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_NORESERVE)
	if err != nil {
		glog.Fatalf("[segment.go-NewSegment]:mmap error,err=%s,segmentName=%s", err.Error(), log.Name)
	}

	newSegmentMmapTime = time.Now()

	s := &Segment{
		SegmentPath: logPath,

		FirstVindex:     -1,
		FirstRindex:     0,
		LastVindex:      -1,
		LastRindex:      0,
		InitFirstVindex: 0,

		Log: log,
		Mc:  cfg.Mc,
		//		IndexCache:    cfg.Ic,
		RangeMetaFile: cfg.RangeMetaFile,
		Status:        SegmentRDWR,
	}
	metaBuff := metaToBinary(cfg.Meta)
	err = s.setSegmentFileMeta(metaBuff)
	if err != nil {
		glog.Errorf("[segment.go-NewSegment]:set metaData error:%s", err.Error())
		return nil, err
	}

	newSegmentMetaTime = time.Now()

	//new index
	indexCfg := &NewIndexConfig{
		Dir:  cfg.Dir,
		Name: getIndexNameBySegmentName(cfg.Name),
	}
	s.IndexFile, err = newIndex(indexCfg)
	if err != nil {
		glog.Errorf("[segment.go-NewSegment]:new index file error:%s", err.Error())
		return nil, err
	}

	newSegmentNewIndexTime = time.Now()

	return s, nil
}

type OpenSegmentConfig struct {
	Dir             string
	Name            string
	OpenSegmentMode OpenMode
	MaxBytes        int64
	Range           *LogRange
	Mc              *MemCache
	RangeMetaFile   *RangeFile
}

//open a exist segment file and its index file
func OpenSegment(cfg *OpenSegmentConfig) (*Segment, error) {
	switch cfg.OpenSegmentMode {
	case ReadWriteMode:
		return OpenSegmentWithWrite(cfg)
	case ReadWriteWithRecoverMode:
		return OpenSegmentWithWrite(cfg)
	case ReadOnlyMode:
		return OpenSegmentWithRead(cfg)
	}
	glog.Fatalf("[segment.go-OpenSegment]:OpenSegment error,cfg=%v", *cfg)
	return nil, nil
}

//以只读方式打开segment
func OpenSegmentWithRead(cfg *OpenSegmentConfig) (*Segment, error) {

	if len(cfg.Dir) == 0 || len(cfg.Name) == 0 || cfg.OpenSegmentMode != ReadOnlyMode ||
		cfg.Mc == nil || cfg.RangeMetaFile == nil {
		return nil, ErrArgsNotAvailable
	}
	cfg.Dir = path.Clean(cfg.Dir)
	segmentPath := path.Join(cfg.Dir, cfg.Name)
	exist := IsFileExist(segmentPath)
	if exist == false {
		return nil, ErrFileNotExist
	}

	//open file
	logFile, err := fileutil.TryLockFile(segmentPath, os.O_RDWR, fileutil.PrivateFileMode)
	if err != nil {
		return nil, err
	}

	log := &LogFile{
		File:          logFile,
		Name:          cfg.Name,
		WritePosition: 0,
		SyncPosition:  0,
		MaxBytes:      cfg.MaxBytes,
	}
	log.MapData, err = syscall.Mmap(int(logFile.File.Fd()), 0, int(cfg.MaxBytes),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_NORESERVE)
	if err != nil {
		glog.Fatalf("[segment.go-OpenSegmentWithRead]:mmap error,err=%s,segmentName=%s", err.Error(), log.Name)
	}

	s := &Segment{
		SegmentPath:     segmentPath,
		InitFirstVindex: 0,

		Log:           log,
		Mc:            cfg.Mc,
		RangeMetaFile: cfg.RangeMetaFile,
		Status:        SegmentReadOnly,
	}

	//打开索引文件
	indexName := getIndexNameBySegmentName(cfg.Name)
	openIndexCfg := &OpenIndexConfig{
		Name:          indexName,
		Dir:           cfg.Dir,
		OpenIndexMode: cfg.OpenSegmentMode,
	}
	s.IndexFile, err = openIndex(openIndexCfg)
	if err != nil {
		glog.Errorf("[segment.go-OpenSegmentWithRead]:openIndex in OpenSegment error:error=%s,cfg=%v", err.Error(), *openIndexCfg)
		return nil, err
	}

	//don't need lock, because only one goroutine will open segment and set this values
	if cfg.Range != nil {
		s.FirstRindex = cfg.Range.FirstRindex
		s.LastRindex = cfg.Range.LastRindex
		s.FirstVindex = cfg.Range.FirstVindex
		s.LastVindex = cfg.Range.LastVindex
		s.InitFirstVindex = s.FirstVindex
	} else {
		//如果只读segment的范围在metadata中没有，则解析索引文件得到范围
		err = s.initRangeByParseIndexFile()
		if err != nil {
			glog.Errorf("[segment.go-OpenSegmentWithRead]:initRangeByParseIndexFile error,err=%s,segmentName=%s",
				err.Error(), s.Log.Name)
			return nil, err
		}
	}
	return s, nil
}

//init segment range with parse index file, and write into segment range meta file
func (s *Segment) initRangeByParseIndexFile() error {
	var err error
	var parseIndexResult *ParseIndexResult
	//var segmentRange *RangeFile
	parseIndexResult, err = s.IndexFile.parseIndex()
	if err != nil {
		//index file exist torn write,rebuild index
		if err == ErrTornWrite {
			glog.Warningf("[segment.go-OpenSegmentWithRead]:index file exist torn write, "+
				"rebuild index file,segmentName=%s,indexPath=%s", s.Log.Name, s.IndexFile.IndexPath)
			parseIndexResult, err = s.RebuildIndexFile()
			if err != nil {
				glog.Fatalf("[segment.go-OpenSegmentWithWrite]:RebuildIndexFile error,err=%s,segmentName=%s",
					err.Error(), s.Log.Name)
			}
		} else {
			glog.Errorf("[segment.go-OpenSegmentWithWrite]:parseIndex in OpenSegment error:error=%s,cfg=%v",
				err.Error(), s.IndexFile.IndexPath)
			return err
		}
	}
	s.FirstRindex = parseIndexResult.FirstRindex
	s.LastRindex = parseIndexResult.LastRindex
	s.FirstVindex = parseIndexResult.FirstVindex
	s.LastVindex = parseIndexResult.LastVindex
	s.InitFirstVindex = s.FirstVindex

	logRange := &LogRange{
		FirstRindex: s.FirstRindex,
		LastRindex:  s.LastRindex,
		FirstVindex: s.FirstVindex,
		LastVindex:  s.LastVindex,
	}

	err = s.RangeMetaFile.AppendLogRange(s.Log.Name, logRange)
	if err != nil {
		return err
	}
	return nil
}

//以读写方式打开segment
func OpenSegmentWithWrite(cfg *OpenSegmentConfig) (*Segment, error) {
	var err error
	var parseIndexResult *ParseIndexResult

	//检查有效性
	if len(cfg.Dir) == 0 || len(cfg.Name) == 0 ||
		(cfg.OpenSegmentMode != ReadWriteMode && cfg.OpenSegmentMode != ReadWriteWithRecoverMode) ||
		cfg.MaxBytes <= 0 || cfg.Mc == nil || cfg.RangeMetaFile == nil {
		return nil, ErrArgsNotAvailable
	}
	cfg.Dir = path.Clean(cfg.Dir)
	segmentPath := path.Join(cfg.Dir, cfg.Name)
	exist := IsFileExist(segmentPath)
	if exist == false {
		return nil, ErrFileNotExist
	}

	//open file
	logFile, err := fileutil.TryLockFile(segmentPath, os.O_RDWR, fileutil.PrivateFileMode)
	if err != nil {
		return nil, err
	}

	log := &LogFile{
		File:     logFile,
		Name:     cfg.Name,
		MaxBytes: cfg.MaxBytes,
	}
	log.MapData, err = syscall.Mmap(int(logFile.File.Fd()), 0, int(cfg.MaxBytes),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_NORESERVE)
	if err != nil {
		glog.Fatalf("[segment.go-OpenSegmentWithWrite]:mmap error,err=%s,segmentName=%s", err.Error(), log.Name)
	}
	s := &Segment{
		SegmentPath:     segmentPath,
		InitFirstVindex: 0,
		Mc:              cfg.Mc,
		RangeMetaFile:   cfg.RangeMetaFile,
		Status:          SegmentRDWR,
	}
	s.Log = log

	bufMeta, err := s.getSegmentFileMeta()
	if err != nil {
		if err != nil {
			glog.Errorf("[segment.go-OpenSegmentWithWrite]:getSegmentFileMeta in OpenSegment error:error=%s,dir=%s", err.Error(), segmentPath)
			return nil, err
		}
	}
	log.WritePosition = int64(RecordHeaderSize + len(bufMeta))
	log.SyncPosition = log.WritePosition

	//打开索引文件
	indexName := getIndexNameBySegmentName(cfg.Name)
	openIndexCfg := &OpenIndexConfig{
		Name:          indexName,
		Dir:           cfg.Dir,
		OpenIndexMode: cfg.OpenSegmentMode,
	}
	s.IndexFile, err = openIndex(openIndexCfg)
	if err != nil {
		glog.Errorf("[segment.go-OpenSegmentWithWrite]:openIndex in OpenSegment error:error=%s,cfg=%v", err.Error(), *openIndexCfg)
		return nil, err
	}

	//因为最后一个segment的范围信息，未在range metadata file中存储，需要解析index文件
	//如果在ReadWriteWithRecoverMode模式下打开该segment文件，则直接根据segment重建index文件
	//如果在ReadWriteMode模式下解析index file文件，发现存在torn write，则也根据segment中的record重建index文件 ？
	if cfg.OpenSegmentMode == ReadWriteWithRecoverMode {
		parseIndexResult, err = s.RebuildIndexFile()
		if err != nil {
			glog.Fatalf("[segment.go-OpenSegmentWithWrite]:RebuildIndexFile error in ReadWriteWithRecoverMode,err=%s,"+
				"parseIndexResult=%v,cfg=%v,segmentName=%s", err.Error(), parseIndexResult, *cfg, s.Log.Name)
		}
	} else {
		parseIndexResult, err = s.IndexFile.parseIndex()
		if err != nil {
			//index file exist torn write,rebuild index
			if err == ErrTornWrite {
				glog.Warningf("[segment.go-OpenSegmentWithWrite]:in normal mode index file exist torn write, "+
					"rebuild index file,segmentName=%s,indexPath=%s", s.Log.Name, s.IndexFile.IndexPath)
				parseIndexResult, err = s.RebuildIndexFile()
				if err != nil {
					glog.Fatalf("[segment.go-OpenSegmentWithWrite]:RebuildIndexFile error,err=%s,segmentName=%s",
						err.Error(), s.Log.Name)
				}
			} else {
				glog.Errorf("[segment.go-OpenSegmentWithWrite]:parseIndex in OpenSegment error:error=%s,cfg=%v",
					err.Error(), s.IndexFile.IndexPath)
				return nil, err
			}
		}
	}

	//加载最后一个Segment中的record到MemCache中
	err = s.loadRecordsByIndex(parseIndexResult)
	if err != nil {
		glog.Errorf("[segment.go-OpenSegmentWithWrite]:recoverFromIndex error:error=%s,indexPath=%v",
			err.Error(), s.IndexFile.IndexPath)
		return nil, err
	}

	return s, nil
}

//根据索引项读取record，并加载到MemCache
func (s *Segment) loadRecordsByIndex(parseResult *ParseIndexResult) error {
	if parseResult == nil {
		return ErrArgsNotAvailable
	}
	//debug info
	if glog.V(1) {
		glog.Infof("D:[segment.go-loadRecordsByIndex]:parseResult.FirstRindex=%d,parseResult.LastRindex=%d,"+
			"parseResult.FirstVindex=%d,parseResult.LastVindex=%d,segment=%v\n", parseResult.FirstRindex, parseResult.LastRindex,
			parseResult.FirstVindex, parseResult.LastVindex, s)
	}

	s.FirstRindex = parseResult.FirstRindex
	s.LastRindex = parseResult.LastRindex
	s.FirstVindex = parseResult.FirstVindex
	s.LastVindex = parseResult.LastVindex
	s.InitFirstVindex = s.FirstVindex

	records := make([]*Record, 0, s.LastRindex-s.FirstRindex+1)
	if len(parseResult.Entries) != 0 {
		lastEntry := parseResult.Entries[len(parseResult.Entries)-1]
		//assign last entry position to the position of segment
		s.Log.WritePosition = lastEntry.Position + lastEntry.Length + RecordHeaderSize
		s.Log.SyncPosition = s.Log.WritePosition
		//read all the records in last segment
		for i := s.FirstRindex; i <= s.LastRindex; i++ {
			//read index entry
			ie, err := readEntryByRindex(parseResult.Entries, i)
			if err != nil {
				return err
			}
			//get the byte slice of record
			b := s.Log.MapData[ie.Position : ie.Position+RecordHeaderSize+ie.Length]
			record, err := binaryToRecord(b)
			if err != nil {
				glog.Fatalf("record Crc32 do not match,rindex=%d,vindex=%d,ie.Length=%d,"+
					"segmentName=%s,"+
					"position=%d,length=%d,b=%v",
					ie.Rindex, ie.Vindex, ie.Length,
					s.Log.Name, ie.Position, RecordHeaderSize+ie.Length, b)
			}
			//debug info
			if glog.V(1) {
				glog.Infof("D:[segment.go-loadRecordsByIndex]:index entry fields(rindex=%d,vindex=%d,pos=%d,length=%d),"+
					"record fields(rindex=%d,vindex=%d,b=%d,length=%d)",
					ie.Rindex, ie.Vindex, ie.Position, ie.Length, record.Rindex, record.Vindex, b, record.DataLen)
			}

			if ie.Length != record.DataLen || ie.Rindex != record.Rindex || ie.Vindex != record.Vindex {
				glog.Errorf("[segment.go-loadRecordsByIndex]:index entry fields(rindex=%d,vindex=%d,length=%d) do match record fields(rindex=%d,vindex=%d,length=%d)",
					ie.Rindex, ie.Vindex, ie.Length, record.Rindex, record.Vindex, record.DataLen)
				return ErrFieldNotMatch
			}
			records = append(records, record)
		}
	}
	//debug info
	if glog.V(1) {
		glog.Infof("D:[segment.go-recoverFromIndex]:segment postion=%d", s.Log.WritePosition)
	}
	s.Mc.LoadRecords(records)
	return nil
}

type OpenSegmentsConfig struct {
	Dir           string
	MaxBytes      int64
	Mc            *MemCache
	Meta          *FileMeta
	Ranges        *RangeInfo
	RangeMetaFile *RangeFile
	NeedRecover   bool
}

//打开segments
func OpenSegments(cfg *OpenSegmentsConfig) ([]*Segment, error) {
	if len(cfg.Dir) == 0 || cfg.MaxBytes <= 0 || cfg.Mc == nil ||
		cfg.Ranges == nil || cfg.RangeMetaFile == nil {
		return nil, ErrArgsNotAvailable
	}

	files, err := ioutil.ReadDir(cfg.Dir)
	if err != nil {
		glog.Errorf("[segment.go-OpenSegments]:read dir error in Newstore,err=%s", err.Error())
		return nil, err
	}
	logFiles := make([]os.FileInfo, 0, 10)
	segments := make([]*Segment, 0, 10)
	//logFile is sort by name
	for _, file := range files {
		if strings.HasSuffix(file.Name(), LogFileSuffix) {
			logFiles = append(logFiles, file)
		}
	}
	count := len(logFiles)
	if count == 0 {
		return nil, ErrFileNotExist
	}

	//open for read
	openSegmentcfg := new(OpenSegmentConfig)
	openSegmentcfg.Dir = cfg.Dir
	openSegmentcfg.MaxBytes = cfg.MaxBytes
	openSegmentcfg.Mc = cfg.Mc
	openSegmentcfg.RangeMetaFile = cfg.RangeMetaFile
	for i := 0; i < count-1; i++ {
		openSegmentcfg.Name = logFiles[i].Name()
		if logRange, ok := cfg.Ranges.NameToRange[openSegmentcfg.Name]; ok {
			openSegmentcfg.Range = logRange
		} else {
			openSegmentcfg.Range = nil
		}
		openSegmentcfg.OpenSegmentMode = ReadOnlyMode //read only
		segment, err := OpenSegment(openSegmentcfg)
		if err != nil {
			return nil, err
		}
		segments = append(segments, segment)
	}

	//最后一个文件以读写方式打开
	openSegmentcfg.Name = logFiles[count-1].Name()
	//the last segment do not use range meta data.
	openSegmentcfg.Range = nil
	//是否是恢复模式？
	if cfg.NeedRecover == true {
		openSegmentcfg.OpenSegmentMode = ReadWriteWithRecoverMode
	} else {
		openSegmentcfg.OpenSegmentMode = ReadWriteMode //read and write
	}

	//open the last segment with write
	segment, err := OpenSegment(openSegmentcfg)
	if err != nil {
		return nil, err
	}
	segments = append(segments, segment)
	return segments, nil
}

//获取segment的metadata
func (s *Segment) getSegmentFileMeta() ([]byte, error) {
	header := s.Log.MapData[:RecordHeaderSize]

	crc := Encoding.Uint32(header[:4])
	recordType := int32(Encoding.Uint32(header[4:8]))
	dataLen := int64(Encoding.Uint64(header[8:16]))
	if recordType != MetaDataType {
		return nil, ErrNoFileMeta
	}
	//metadata
	data := s.Log.MapData[RecordHeaderSize : RecordHeaderSize+dataLen]
	//check crc
	tmpBuff := s.Log.MapData[4 : RecordHeaderSize+dataLen]
	newCrc := crc32.ChecksumIEEE(tmpBuff)
	if crc != newCrc {
		return nil, ErrCrcNotMatch
	}
	//debug info
	if glog.V(1) {
		glog.Infof("D:[segment.go-getSegmentFileMeta]:meta=%v", data)
	}
	return data, nil
}

//设置Segment的metada
func (s *Segment) setSegmentFileMeta(metadata []byte) error {
	var err error
	if s.Status != SegmentRDWR {
		return ErrNotAllowWrite
	}

	b := make([]byte, RecordHeaderSize)
	dataLen := len(metadata)
	Encoding.PutUint32(b[4:8], uint32(MetaDataType)) //type
	Encoding.PutUint64(b[8:16], uint64(dataLen))     //data length
	Encoding.PutUint64(b[16:24], uint64(0))          //vindex
	Encoding.PutUint64(b[24:32], 0)                  //term
	Encoding.PutUint64(b[32:40], 0)                  //rindex
	Encoding.PutUint32(b[40:44], 0)                  //raft type

	//写metadata
	b = append(b, metadata...)
	crc := crc32.ChecksumIEEE(b[4:])
	Encoding.PutUint32(b[:4], crc)
	copy(s.Log.MapData[s.Log.WritePosition:s.Log.WritePosition+int64(len(b))], b)
	s.Log.WritePosition = s.Log.WritePosition + int64(len(b))
	err = myos.Syncfilerange(
		s.Log.File.Fd(),
		s.Log.SyncPosition,
		s.Log.WritePosition-s.Log.SyncPosition,
		myos.SYNC_FILE_RANGE_WAIT_BEFORE|myos.SYNC_FILE_RANGE_WRITE|myos.SYNC_FILE_RANGE_WAIT_AFTER,
	)
	if err != nil {
		glog.Fatalf("[segment.go-setSegmentFileMeta]:Syncfilerange error,err=%s,segmentName=%s",
			err.Error(), s.Log.Name)
	}
	s.Log.SyncPosition = s.Log.WritePosition
	return nil
}

func (s *Segment) isFull(recordSize int64) bool {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.Log.WritePosition+recordSize > s.Log.MaxBytes
}

//写records
func (s *Segment) WriteRecords(records []*Record) error {
	s.Mu.Lock()
	//panic
	if s.Status != SegmentRDWR {
		glog.Fatalf("[[segment.go-writeRecords]:segment is not allowed to write,name=%s", s.Log.Name)
	}

	lastRindex := s.LastRindex
	if 0 < lastRindex && lastRindex+1 != records[0].Rindex {
		glog.Fatalf("[segment.go-writeRecords]:the records are not sequential, s.LastRindex=%d, "+
			"the record raft index should be %d, but records[0].Rindex=%d",
			lastRindex, lastRindex+1, records[0].Rindex)
	}

	recordStartPosition := uint64(s.Log.WritePosition)
	startCopy := time.Now()
	//将records写到Mmap映射的内存中，返回最小的vindex和最大的vindex
	minVindex, maxVindex := s.writeRecordsWithVindex(records)

	startSync := time.Now()
	copyDuration = startSync.Sub(startCopy)

	//sync write records to disk
	err := myos.Syncfilerange(
		s.Log.File.Fd(),
		s.Log.SyncPosition,
		s.Log.WritePosition-s.Log.SyncPosition,
		myos.SYNC_FILE_RANGE_WAIT_BEFORE|myos.SYNC_FILE_RANGE_WRITE|myos.SYNC_FILE_RANGE_WAIT_AFTER,
	)
	if err != nil {
		glog.Fatalf("[segment.go-WriteRecords]:Syncfilerange error,err=%s,segmentName=%s",
			err.Error(), s.Log.Name)
	}
	syncDuration = time.Now().Sub(startSync)
	s.Log.SyncPosition = s.Log.WritePosition

	//FIU test[15],模拟Leader调用该函数Crash
	if fiu.IsSyncPointExist(vdlfiu.FiuIndexTornWrite) {
		vdlfiu.MockCrash()
	}
	//FIU test[22],模拟Leader调用该函数，最后一条记录部分写,截断10字节
	//FIU test[23],模拟Follower调用该函数，最后一条记录部分写,截断10字节
	if fiu.IsSyncPointExist(vdlfiu.FiuSegmentTornWrite2) {
		MockSegmentTornWrite(s, 10)
	}

	//写索引信息
	startWriteIndex := time.Now()
	s.IndexFile.writeEntriesByRecords(records, recordStartPosition) //write index file
	writeIndexDuration = time.Now().Sub(startWriteIndex)
	//debug info
	if glog.V(1) {
		glog.Infof("D:[segment.go-writeRecords]:write records,rindex:[%d,%d] vindex:[%d,%d]at position at:%d",
			records[0].Rindex, records[len(records)-1].Rindex, minVindex, maxVindex, recordStartPosition)
	}

	//更新范围
	if s.FirstRindex == 0 {
		s.FirstRindex = records[0].Rindex
	}
	if s.FirstVindex == -1 && 0 <= minVindex {
		s.FirstVindex = minVindex
	}
	if s.LastRindex < records[len(records)-1].Rindex {
		s.LastRindex = records[len(records)-1].Rindex
	}
	if s.LastVindex < maxVindex {
		s.LastVindex = maxVindex
	}
	//unlock before write MemCache
	s.Mu.Unlock()

	writeMemStart := time.Now()
	//write record to memory store
	s.Mc.WriteRecords(records)
	writeMemDuration = time.Now().Sub(writeMemStart)
	return nil
}

//set the vdl log index
//if segment has no records, r.vindex = previous_segment.GetLastVindex() + 1,this value equal InitFirstVindex
//if this segment has no records and it is the first segment, r.Vindex = 0
//return minVindex,maxVindex
func (s *Segment) writeRecordsWithVindex(records []*Record) (int64, int64) {
	var vindex, minVindex, maxVindex int64
	if s.FirstVindex == -1 {
		vindex = s.InitFirstVindex
	} else {
		vindex = s.LastVindex + 1
	}
	minVindex = -1
	maxVindex = -1
	//set the vdl log index
	//if segment has no records, r.vindex = previous_segment.GetLastVindex() + 1,this value equal InitFirstVindex
	//if this segment has no records and it is the first segment, r.Vindex = 0
	for i := 0; i < len(records); i++ {
		if records[i].RecordType == VdlLogType {
			records[i].Vindex = vindex
			//set minVindex
			if minVindex == -1 {
				minVindex = vindex
			}
			//set maxVindex
			if maxVindex < vindex {
				maxVindex = vindex
			}
			vindex++
		} else {
			records[i].Vindex = -1
		}
		buf := recordToBinary(records[i])
		wroteBufLen := copy(s.Log.MapData[s.Log.WritePosition:s.Log.WritePosition+int64(len(buf))], buf)
		// add logical check, when copy date != copied data
		if wroteBufLen != len(buf) {
			glog.Fatalf("[segment.go-writeRecordsWithVindex]: copy mem error, want to copy %d len, but copied %d",
				len(buf), wroteBufLen)
		}
		s.Log.WritePosition = s.Log.WritePosition + int64(len(buf))

		//FIU test[14],模拟Leader调用该函数Crash
		//FIU test[16],模拟Follower调用该函数Crash
		if fiu.IsSyncPointExist(vdlfiu.FiuSegmentTornWrite) {
			//copy record[0],drop other records
			vdlfiu.MockCrash()
		}
	}
	return minVindex, maxVindex
}

type VdlResult struct {
	entries    []logstream.Entry
	leftSize   int32
	nextVindex int64
}

//根据vindex读取vdl log
func (s *Segment) ReadVdlLogsByVindex(startVindex int64, endRindex uint64, maxSize int32) (*VdlResult, error) {

	s.Mu.RLock()
	defer s.Mu.RUnlock()

	if s.Status == SegmentClosed {
		glog.Errorf("[segment.go-ReadVdlLogsByVindex]:segment is closed,segmentName=%s,s.FirstVindex=%d"+
			"s.FirstRindex=%d,s.LastVindex=%d,s.LastRindex=%d,startVindex=%d,endRindex=%d", s.Log.Name,
			s.FirstVindex, s.FirstRindex, s.LastVindex, s.LastRindex, startVindex, endRindex)
		return nil, ErrOutOfRange
	}

	var readEndVindex int64
	readStartVindex := startVindex
	vdlLogs := make([]logstream.Entry, 0, 100)
	var hadReadSize int32

	for {
		if readStartVindex+int64(IndexCountPerPage) < s.LastVindex {
			readEndVindex = readStartVindex + int64(IndexCountPerPage) - 1
		} else {
			readEndVindex = s.LastVindex
		}

		//按页读取index entry,[start,end)
		//ies中保证了vindex连续，rindex有可能不连续！
		ies, err := s.getIndexEntries(VdlIndexType, uint64(readStartVindex), uint64(readEndVindex+1), uint64(noLimitVdlLog))
		if err != nil {
			glog.Errorf("[segment.go-ReadVdlLogsByVindex]:getIndexEntries error,err=%s,"+
				"start=%d,end=%d", err.Error(), readStartVindex, readEndVindex+1)
			return nil, err
		}

		for i := 0; i < len(ies); i++ {
			b := s.Log.MapData[ies[i].Position : ies[i].Position+RecordHeaderSize+ies[i].Length]
			entry := logstream.Entry{
				//Record头部为44字节，后续的8字节是ReqID，真正的数据是从52字节开始的
				Data:   b[52:],
				Offset: ies[i].Vindex,
			}

			//检查vdl log连续性
			offsetInRecord := int64(Encoding.Uint64(b[16:24]))
			if ies[i].Vindex != readStartVindex+int64(i) || offsetInRecord != ies[i].Vindex {
				//打印ies
				for j := 0; j < len(ies); j++ {
					glog.Infof("j=%d,ies[j].Crc=%d,ies[j].Vindex=%d,ies[j].Rindex=%d,ies[j].Position=%d,ies[j].Length=%d",
						j, ies[j].Crc, ies[j].Vindex, ies[j].Rindex, ies[j].Position, ies[j].Length)
				}
				glog.Fatalf("[segment.go-ReadVdlLogsByVindex]: read entries are not continuous, ies[%d].Vindex=%d,"+
					"startVindex+int64(i)=%d,endRindex=%d,offsetInRecord=%d,maxSize=%d,b=%v",
					i, ies[i].Vindex, readStartVindex+int64(i), endRindex, offsetInRecord, maxSize, b)
			}

			if ies[i].Rindex <= endRindex {
				vdlLogs = append(vdlLogs, entry)
				hadReadSize += int32(len(entry.Data))
				if ies[i].Rindex == endRindex || maxSize <= hadReadSize {
					return &VdlResult{
						entries:    vdlLogs,
						leftSize:   0,
						nextVindex: -1,
					}, nil
				}
			}

			if ies[i].Rindex > endRindex {
				return &VdlResult{
					entries:    vdlLogs,
					leftSize:   0,
					nextVindex: -1,
				}, nil
			}

		}

		//下一次开始读取的位置
		readStartVindex = readEndVindex + 1
		//还需要再读取下一个segment的数据
		if s.LastVindex < readStartVindex {
			if (maxSize - hadReadSize) <= 0 {
				glog.Fatalf("[segment.go-ReadVdlLogsByVindex]: should not be here, maxSize:%d, hadReadSize:%d,"+
					"startVindex:%d , endRindex:%d",
					maxSize, hadReadSize, startVindex, endRindex)
			}
			return &VdlResult{
				entries:    vdlLogs,
				leftSize:   maxSize - hadReadSize,
				nextVindex: s.LastVindex + 1,
			}, nil
		}
	}
	glog.Fatalf("[segment.go-ReadVdlLogsByVindex]:can't be here,startVindex=%d,"+
		"endRindex=%d,maxSize=%d", startVindex, endRindex, maxSize)
	return nil, nil
}

//读取[start,end)范围的raft log，同时满足整个raft log size大小不能超过maxSize，hasRead表示是否已经读取了部分raftlog
//用于判断第一条raftlog 就超过maxSize时是否添加该raftlog
//返回<raft_log,剩下应该读取的大小,error>
func (s *Segment) ReadRaftLogsByRindex(start, end uint64, maxSize uint64) ([]raftpb.Entry, uint64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	//如果文件被关闭，触发了Log compaction
	if s.Status == SegmentClosed {
		glog.Errorf("[segment.go-ReadVdlLogsByVindex]:segment is closed,segmentName=%s,start=%d,end=%d,maxSize=%d,"+
			"s.FirstRindex=%d,s.LastRindex=%d", s.Log.Name, start, end, maxSize, s.FirstRindex, s.LastRindex)
		return nil, 0, raft.ErrCompacted
	}

	startGetIndex := time.Now()

	//获取[start,end)的第一个索引项
	firstIndexEntry := s.getRaftLogStartIndex(start)
	readSegmentPosition := firstIndexEntry.Position

	endGetIndex := time.Now()
	getIndexDuration = endGetIndex.Sub(startGetIndex)
	returnIesFromFileDuration = endGetIndex.Sub(returnIesFromFileBegin)

	var readSize uint64
	//根据索引项读取raft entry

	raftLogs := s.createRaftEntrySlice(end - start)
	var i uint64 = 0
	for i = 0; i < noLimit; i++ {
		headerBytes := s.Log.MapData[readSegmentPosition : readSegmentPosition+RecordHeaderSize]
		dateLen := recordBinaryToDataLen(headerBytes)

		// pre check, try best
		if dateLen == 0 {
			// when data is null, maybe is raft non-data entry, check term and index indeed
			term := recordBinaryToTerm(headerBytes)
			index := recordBinaryToIndex(headerBytes)
			if term == 0 || index == 0 {
				glog.Fatalf("ReadRaftLogsByRindex, read data error: dateLen is 0,"+
					"segmentName:%s,term:%d,index:%d, readPosition:%d",
					s.Log.Name, term, index, readSegmentPosition)
			}
		}

		raftEntry, err := binaryToRaftEntry(s.Log.MapData[readSegmentPosition : readSegmentPosition+RecordHeaderSize+dateLen])
		if err != nil {
			glog.Fatalf("record Crc32 do not match,segmentName=%s,position=%d,length=%d,b=%v",
				s.Log.Name, readSegmentPosition, dateLen,
				s.Log.MapData[readSegmentPosition:readSegmentPosition+RecordHeaderSize+dateLen])
		}

		readSize += uint64(dateLen)
		raftLogs = append(raftLogs, raftEntry)

		//检查读取的raft log是否正确, 并检查连续性
		if raftEntry.Index != start+i {
			glog.Fatalf("[segment.go-ReadRaftLogsByRindex]: read entries are not continuous,"+
				"start+uint64(i)=%d,raftEntry.Index=%d,start=%d,end=%d,maxSize=%d",
				start+uint64(i), raftEntry.Index, start, end, maxSize)
		}

		if readSize >= maxSize {
			break
		}
		if raftEntry.Index+1 == end {
			break
		}
		if raftEntry.Index == s.LastRindex {
			break
		}
		readSegmentPosition = readSegmentPosition + RecordHeaderSize + dateLen
	}

	var leftSize uint64
	if readSize > maxSize {
		leftSize = 0
	} else {
		leftSize = maxSize - readSize
	}
	GetRecordDuration = time.Now().Sub(endGetIndex)
	//返回raftlog和还需要读取的size大小
	return raftLogs, leftSize, nil
}

//根据rindex读取对应的vindex
func (s *Segment) GetVindexByRindex(rindex uint64) (int64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	if s.Status == SegmentClosed {
		return -1, ErrSegmentClosed
	}
	ies, err := s.getIndexEntries(RaftIndexType, rindex, rindex+1, noLimit)
	if err != nil {
		glog.Errorf("[segment.go-GetVindexByRindex]:getIndexEntries error,err=%s,rindex=%d",
			err.Error(), rindex)
		return -1, err
	}
	return ies[0].Vindex, nil
}

//only delete record in the last segment, and must guarantee the rindex(start) in this segment.
func (s *Segment) DeleteRecordsByRindex(start uint64) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	//检查Segment状态，只有最后一个segment(状态是可读写)才能被删除raft log
	if s.Status == SegmentClosed {
		glog.Infof("[segment.go-DeleteRecordsByRindex]:segment is closed,segmentName=%s",
			s.Log.Name)
		return nil
	}
	if s.Status == SegmentReadOnly {
		glog.Errorf("[segment.go-DeleteRecordsByRindex]:segment open with read, not allow to delete record,segmentName=%s",
			s.Log.Name)
		return ErrNotAllowDelete
	}

	firstRindex := s.FirstRindex
	lastRindex := s.LastRindex
	//must keep start in this segment
	if start < firstRindex || start > lastRindex {
		return nil
	}

	//get segment position
	ies, err := s.getIndexEntries(RaftIndexType, start, lastRindex+1, noLimit)
	if err != nil {
		glog.Errorf("[segment.go-DeleteRecordsByRindex]:getIndexEntries error,err=%s,rindexStart=%d,rindexEnd=%d",
			err.Error(), start, lastRindex+1)
		return err
	}

	//删除索引项
	//ies[0].Rindex=start
	//set index position
	indexPos := (start - firstRindex) * IndexEntrySize
	err = s.IndexFile.truncateFileByPosition(int64(indexPos))
	if err != nil {
		return err
	}

	//删除segment文件中的记录
	//将ies对应的record全部设置为0
	s.deleteRecordsByIndexEntry(ies)

	//delete records from memcache
	s.Mc.DeleteRecordsByRindex(start)

	//更改segment范围
	var minVindex int64
	minVindex = -1
	//get the min Vindex from start
	for i := 0; i < len(ies); i++ {
		if 0 <= ies[i].Vindex {
			minVindex = ies[i].Vindex
			break
		}
	}
	//update last raft index and vdl index
	if start == firstRindex {
		s.FirstRindex = 0
		s.LastRindex = 0
		s.FirstVindex = -1
		s.LastVindex = -1
	} else {
		//s.FirstRindex+1 <= start
		s.LastRindex = start - 1
		//if minVindex == -1, the last vindex not change
		if minVindex == 0 {
			s.FirstVindex = -1
			s.LastVindex = -1
		}
		if 0 < minVindex {
			s.LastVindex = minVindex - 1
		}
	}

	return nil
}

//将ies对应的records占用的空间，设置为0
func (s *Segment) deleteRecordsByIndexEntry(ies []*IndexEntry) {
	var position int64
	var length int64

	if len(ies) == 0 {
		glog.Fatalf("[segment.go-deleteRecordsByIndexEntry]:len(ies)=0")
	}
	position = ies[0].Position
	for i := 0; i < len(ies); i++ {
		length += RecordHeaderSize + ies[i].Length
	}
	zeroBuf := make([]byte, length)

	copy(s.Log.MapData[position:position+length], zeroBuf)
	err := myos.Syncfilerange(
		s.Log.File.Fd(),
		position,
		length,
		myos.SYNC_FILE_RANGE_WAIT_BEFORE|myos.SYNC_FILE_RANGE_WRITE|myos.SYNC_FILE_RANGE_WAIT_AFTER,
	)
	if err != nil {
		glog.Fatalf("[segment.go-deleteRecordsByPosition]:Syncfilerange error,err=%s,position=%d,length=%d",
			err.Error(), position, length)
	}

	s.Log.WritePosition = ies[0].Position
	s.Log.SyncPosition = s.Log.WritePosition
}

func (s *Segment) SyncIndexFile() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	err := s.IndexFile.sync()
	if err != nil {
		return err
	}
	return nil
}

func (s *Segment) SetReadOnly() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Status = SegmentReadOnly
}

//将只读的segment以读写方式打开
func (s *Segment) ReOpenWithWrite() error {
	var indexEntries []*IndexEntry
	var err error
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.Status = SegmentRDWR
	s.IndexFile.Position, err = s.IndexFile.IndexFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	indexEntries, err = s.getIndexEntries(RaftIndexType, s.FirstRindex, s.LastRindex+1, noLimit)
	if err != nil {
		return err
	}
	parseResult := &ParseIndexResult{
		Entries:     indexEntries,
		FirstVindex: s.FirstVindex,
		FirstRindex: s.FirstRindex,
		LastVindex:  s.LastVindex,
		LastRindex:  s.LastRindex,
	}
	err = s.loadRecordsByIndex(parseResult)
	if err != nil {
		glog.Errorf("[segment.go-OpenSegmentWithWrite]:recoverFromIndex error:error=%s,indexPath=%v",
			err.Error(), s.IndexFile.IndexPath)
		return err
	}

	return nil
}

func (s *Segment) Close() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if s.Status == SegmentClosed {
		return nil
	}
	s.Status = SegmentClosed
	err := syscall.Munmap(s.Log.MapData)
	if err != nil {
		glog.Errorf("[segment.go-Close]:Munmap error,err=%s,segmentName=%s", err.Error(), s.Log.Name)
	}
	err = s.Log.File.Close()
	if err != nil {
		glog.Errorf("[segment.go-Close]:file Close error,err=%s,segmentName=%s", err.Error(), s.Log.Name)
	}
	err = s.IndexFile.close()
	if err != nil {
		glog.Errorf("[segment.go-Close]:indexFile Close error,err=%s,segmentName=%s",
			err.Error(), s.IndexFile.Name)
	}

	glog.Infof("[segment.go-Close]:segment[%s] has been closed", s.Log.Name)
	return nil
}

//暂不使用异步删除！！
//func (s *Segment) RemoveWithMark() {
//	err := s.Close()
//	if err != nil {
//		glog.Errorf("[segment.go-RemoveWithMark]:close segment error,err=%s,segmentName=%s",
//			err.Error(), s.Log.Name)
//	}
//}

//remove the segment file and index file synchronously
func (s *Segment) Remove() error {
	err := s.Close()
	if err != nil {
		glog.Errorf("[segment.go-Remove]:close segment error,err=%s,segmentName=%s",
			err.Error(), s.Log.Name)
		return err
	}
	err = os.Remove(s.SegmentPath)
	if err != nil {
		return err
	}
	err = os.Remove(s.IndexFile.IndexPath)
	if err != nil {
		glog.Errorf("[segment.go-Remove]:remove segment[%s] error,err=%s", s.Log.Name, err.Error())
		return err
	}
	glog.Infof("[segment.go-Remove]:segment[%s] has been removed", s.Log.Name)
	return nil
}

func (s *Segment) getRaftLogStartIndex(start uint64) *IndexEntry {

	position := int64(start-s.FirstRindex) * IndexEntrySize
	ies, err := s.IndexFile.getIndexEntries(int64(position), 1)

	if err != nil {
		glog.Fatalf("[segment.go-getRaftLogStartIndex]:getRaftLogStartIndex by rindex error,"+
			"err: %s, position: %d, segmentName: %s, start: %d",
			err.Error(), position, s.Log.Name, start)
	}
	if len(ies) != 1 {
		glog.Fatalf("[segment.go-getRaftLogStartIndex]:getRaftLogStartIndex by rindex error, "+
			"should read 1 index entry, but read : %d, segmentName: %s, start: %d",
			len(ies), s.Log.Name, start)
	}

	return ies[0]
}

//get the index entry:[start,end)
func (s *Segment) getIndexEntries(rt RequestType, start, end, maxSize uint64) ([]*IndexEntry, error) {
	var iesFromFile []*IndexEntry
	var err error

	startReadFile := time.Now()
	iesFromFile, err = s.getIndexEntriesFromFile(rt, start, end, maxSize)
	if err != nil {
		glog.Errorf("[segment.go-getIndexEntries]:getIndexEntriesFromFile error,"+
			"err=%s,requestType=%d, start=%d,end=%d",
			err.Error(), rt, start, end)
		return nil, err
	}

	endReadFile := time.Now()
	readIndexFileDuration = endReadFile.Sub(startReadFile)

	for i := 0; i < len(iesFromFile)-1; i++ {
		if iesFromFile[i].Rindex+1 != iesFromFile[i+1].Rindex && rt == RaftIndexType {
			glog.Infof("[segment.go-getIndexEntries]:ies is not continuous,sum=%d,", len(iesFromFile))
			for j := 0; j < len(iesFromFile); j++ {
				glog.Infof("[segment.go-getIndexEntries]:j=%d,ies[j].Crc=%d,ies[j].Vindex=%d,ies[j].Rindex=%d,"+
					"ies[j].Position=%d,ies[j].Length=%d", j, iesFromFile[j].Crc, iesFromFile[j].Vindex, iesFromFile[j].Rindex,
					iesFromFile[j].Position, iesFromFile[j].Length)
			}
			glog.Fatalf("[segment.go-getIndexEntries]:ies is not continuous,i=%d,ies[i].Crc=%d,ies[i].Vindex=%d,ies[i].Rindex=%d,"+
				"ies[i].Position=%d,ies[i].Length=%d", i, iesFromFile[i].Crc, iesFromFile[i].Vindex, iesFromFile[i].Rindex,
				iesFromFile[i].Position, iesFromFile[i].Length)
		}
	}

	returnIesFromFileBegin = time.Now()
	checkIndexDuration = returnIesFromFileBegin.Sub(endReadFile)
	return iesFromFile, nil
}

//func (s *Segment) readIndexFromLRU(rt RequestType, start, end, maxSize uint64) ([]*IndexEntry, uint64) {
//
//	var ies = s.createIndexEntriesSlice(end - start)
//	var readSizeInLRU uint64
//
//	//get from LRU
//	for i := start; i < end; i++ {
//		var v *IndexEntry
//		var exist bool
//		if rt == RaftIndexType {
//			v, exist = s.IndexCache.GetByRindex(i)
//		} else {
//			v, exist = s.IndexCache.GetByVindex(int64(i))
//		}
//		if exist {
//			ies = append(ies, v)
//			readSizeInLRU += uint64(v.Length)
//			if readSizeInLRU >= maxSize {
//				break
//			}
//		} else {
//			break
//		}
//	}
//	return ies, readSizeInLRU
//}

//从文件读取index entry
//RequestType：读取index 类型，分为:RaftIndexType和VdlIndexType
//读取范围：[start,end)
func (s *Segment) getIndexEntriesFromFile(rt RequestType, start, end, maxSize uint64) ([]*IndexEntry, error) {

	if rt == RaftIndexType {
		return s.readRindexFromFile(start, end, maxSize), nil
	} else if rt == VdlIndexType {
		return s.readVindexFromFile(start, end, maxSize)
	}
	glog.Fatalf("[segment.go-getIndexEntriesFromFile]: RequestType not RaftIndexType and VdlIndexType,"+
		"RequestType=%d, start=%d,end=%d,maxSize=%d, segmentName=%s",
		rt, start, end, maxSize, s.Log.Name)
	return nil, nil

}

//从文件读取[startVindex,endVindex)范围的index entry
//s.FirstVindex <=startVindex
//endVindex <= s.LastVindex+1
func (s *Segment) readVindexFromFile(startVindex, endVindex, maxSize uint64) ([]*IndexEntry, error) {

	var ies []*IndexEntry
	var err error
	totalIndexCount := endVindex - startVindex
	var result = s.createIndexEntriesSlice(totalIndexCount)
	position := (int64(startVindex) - s.FirstVindex) * IndexEntrySize
	count := totalIndexCount
	var hadReadVindexSize uint64
	var hadReadVindexCount uint64

	//按页读取索引
	for {
		if totalIndexCount <= hadReadVindexCount {
			glog.Fatalf("[segment.go-readVindexFromFile]:should not reach, totalIndexCount:%d,"+
				"hadReadVindexSize:%d,hadReadVindexCount:%d,maxSize:%d,startVindex:%d,endVindex:%d",
				totalIndexCount, hadReadVindexSize, hadReadVindexCount, maxSize, startVindex, endVindex)
		}

		readOnce := totalIndexCount - hadReadVindexCount
		ies, err = s.IndexFile.getIndexEntries(position, readOnce)
		if err != nil {
			glog.Errorf("[segment.go-readVindexFromFile]:getIndexEntries by vindex error,err=%s,position=%d,count=%d",
				err.Error(), position, totalIndexCount)
			return nil, err
		}
		if readOnce != uint64(len(ies)) {
			glog.Fatalf("[segment.go-readVindexFromFile]:getIndexEntries error,len(ies) is not equal count,"+
				"len(ies)=%d,count=%d,position=%d", len(ies), count, position)
		}
		for i := 0; i < len(ies); i++ {
			if int64(startVindex) <= ies[i].Vindex && ies[i].Vindex < int64(endVindex) {
				result = append(result, ies[i])
				hadReadVindexCount++
				hadReadVindexSize += uint64(ies[i].Length)
				if hadReadVindexSize >= maxSize || hadReadVindexCount == totalIndexCount {
					return result, nil
				}
			}
		}

		position += int64(readOnce * IndexEntrySize)
	}
	glog.Fatalf("[segment.go-readVindexFromFile]:should not reach the last, totalIndexCount:%d,"+
		"hadReadVindexSize:%d,hadReadVindexCount:%d,maxSize:%d,startVindex:%d,endVindex:%d",
		totalIndexCount, hadReadVindexSize, hadReadVindexCount, maxSize, startVindex, endVindex)
	return nil, nil
}

//从文件读取[startRindex,endRindex)范围的index entry
//s.FirstRindex <=startRindex
//endRindex <= s.LastRindex+1
func (s *Segment) readRindexFromFile(startRindex, endRindex, maxSize uint64) []*IndexEntry {

	var ies []*IndexEntry
	var err error

	totalIndexCount := endRindex - startRindex
	var result = s.createIndexEntriesSlice(totalIndexCount)
	var hadReadSize uint64
	var hadReadCount uint64
	position := int64(startRindex-s.FirstRindex) * IndexEntrySize

	for {
		if totalIndexCount <= hadReadCount {
			glog.Fatalf("[segment.go-readRindexFromFile]:totalIndexCount is little than hadReadCount,"+
				"totalIndexCount=%d,hadReadCount=%d,startRindex=%d,endRindex=%d,maxSize=%d,hadReadSize=%d",
				totalIndexCount, hadReadCount, startRindex, endRindex, maxSize, hadReadSize)
		}

		//loop read, read readIndexCountFromFileOnce per once
		readOnce := totalIndexCount - hadReadCount
		if readOnce > IndexCountPerPage {
			readOnce = IndexCountPerPage
		}

		// read from file
		ies, err = s.IndexFile.getIndexEntries(int64(position), readOnce)
		if err != nil {
			glog.Fatalf("[segment.go-getIndexEntriesFromFile]:getIndexEntries by rindex error,"+
				"err=%s,position=%d,totalIndexCount=%d, hadReadCount=%d, hadReadSize=%d",
				err.Error(), position, totalIndexCount, hadReadCount, hadReadSize)
		}
		if uint64(len(ies)) != readOnce {
			glog.Fatalf("[segment.go-getIndexEntriesFromFile]:getIndexEntries by rindex error, readOnce not equal had read(len(ies)),"+
				"len(ies)=%d, readOnce=%d, position=%d, totalIndexCount=%d, hadReadCount=%d, hadReadSize=%d",
				len(ies), readOnce, position, totalIndexCount, hadReadCount, hadReadSize)
		}

		for _, idx := range ies {
			result = append(result, idx)
			hadReadCount++
			hadReadSize += uint64(idx.Length)
			if hadReadSize >= maxSize || hadReadCount == totalIndexCount {
				return result
			}
		}
		position += int64(readOnce * IndexEntrySize)
	}
	glog.Fatalf("[segment.go-readRindexFromFile]:should not be the last place"+
		"totalIndexCount=%d,hadReadCount=%d,startRindex=%d,endRindex=%d,maxSize=%d,hadReadSize=%d",
		totalIndexCount, hadReadCount, startRindex, endRindex, maxSize, hadReadSize)
	return nil
}

func (s *Segment) createIndexEntriesSlice(maxCount uint64) []*IndexEntry {
	if maxSliceSize < maxCount {
		return make([]*IndexEntry, 0, maxSliceSize)
	} else {
		return make([]*IndexEntry, 0, maxCount)
	}
}

func (s *Segment) createRaftEntrySlice(maxCount uint64) []raftpb.Entry {
	if maxSliceSize < maxCount {
		return make([]raftpb.Entry, 0, maxSliceSize)
	} else {
		return make([]raftpb.Entry, 0, maxCount)
	}
}

//只有在启动的时候，才有可能进行重建索引操作
//所以不需要加锁
func (s *Segment) RebuildIndexFile() (*ParseIndexResult, error) {
	var position, dataLen int64
	var recordCrc, newCrc, indexEntryCrc uint32
	var rindex uint64
	var vindex int64

	position = 0
	indexEntryBuf := make([]byte, IndexEntrySize)
	indexBuf := make([]byte, 0, IndexEntrySize*4096)
	parseIndexResult := &ParseIndexResult{
		FirstRindex: 0,
		LastRindex:  0,
		FirstVindex: -1,
		LastVindex:  -1,
		Entries:     make([]*IndexEntry, 0, 4096),
	}

	//遍历segment文件内容
	for position < s.Log.MaxBytes {
		if s.Log.MaxBytes < position+RecordHeaderSize {
			break
		}
		RecordHeader := s.Log.MapData[position : position+RecordHeaderSize]
		recordCrc = Encoding.Uint32(RecordHeader[:4])
		recordType := int32(Encoding.Uint32(RecordHeader[4:8]))
		dataLen = int64(Encoding.Uint64(RecordHeader[8:16]))

		if dataLen < 0 || s.Log.MaxBytes < position+RecordHeaderSize+dataLen {
			break
		}
		//check Crc
		newCrc = crc32.ChecksumIEEE(s.Log.MapData[position+4 : position+RecordHeaderSize+dataLen])
		if recordCrc != newCrc {
			break
		}
		vindex = int64(Encoding.Uint64(RecordHeader[16:24]))
		rindex = Encoding.Uint64(RecordHeader[32:40])
		switch recordType {
		case MetaDataType:
			//只修改文件偏移，不存储对应的index entry
			position += RecordHeaderSize + dataLen
			continue
		case RaftLogType:
			if parseIndexResult.FirstRindex == 0 {
				parseIndexResult.FirstRindex = rindex
			}
			if parseIndexResult.LastRindex < rindex {
				parseIndexResult.LastRindex = rindex
			}
		case VdlLogType:
			if parseIndexResult.FirstRindex == 0 {
				parseIndexResult.FirstRindex = rindex
			}
			if parseIndexResult.LastRindex < rindex {
				parseIndexResult.LastRindex = rindex
			}
			if parseIndexResult.FirstVindex == -1 {
				parseIndexResult.FirstVindex = vindex
			}
			if parseIndexResult.LastVindex < vindex {
				parseIndexResult.LastVindex = vindex
			}
		}

		//vindex
		copy(indexEntryBuf[4:12], RecordHeader[16:24])
		//rindex
		copy(indexEntryBuf[12:20], RecordHeader[32:40])
		//position
		Encoding.PutUint64(indexEntryBuf[20:28], uint64(position))
		//data length
		copy(indexEntryBuf[28:36], RecordHeader[8:16])
		indexEntryCrc = crc32.ChecksumIEEE(indexEntryBuf[4:])
		Encoding.PutUint32(indexEntryBuf[:4], indexEntryCrc)
		//append index entry into the index buffer
		indexBuf = append(indexBuf, indexEntryBuf...)
		ie, err := binaryToIndex(indexEntryBuf, false)
		if err != nil {
			return nil, err
		}
		parseIndexResult.Entries = append(parseIndexResult.Entries, ie)
		//RecordHeaderSize + data_length
		position += RecordHeaderSize + dataLen
	}

	//write index file
	err := s.IndexFile.truncateFileByPosition(0)
	if err != nil {
		glog.Fatalf("[segment.go-RebuildIndexFile]:truncateFileByPosition error,err=%s,indexName=%s",
			err.Error(), s.IndexFile.Name)
	}
	_, err = s.IndexFile.IndexFile.WriteAt(indexBuf, 0)
	if err != nil {
		glog.Fatalf("[index.go-writeEntriesByRecords]:write index entry error:%s", err.Error())
	}
	s.IndexFile.Position += int64(len(indexBuf))

	glog.Infof("[segment.go-RebuildIndexFile]:rebuild index file success,firstRindex=%d,lastRindex=%d,"+
		"firstVindex=%d,lastVindex=%d,len(entries)=%d", parseIndexResult.FirstRindex, parseIndexResult.LastRindex,
		parseIndexResult.FirstVindex, parseIndexResult.LastVindex, len(parseIndexResult.Entries))

	return parseIndexResult, nil
}

func (s *Segment) GetFirstRindex() uint64 {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.FirstRindex
}

func (s *Segment) GetLastRindex() uint64 {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.LastRindex
}

func (s *Segment) GetFirstVindex() int64 {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.FirstVindex
}

func (s *Segment) GetLastVindex() int64 {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.LastVindex
}

func (s *Segment) GetName() string {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.Log.Name
}

func (s *Segment) GetMaxBytes() int64 {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.Log.MaxBytes
}

//just for fiu test
func MockSegmentTornWrite(s *Segment, count int64) {
	zeroBuf := make([]byte, count)
	startPos := s.Log.WritePosition - count
	if startPos < 0 {
		glog.Fatalf("[util.go-MockSegmentTornWrite]:startPos is negative")
	}
	copy(s.Log.MapData[startPos:s.Log.WritePosition], zeroBuf)
	err := myos.Syncfilerange(
		s.Log.File.Fd(),
		startPos,
		count,
		myos.SYNC_FILE_RANGE_WAIT_BEFORE|myos.SYNC_FILE_RANGE_WRITE|myos.SYNC_FILE_RANGE_WAIT_AFTER,
	)
	if err != nil {
		glog.Fatalf("[util.go-MockSegmentTornWrite]:Syncfilerange error,err=%s,segmentName=%s",
			err.Error(), s.Log.Name)
	}
	glog.Fatalf("[util.go-MockSegmentTornWrite]:mock crash after torn write segment in fiu")
}

//读取rindex在segment和index中对应记录的结束位置
//用于记录snapshot中最后一个segment和index文件
func (s *Segment) GetEndPositionByRindex(rindex uint64) (int64, int64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	var segmentEndPosition, indexEndPosition int64
	if rindex < s.FirstRindex || rindex > s.LastRindex {
		glog.Errorf("rindex is out of range,rindex=%d,s.FirstRindex=%d,s.LastRindex=%d", rindex, s.FirstRindex, s.LastRindex)
		return -1, -1, ErrArgsNotAvailable
	}

	position := int64(rindex-s.FirstRindex) * IndexEntrySize
	ies, err := s.IndexFile.getIndexEntries(int64(position), 1)
	if err != nil {
		glog.Errorf("getIndexEntries error,err=%s,position=%d,indeFile=%s", err.Error(), position, s.IndexFile.Name)
		return -1, -1, err
	}
	if len(ies) == 0 {
		glog.Errorf("len(ies) is 0,position=%d,indexFile=%s", position, s.IndexFile.Name)
		return -1, -1, ErrRangeNotExist
	}

	segmentEndPosition = ies[0].Position + RecordHeaderSize + ies[0].Length - 1
	indexEndPosition = position + IndexEntrySize - 1

	return segmentEndPosition, indexEndPosition, nil
}

//获取rindex对应的最大vindex
func (s *Segment) GetMaxVindexByRindex(rindex uint64) (int64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	var maxVindex int64
	var maxRindex uint64
	maxVindex = -1

	if rindex < s.FirstRindex || rindex > s.LastRindex {
		glog.Errorf("rindex is out of range,rindex=%d,s.FirstRindex=%d,s.LastRindex=%d",
			rindex, s.FirstRindex, s.LastRindex)
		return -1, ErrArgsNotAvailable
	}

	for maxRindex = rindex; s.FirstRindex <= maxRindex; maxRindex-- {
		ies, err := s.getIndexEntries(RaftIndexType, maxRindex, maxRindex+1, noLimit)
		if err != nil {
			glog.Errorf("getIndexEntries by rindex error,err=%s,rindex=%d", err.Error(), maxRindex)
			return -1, err
		}
		if len(ies) == 0 {
			glog.Errorf("len(ies)==0,maxRindex=%d", maxRindex)
			return -1, ErrRangeNotExist
		}
		if 0 <= ies[0].Vindex {
			maxVindex = ies[0].Vindex
			break
		}
	}

	//该segment中没有vdl log，使用InitFirstVindex-1作为最后一个vindex
	if maxRindex < s.FirstRindex && maxVindex == -1 {
		maxVindex = s.InitFirstVindex - 1
	}

	return maxVindex, nil
}
