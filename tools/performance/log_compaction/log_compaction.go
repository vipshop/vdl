package main

import (
	"encoding/binary"
	"os"

	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/logstore"

	"flag"
	"fmt"
	"log"
)

var (
	TmpDir        = "/Users/flike/vdl"
	MsgSize  *int = flag.Int("size", 100, "message size")
	Batch    *int = flag.Int("batch", 30, "batch count")
	Duration *int = flag.Int("time", 5, "the seconds of write test duration")
)

func InitTmpVdlDir(tmpDir string) {
	if logstore.IsFileExist(tmpDir) {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			log.Panicf("Remove All error:%s\n", err.Error())
		}
	}
	err := os.MkdirAll(tmpDir, 0700)
	if err != nil {
		log.Panicf("Remove All error:%s\n", err.Error())
	}
}

func newLogStore() (*logstore.LogStore, error) {
	var err error
	cfg := &logstore.LogStoreConfig{
		Dir: "/Users/flike/vdl",
		Meta: &logstore.FileMeta{
			VdlVersion: "1.0",
		},
		SegmentSizeBytes:    512 * 1024 * 1024,
		ReserveSegmentCount: 10,
		MemCacheSizeByte:    512 * 1024 * 1024,
	}
	s, err := logstore.NewLogStore(cfg)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func newEntries(start uint64, size, count int) []raftpb.Entry {
	entries := make([]raftpb.Entry, 0, 10)
	for i := 0; i < count; i++ {
		entry := raftpb.Entry{
			Term:  1,
			Index: start + uint64(i),
			Type:  raftpb.EntryNormal,
			Data:  make([]byte, 8),
		}
		binary.BigEndian.PutUint64(entry.Data, start+uint64(i)) //reqId equal Index
		data := make([]byte, size-8)
		entry.Data = append(entry.Data, data...)
		entries = append(entries, entry)
	}
	return entries
}

func GenerateSegments(s *logstore.LogStore, size, count int) {
	for len(s.Segments) < count {
		lastIndex, err := s.LastIndex()
		if err != nil {
			fmt.Printf("s.LastIndex error:%s", err.Error())
			return
		}
		start := lastIndex + 1
		entries := newEntries(start, size, 1000)
		if err != nil {
			fmt.Printf("newEntries error:%s", err.Error())
			return
		}
		err = s.StoreEntries(entries)
		if err != nil {
			fmt.Printf("newEntries error:%s", err.Error())
			return
		}
	}
}

func main() {
	InitTmpVdlDir(TmpDir)
	flag.Parse()
	s, err := newLogStore()
	if err != nil {
		fmt.Printf("NewLogStore error:%s\n", err.Error())
		return
	}
	defer func() {
		err = s.Close()
		if err != nil {
			fmt.Printf("LogStore.Close error:%s\n", err.Error())
		}
	}()
	segmentCount := 5
	GenerateSegments(s, *MsgSize, segmentCount)
	segmentNames := make([]string, 0, 1)
	segmentNames = append(segmentNames, s.Segments[0].GetName())
	//startTime := time.Now()
	s.DeleteFiles(segmentNames)
	//duration := time.Since(startTime)
	//fmt.Printf("Log Compaction:delete one segment cost: %fms\n", duration.Seconds()*1000)
	if logstore.IsFileExist(TmpDir) {
		err := os.RemoveAll(TmpDir)
		if err != nil {
			fmt.Printf("Remove All error:%s\n", err.Error())
		}
	}

	return
}
