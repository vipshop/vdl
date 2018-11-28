package main

import (
	"encoding/binary"
	"os"
	"time"

	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/logstore"

	"flag"
	"fmt"
	"log"
	"sync"
)

var (
	TmpDir        = "/Users/flike/vdl"
	MsgSize  *int = flag.Int("size", 100, "message size")
	Batch    *int = flag.Int("batch", 30, "batch count")
	Duration *int = flag.Int("time", 5, "the seconds of write test duration")
	wg       sync.WaitGroup
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

//read in MemCache
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
	segmentCount := 3
	GenerateSegments(s, *MsgSize, segmentCount)
	wg.Add(1)
	go readInMemCache(s)
	wg.Wait()
	if logstore.IsFileExist(TmpDir) {
		err := os.RemoveAll(TmpDir)
		if err != nil {
			fmt.Printf("Remove All error:%s\n", err.Error())
		}
	}

	return
}

func readInMemCache(s *logstore.LogStore) {
	var maxDuration, minDuration, totalDuration int64
	var totalReadRecordsCount int64
	var readRecordsOnce int64
	for {
		var perDuration int64
		var perReadRecordsCount int64
		var perRead int
		for {
			startTime := time.Now()
			entries, err := s.GetEntriesInMemCache(*Batch)
			if err != nil {
				fmt.Printf("Entries error:%s\n", err.Error())
				return
			}
			duration := time.Since(startTime)
			perDuration += duration.Nanoseconds() //ns
			perReadRecordsCount += int64(len(entries))
			if perRead >= 10000 {
				//set min duration
				if minDuration == 0 {
					minDuration = perDuration
				} else if perDuration < minDuration {
					minDuration = perDuration
				}
				//set max duration
				if maxDuration == 0 {
					maxDuration = perDuration
				} else if maxDuration < perDuration {
					maxDuration = perDuration
				}

				totalDuration += perDuration
				totalReadRecordsCount += perReadRecordsCount
				readRecordsOnce = perReadRecordsCount
				break
			}
			perRead++
		}
		if float64(*Duration) <= float64(totalDuration)/(1000*1000*1000) {
			break
		}
	}
	fmt.Printf("=================readInMemCache begin===================\n")
	fmt.Printf("totalMessageCount=%d,perMessageSize=%d ,totalMessageSize=%fMB\n"+
		"batch=%d,totalCostTime=%fs\ntps=%.2f\nAvg:%.4fms\nMin:%.4fms\nMax:%.4fms\n",
		totalReadRecordsCount, *MsgSize, float64(int64(*MsgSize)*totalReadRecordsCount)/(1024*1024),
		*Batch, float64(totalDuration)/(1000*1000*1000),
		float64(totalReadRecordsCount)/(float64(totalDuration)/(1000*1000*1000)),
		(float64(totalDuration)/(1000*1000*1000))/float64(totalReadRecordsCount),
		(float64(minDuration)/(1000*1000*1000))/float64(readRecordsOnce),
		(float64(maxDuration)/(1000*1000*1000))/float64(readRecordsOnce))
	fmt.Printf("=================readInMemCache end===================\n")
	//quit
	wg.Done()
}
