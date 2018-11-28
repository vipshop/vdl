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
	"math"
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

//read in Disk
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
	segmentCount := 2
	GenerateSegments(s, *MsgSize, segmentCount)
	wg.Add(3)
	go writeRecords(s)
	go readInMemCache(s)
	go readInDisk(s)
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

func readInDisk(s *logstore.LogStore) {
	var totalDuration float64 //seconds
	var minDuration time.Duration
	var maxDuration time.Duration
	var messageCount int64
	firstRindex, err := s.FirstIndex()
	if err != nil {
		fmt.Printf("FirstIndex error:%s\n", err.Error())
		return
	}

	var perReadCount int64
	for {
		var RecordCount int64
		var rindexReadCount int

		mcFirstRindex := s.Mc.GetFirstRindex()
		var i uint64
		i = firstRindex

		startTime := time.Now()
		for {
			entries, err := s.Entries(i, i+uint64(*Batch), math.MaxUint64)
			if err != nil {
				fmt.Printf("Entries error:%s\n", err.Error())
				return
			}
			i += uint64(*Batch)
			//从头开始读，读到MemCache之前退出
			if i >= mcFirstRindex-uint64(*Batch)+1 {
				break
			}
			RecordCount += int64(len(entries))
			//每次读1000次，再退出
			rindexReadCount++
			if rindexReadCount >= 1000 {
				break
			}
		}

		duration := time.Since(startTime)
		messageCount += RecordCount
		perReadCount = RecordCount
		//set min duration
		if minDuration.Nanoseconds() == 0 {
			minDuration = duration
		} else if duration < minDuration {
			minDuration = duration
		}
		//set max duration
		if maxDuration.Nanoseconds() == 0 {
			maxDuration = duration
		} else if maxDuration < duration {
			maxDuration = duration
		}
		totalDuration += duration.Seconds()
		if float64(*Duration) < totalDuration {
			break
		}
		if i >= mcFirstRindex-uint64(*Batch)+1 {
			break
		}
	}
	fmt.Printf("==============readInDisk begin================\n")
	fmt.Printf("totalMessageCount=%d,perMessageSize=%d ,totalMessageSize=%fMB\n"+
		"batch=%d,totalCostTime=%fs\ntps=%.2f\nAvg:%.4fms\nMin:%.4fms\nMax:%.4fms\n",
		messageCount, *MsgSize, float64(int64(*MsgSize)*messageCount)/(1024*1024),
		*Batch, totalDuration, float64(messageCount)/totalDuration,
		totalDuration*1000/float64(messageCount), minDuration.Seconds()*1000/float64(perReadCount),
		maxDuration.Seconds()*1000/float64(perReadCount))
	fmt.Printf("==============readInDisk end================\n")
	//quit
	wg.Done()

}

func writeRecords(s *logstore.LogStore) {
	var totalDuration float64 //seconds
	var minDuration time.Duration
	var maxDuration time.Duration
	var messageCount int64

	for {
		lastIndex, err := s.LastIndex()
		if err != nil {
			fmt.Printf("s.LastIndex error:%s\n", err.Error())
			return
		}
		start := lastIndex + 1
		entries := newEntries(start, *MsgSize, 30000)
		if err != nil {
			fmt.Printf("newEntries error:%s\n", err.Error())
			return
		}
		startTime := time.Now()
		for i := 0; i < 30000/(*Batch); i++ {
			tmpEntries := entries[i*(*Batch) : (i+1)*(*Batch)]
			err = s.StoreEntries(tmpEntries)
			if err != nil {
				fmt.Printf("StoreEntries:%s\n", err.Error())
				return
			}
		}
		duration := time.Since(startTime)
		messageCount += 30000
		//set min duration
		if minDuration.Nanoseconds() == 0 {
			minDuration = duration
		} else if duration < minDuration {
			minDuration = duration
		}
		//set max duration
		if maxDuration.Nanoseconds() == 0 {
			maxDuration = duration
		} else if maxDuration < duration {
			maxDuration = duration
		}
		totalDuration += duration.Seconds()
		if float64(*Duration) < totalDuration {
			break
		}
		totalSize := float64(int64(*MsgSize)*messageCount) / (1024 * 1024) //MB
		if float64(200000) < totalSize {
			break
		}
	}
	fmt.Printf("==============writeRecords begin================\n")
	fmt.Printf("totalMessageCount=%d,perMessageSize=%d ,totalMessageSize=%fMB\n"+
		"batch=%d,totalCostTime=%fs\ntps=%.2f\nAvg:%.4fms\nMin:%.4fms\nMax:%.4fms\n",
		messageCount, *MsgSize, float64(int64(*MsgSize)*messageCount)/(1024*1024),
		*Batch, totalDuration, float64(messageCount)/totalDuration,
		totalDuration*1000/float64(messageCount), minDuration.Seconds()*1000/30000,
		maxDuration.Seconds()*1000/30000)
	fmt.Printf("==============writeRecords end================\n")
	//quit
	wg.Done()
}
