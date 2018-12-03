package main

import (
	"encoding/binary"

	"os"

	"time"

	"github.com/vipshop/vdl/consensus/raft/raftpb"
	. "github.com/vipshop/vdl/logstore"

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
	if IsFileExist(tmpDir) {
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

func newLogStore() (*LogStore, error) {
	var err error
	cfg := &LogStoreConfig{
		Dir: "/Users/flike/vdl",
		Meta: &FileMeta{
			VdlVersion: "1.0",
		},
		SegmentSizeBytes:    512 * 1024 * 1024,
		ReserveSegmentCount: 10,
		MemCacheSizeByte:    512 * 1024 * 1024,
	}
	s, err := NewLogStore(cfg)
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
	//indexFileSize, err := GetAvgFileSize(TmpDir, ".idx")
	//if err != nil {
	//	fmt.Printf("GetAvgFileSize error:%s\n", err.Error())
	//	return
	//}
	fmt.Printf("totalMessageCount=%d,perMessageSize=%d ,totalMessageSize=%fMB\n"+
		"batch=%d,totalCostTime=%fs\ntps=%.2f\nAvg:%.4fms\nMin:%.4fms\nMax:%.4fms\n",
		messageCount, *MsgSize, float64(int64(*MsgSize)*messageCount)/(1024*1024),
		*Batch, totalDuration, float64(messageCount)/totalDuration,
		totalDuration*1000/float64(messageCount), minDuration.Seconds()*1000/30000,
		maxDuration.Seconds()*1000/30000)

	if IsFileExist(TmpDir) {
		err := os.RemoveAll(TmpDir)
		if err != nil {
			fmt.Printf("Remove All error:%s\n", err.Error())
		}
	}

	return
}

//func GetAvgFileSize(directory string, suffix string) (float64, error) {
//	var totalFileSize int64
//	FilesNames := make([]string, 0, 5)
//	files, err := ioutil.ReadDir(directory)
//	if err != nil {
//		return -1, err
//	}
//	for _, file := range files {
//		if file.IsDir() {
//			continue
//		}
//		if strings.HasSuffix(file.Name(), suffix) == true {
//			FilesNames = append(FilesNames, file.Name())
//		}
//	}
//	for i := 0; i < len(FilesNames); i++ {
//		totalFileSize += getSize(path.Join(directory, FilesNames[i]))
//	}
//	avgFileSize := float64(totalFileSize) / (float64(len(FilesNames)) * 1024 * 1024) //MB
//	return avgFileSize, nil
//}
//
//func getSize(path string) int64 {
//	fileInfo, err := os.Stat(path)
//	if err != nil {
//		panic(err)
//	}
//	fileSize := fileInfo.Size()
//	return fileSize
//}
