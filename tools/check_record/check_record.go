//解析segment file工具，
//使用方式：./main -path=/Users/flike/Downloads/00000000000007f9.log -start=1 -count=10
//查看使用方式：./main --help
package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"os"
	"syscall"
)

var Encoding = binary.BigEndian

var (
	segmentFilePath  *string = flag.String("path", "", "segment file path")
	startPos         *uint64 = flag.Uint64("start", 0, "the rindex in this segment,must be positive")
	maxCount         *int    = flag.Int("count", 0, "the count of parse records")
	RecordHeaderSize int64   = 44
	MaxSegmentSize   int64   = 512 * 1024 * 1024
)

const (
	MetaDataType int32 = iota
	RaftLogType
	VdlLogType
)

var (
	RecordTypeMap = map[int32]string{
		MetaDataType: "MetaDataType",
		RaftLogType:  "RaftLogType",
		VdlLogType:   "VdlLogType",
	}

	RaftTypeMap = map[int32]string{
		EntryNormal:     "EntryNormal",
		EntryConfChange: "EntryConfChange",
	}
)

const (
	EntryNormal     int32 = 0
	EntryConfChange int32 = 1
)

//Crc|RecordType|DataLen|Vindex|Term|Rindex|RaftType|Data
type Record struct {
	Crc        uint32 // crc for the remainder field
	RecordType int32  //
	DataLen    int64  //data length
	Vindex     int64

	Term     uint64
	Rindex   uint64
	RaftType int32
	Data     []byte
}

func binaryToRecord(b []byte) (*Record, error) {
	record := new(Record)
	record.Crc = Encoding.Uint32(b[:4])
	newCrc := crc32.ChecksumIEEE(b[4:])
	if record.Crc != newCrc {
		fmt.Printf("data=%v,newCrc=%d,record.Crc=%d\n", b, newCrc, record.Crc)
		return nil, errors.New("crc32 not match")
	}

	record.RecordType = int32(Encoding.Uint32(b[4:8]))
	record.DataLen = int64(Encoding.Uint64(b[8:16]))
	record.Vindex = int64(Encoding.Uint64(b[16:24]))

	record.Term = Encoding.Uint64(b[24:32])
	record.Rindex = Encoding.Uint64(b[32:40])
	record.RaftType = int32(Encoding.Uint32(b[40:44]))
	record.Data = b[44:]

	return record, nil
}

func main() {
	var offset int64
	var count int64
	var record *Record
	var err error
	flag.Parse()

	if len(*segmentFilePath) == 0 {
		fmt.Printf("args error,use ./check_record --help\n")
		return
	}
	if (*startPos) < 0 || (*maxCount) <= 0 {
		fmt.Printf("start and maxCount must be positive,use ./check_record --help\n")
		return
	}
	f, err := os.OpenFile(*segmentFilePath, os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	logMapData, err := syscall.Mmap(int(f.Fd()), 0, int(MaxSegmentSize),
		syscall.PROT_READ, syscall.MAP_SHARED|syscall.MAP_NORESERVE)
	if err != nil {
		fmt.Printf("mmap error,err=%s\n", err.Error())
		return
	}
	//输出相关信息
	for offset < MaxSegmentSize {
		if MaxSegmentSize < offset+RecordHeaderSize {
			break
		}
		RecordHeader := logMapData[offset : offset+RecordHeaderSize]
		dataLen := int64(Encoding.Uint64(RecordHeader[8:16]))

		if dataLen < 0 || MaxSegmentSize < offset+RecordHeaderSize+dataLen {
			break
		}
		recordBuf := logMapData[offset : offset+RecordHeaderSize+dataLen]
		record, err = binaryToRecord(recordBuf)
		if err != nil {
			fmt.Printf("binaryToRecord error:%s\n", err.Error())
			return
		}
		if count == int64(*maxCount) {
			break
		}
		if *startPos <= record.Rindex {
			record.Print()
			count++
		}

		offset += RecordHeaderSize + dataLen
	}

	err = syscall.Munmap(logMapData)
	if err != nil {
		fmt.Printf("Munmap error,err=%s\n", err.Error())
	}
	err = f.Close()
	if err != nil {
		fmt.Printf("file Close error,err=%s\n", err.Error())
	}
}

func (r *Record) Print() {
	fmt.Printf("Record.Crc=%d,Record.RecordType=%s,Record.DataLen=%d,Record.Vindex=%d,"+
		"Record.Term=%d,Record.Rindex=%d,Record.RaftType=%s,Record.Data=%v\n",
		r.Crc, RecordTypeMap[r.RecordType], r.DataLen, r.Vindex, r.Term, r.Rindex, RaftTypeMap[r.RaftType], r.Data)
}
