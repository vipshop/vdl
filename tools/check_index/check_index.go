//解析index file工具，
//使用方式：./main -path=/Users/flike/Downloads/00000000000007f9.idx -start=1 -count=10
//查看使用方式：./main --help
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"errors"
)

var Encoding = binary.BigEndian

var (
	indexFilePath  *string = flag.String("path", "", "index file path")
	startPos       *int    = flag.Int("start", 0, "the sequence number of index entry start to parse,if negative,parse by reversed order")
	maxCount       *int    = flag.Int("count", 0, "the count of parse index entries")
	IndexEntrySize int64   = 36
)

type IndexEntry struct {
	Crc      uint32
	Vindex   int64  //vdl log index
	Rindex   uint64 //raft log index
	Position int64  //raft log in the segment file position
	Length   int64  //the length of data field in record
}

func main() {
	var offset, totalSize int64
	var count int64
	flag.Parse()

	if len(*indexFilePath) == 0 {
		fmt.Printf("args error,use ./check_index --help\n")
		return
	}
	f, err := os.Open(*indexFilePath)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}

	if 0 <= *startPos {
		offset = int64(*startPos) * IndexEntrySize
	} else {
		lastPos, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			fmt.Printf("file seek error:%s\n", err.Error())
			return
		}
		offset = lastPos + int64(*startPos)*IndexEntrySize
	}

	buf := make([]byte, IndexEntrySize)
	for {
		n, err := f.ReadAt(buf, offset)
		if err != nil {
			//read to the end of file
			if err == io.EOF {
				//index file is unbroken
				if n == 0 {
					break
				} else if 0 < n {
					fmt.Printf("ReadAt offset=%d, exist torn write,"+
						"the last %d bytes is a torn write", offset, n)
					return
				}
			}
			fmt.Printf("ReadAt error:%s", err.Error())
			return
		}
		ie, err := binaryToIndex(buf, true)
		if err != nil {
			fmt.Printf("binaryToIndex error:%s,buf=%v", err.Error(), buf)
			return
		}
		count++
		totalSize += ie.Length
		fmt.Printf("offset=%d,vindex=%d,rindex=%d,recordPos=%d,recordLen=%d,totalRecordSize=%d\n",
			offset, ie.Vindex, ie.Rindex, ie.Position, ie.Length, totalSize)
		if 0 < *maxCount && count == int64(*maxCount) {
			break
		}
		offset += IndexEntrySize
	}
}

func binaryToIndex(b []byte, checkCrc bool) (*IndexEntry, error) {
	index := new(IndexEntry)
	index.Vindex = int64(Encoding.Uint64(b[4:12]))
	index.Rindex = Encoding.Uint64(b[12:20])
	index.Position = int64(Encoding.Uint64(b[20:28]))
	index.Length = int64(Encoding.Uint64(b[28:36]))
	//if not check crc, index.Crc is 0
	if checkCrc {
		index.Crc = Encoding.Uint32(b[:4])
		newCrc := crc32.ChecksumIEEE(b[4:])
		if index.Crc != newCrc {
			return nil, errors.New("crc32 not match")
		}
	}
	return index, nil
}
