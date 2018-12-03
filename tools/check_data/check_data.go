package main

import (
	"flag"
	"fmt"
	"os"
	"syscall"
)

var (
	segmentFilePath *string = flag.String("path", "", "segment file path")
	startPos        *int64  = flag.Int64("start", 0, "the position of file to parse,must be positive")
	size            *int64  = flag.Int64("size", 0, "the size of print")
	MaxSegmentSize  int64   = 512 * 1024 * 1024
)

func main() {
	var offset int64
	var err error
	flag.Parse()

	if len(*segmentFilePath) == 0 {
		fmt.Printf("args error,use ./check_record --help\n")
		return
	}
	if (*startPos) < 0 {
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
	offset = *startPos

	if offset+(*size) < MaxSegmentSize {
		fmt.Printf("data=%v\n", logMapData[offset:offset+(*size)])
	} else {
		fmt.Printf("offset+size=%d,out of MaxSegmentSize=%d\n", offset+(*size), MaxSegmentSize)
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
