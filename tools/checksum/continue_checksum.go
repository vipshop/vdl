// 从startIndex连续checksum,
// 现在总本记录checksum = checksum(前一第记录总本记录checksum + 本记录checksum)
// 只要不同节点的相同index有相同的checksum,则节点间数据一致
// 不能检查最后一个文件,因为此文件正在被写入

package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/vipshop/vdl/logstore"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	metaFile     = "checksum_meta.data"
	checksumFile = "checksum.log"
)

var Encoding = binary.BigEndian

var (
	startIndex    *uint64 = flag.Uint64("startIndex", 0, "the start index for checksum begin(exclude start index)")
	dataDir       *string = flag.String("dataDir", "", "log data dir")
	checksumDir   *string = flag.String("checksumDir", "", "log data dir")
	printPerCount *int    = flag.Int("printPerCount", 100000, "how count to print crc once")
)

type meta struct {
	Index                 uint64 `json:"index"`
	FilenameWithoutSuffix string `json:"filename"`
	Position              int64  `json:"position"`
	Crc                   string `json:"crc"`
}

func main() {

	flag.Parse()

	if len(*dataDir) == 0 {
		fmt.Printf("args error,use --help\n")
		return
	}

	cf, cfErr := os.OpenFile(path.Join(*checksumDir, checksumFile), os.O_CREATE|os.O_RDWR|os.O_SYNC|os.O_APPEND, 0600)
	if cfErr != nil {
		panic(cfErr)
	}
	defer cf.Close()

	var m *meta
	if isMetaFileExists() {
		m = loadMeta()
	} else {
		m = newMeta(*startIndex)
	}

	continueCrcStr := m.Crc
	readPosition := m.Position
	currentFilenameWithoutSuffix := m.FilenameWithoutSuffix
	firstRead := true

	for {
		if canRoll(currentFilenameWithoutSuffix) {
			break
		}
	}

	fmt.Printf("[main]:op segment file:%s\n", path.Join(*dataDir, currentFilenameWithoutSuffix+".log"))
	f, err := os.OpenFile(path.Join(*dataDir, currentFilenameWithoutSuffix+".log"), os.O_RDONLY, 0600)
	defer f.Close()
	if err != nil {
		panic(err.Error())
	}
	fileStat, errStat := f.Stat()
	if err != nil {
		panic(errStat.Error())
	}
	var count uint64 = 0
	for {
		// rolling
		if readPosition+logstore.RecordHeaderSize >= fileStat.Size() {
			if !canRoll(currentFilenameWithoutSuffix) {
				fmt.Printf("need at least two segment to check, waiting [readPosition+logstore.RecordHeaderSize >= fileStat.Size()] \n ")
				time.Sleep(5 * time.Second)
				continue
			}
			maybeNextSegment := fileNameWithoutSuffix(parseIndexName(currentFilenameWithoutSuffix+".idx") + 1)
			if fileutil.Exist(path.Join(*dataDir, maybeNextSegment+".log")) {
				fmt.Printf("segment have rolling by readPosition[%d]+logstore.RecordHeaderSize[%d] >= fileStat.Size()[%d]\n ",
					readPosition, logstore.RecordHeaderSize, fileStat.Size())
				f.Close()
				currentFilenameWithoutSuffix = maybeNextSegment
				readPosition = 0
				fmt.Printf("[main]:op segment file:%s\n", path.Join(*dataDir, currentFilenameWithoutSuffix+".log"))
				f, err = os.OpenFile(path.Join(*dataDir, currentFilenameWithoutSuffix+".log"), os.O_RDONLY, 0600)
				if err != nil {
					panic(err.Error())
				}
				fileStat, errStat = f.Stat()
				if err != nil {
					panic(errStat.Error())
				}
				continue
			} else {
				fmt.Printf("waiting segment file rolling \n ")
				time.Sleep(5 * time.Second)
				continue
			}
		}
		headerByte := make([]byte, logstore.RecordHeaderSize)
		f.ReadAt(headerByte, readPosition)
		dataLen := int64(binary.BigEndian.Uint64(headerByte[8:16]))
		crc := Encoding.Uint32(headerByte[:4])

		// maybe not data, maybe end of file
		if dataLen == 0 && crc == 0 {
			maybeNextSegment := fileNameWithoutSuffix(parseIndexName(currentFilenameWithoutSuffix+".idx") + 1)
			if fileutil.Exist(path.Join(*dataDir, maybeNextSegment+".log")) {
				if !canRoll(currentFilenameWithoutSuffix) {
					fmt.Printf("need at least two segment to check, waiting [dataLen == 0 && crc == 0 ] \n ")
					time.Sleep(5 * time.Second)
					continue
				}
				fmt.Printf("segment have rolling by data == 0 \n")
				f.Close()
				currentFilenameWithoutSuffix = maybeNextSegment
				readPosition = 0
				fmt.Printf("[main]:op segment file:%s\n", path.Join(*dataDir, currentFilenameWithoutSuffix+".log"))
				f, err = os.OpenFile(path.Join(*dataDir, currentFilenameWithoutSuffix+".log"), os.O_RDONLY, 0600)
				if err != nil {
					panic(err.Error())
				}
				fileStat, errStat = f.Stat()
				if err != nil {
					panic(errStat.Error())
				}
				continue
			} else {
				fmt.Printf("no record, waiting to fetch again \n ")
				time.Sleep(5 * time.Second)
				continue
			}

		}

		dataByte := make([]byte, logstore.RecordHeaderSize+dataLen)
		f.ReadAt(dataByte, readPosition)
		record, err := binaryToRecord(dataByte)
		if count%uint64(*printPerCount) == 0 {
			fmt.Printf("[main]: read record :%d\n", record.Rindex)
		}
		count++

		if err != nil {
			panic(err.Error())
		}
		if !firstRead && record.Rindex != 0 {
			newCrc := crc32.ChecksumIEEE([]byte(continueCrcStr + strconv.FormatInt(int64(record.Crc), 10)))
			continueCrcStr = strconv.FormatInt(int64(newCrc), 10)
		}
		firstRead = false

		if record.Rindex%uint64(*printPerCount) == 0 && record.Rindex != 0 {
			// save
			m.FilenameWithoutSuffix = currentFilenameWithoutSuffix
			m.Index = record.Rindex
			m.Position = readPosition
			m.Crc = continueCrcStr
			m.Save()

			// print
			printCheckSum(cf, m)
		}

		readPosition = readPosition + dataLen + logstore.RecordHeaderSize
	}

}

func canRoll(currentFilenameWithoutSuffix string) bool {

	maybeNextSegment := fileNameWithoutSuffix(parseIndexName(currentFilenameWithoutSuffix+".idx") + 1)
	if !fileutil.Exist(path.Join(*dataDir, maybeNextSegment+".log")) {
		return false
	}

	maybeNextTwoSegment := fileNameWithoutSuffix(parseIndexName(currentFilenameWithoutSuffix+".idx") + 2)
	if !fileutil.Exist(path.Join(*dataDir, maybeNextTwoSegment+".log")) {
		return false
	}

	return true
}

func printCheckSum(f *os.File, m *meta) {

	info := strconv.FormatUint(m.Index, 10) + " crc " + m.Crc + "\n"

	_, err := f.WriteString(info)
	if err != nil {
		panic(err.Error())
	}

}

func isMetaFileExists() bool {
	return fileutil.Exist(path.Join(*checksumDir, metaFile))
}

func loadMeta() *meta {

	data, err := ioutil.ReadFile(path.Join(*checksumDir, metaFile))
	if err != nil {
		panic(err)
	}
	m := meta{}
	err = json.Unmarshal(data, &m)
	if err != nil {
		panic(err)
	}
	return &m
}

func newMeta(index uint64) *meta {

	rangeInfo, err := logstore.NewRangeFile(*dataDir).GetRangeInfo()
	var segmentStartIndex uint64
	var matchSegmentName string
	for key, logRange := range rangeInfo.NameToRange {
		if index > logRange.LastRindex {
			continue
		}
		if index >= logRange.FirstRindex {
			segmentStartIndex = logRange.FirstRindex
			matchSegmentName = key

			fmt.Printf("find match from range info, segmentStartIndex:%d, matchSegmentName:%s\n",
				segmentStartIndex, matchSegmentName)
			break
		}

	}

	// find from last segment
	if matchSegmentName == "" {
		files, err := ioutil.ReadDir(*dataDir)
		if err != nil {
			panic(err.Error())
		}
		var matchIndexFile os.FileInfo
		for i := len(files) - 1; i >= 0; i-- {
			if strings.HasSuffix(files[i].Name(), logstore.IndexFileSuffix) {
				matchIndexFile = files[i]
				break
			}
		}
		buf := make([]byte, logstore.IndexEntrySize)
		f, err := os.Open(path.Join(*dataDir, matchIndexFile.Name()))
		if err != nil {
			panic(err.Error())
		}
		f.ReadAt(buf, 0)
		f.Close()
		indexEntry := binaryToIndex(buf, true)
		matchSegmentName = segmentFileName(parseIndexName(matchIndexFile.Name()))
		segmentStartIndex = indexEntry.Rindex

		fmt.Printf("find match from last segment, segmentStartIndex:%d, matchSegmentName:%s\n",
			segmentStartIndex, matchSegmentName)
	}

	offset := (index - segmentStartIndex) * logstore.IndexEntrySize
	buf := make([]byte, logstore.IndexEntrySize)
	fmt.Printf("[newMeta]:op idx file:%s\n", path.Join(*dataDir, indexFileName(parseSegmentName(matchSegmentName))))
	f, err := os.Open(path.Join(*dataDir, indexFileName(parseSegmentName(matchSegmentName))))
	if err != nil {
		panic(err.Error())
	}
	f.ReadAt(buf, int64(offset))
	f.Close()
	indexEntry := binaryToIndex(buf, true)
	m := &meta{
		Index: index,
		FilenameWithoutSuffix: fileNameWithoutSuffix(parseSegmentName(matchSegmentName)),
		Position:              indexEntry.Position,
		Crc:                   "",
	}
	m.Save()

	return m
}

func (m *meta) Save() {
	fmt.Printf("[save]: meta:%v \n", m)
	resultBuf, err := json.Marshal(m)
	if err != nil {
		panic(err.Error())
	}
	err = ioutil.WriteFile(path.Join(*checksumDir, metaFile), resultBuf, fileutil.PrivateFileMode)
	if err != nil {
		panic(err.Error())
	}
}

func binaryToIndex(b []byte, checkCrc bool) *logstore.IndexEntry {
	index := new(logstore.IndexEntry)
	index.Vindex = int64(Encoding.Uint64(b[4:12]))
	index.Rindex = Encoding.Uint64(b[12:20])
	index.Position = int64(Encoding.Uint64(b[20:28]))
	index.Length = int64(Encoding.Uint64(b[28:36]))
	//if not check crc, index.Crc is 0
	if checkCrc {
		index.Crc = Encoding.Uint32(b[:4])
		newCrc := crc32.ChecksumIEEE(b[4:])
		if index.Crc != newCrc {
			panic("index crc not match")
		}
	}
	return index
}

func parseSegmentName(str string) int64 {
	var index int64
	if !strings.HasSuffix(str, ".log") {
		panic("ErrBadSegmentName")
	}
	fmt.Sscanf(str, "%016x.log", &index)
	return index
}

func parseIndexName(str string) int64 {
	var index int64
	if !strings.HasSuffix(str, ".idx") {
		panic("ErrBadIndexName")
	}
	fmt.Sscanf(str, "%016x.idx", &index)
	return index
}

func segmentFileName(index int64) string {
	return fmt.Sprintf("%016x.log", index)
}

func indexFileName(index int64) string {
	return fmt.Sprintf("%016x.idx", index)
}

func fileNameWithoutSuffix(index int64) string {
	return fmt.Sprintf("%016x", index)
}

func binaryToRecord(b []byte) (*logstore.Record, error) {
	record := new(logstore.Record)
	record.Crc = Encoding.Uint32(b[:4])
	newCrc := crc32.ChecksumIEEE(b[4:])

	record.RecordType = int32(Encoding.Uint32(b[4:8]))
	record.DataLen = int64(Encoding.Uint64(b[8:16]))
	record.Vindex = int64(Encoding.Uint64(b[16:24]))

	record.Term = Encoding.Uint64(b[24:32])
	record.Rindex = Encoding.Uint64(b[32:40])
	//record.RaftType = raftpb.EntryType(Encoding.Uint32(b[40:44]))
	record.Data = b[44:]

	if record.Crc != newCrc {
		fmt.Printf("rindex=%d,record.DataLen=%d, data=%v,newCrc=%d,record.Crc=%d\n", record.Rindex, record.DataLen,
			string(record.Data), newCrc, record.Crc)
		return nil, errors.New("record crc32 not match")
	}

	return record, nil
}
