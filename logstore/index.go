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
	"hash/crc32"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"

	//	"time"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/myos"
	"github.com/vipshop/vdl/vdlfiu"
	"vipshop.com/distributedstorage/fiu"
)

const (
	IndexEntrySize    = 36
	PageSize          = 4096
	IndexCountPerPage = PageSize / IndexEntrySize //113

)

type Index struct {
	Name      string
	IndexPath string

	IndexFile *fileutil.LockedFile
	Position  int64
}

type IndexEntry struct {
	Crc      uint32
	Vindex   int64  //vdl log index
	Rindex   uint64 //raft log index
	Position int64  //raft log in the segment file position
	Length   int64  //the length of data field in record
}

type NewIndexConfig struct {
	Dir  string
	Name string
}

//create a new index
func newIndex(cfg *NewIndexConfig) (*Index, error) {
	if len(cfg.Dir) == 0 || len(cfg.Name) <= 0 {
		glog.Errorf("[index.go-newIndex]:newIndex args errors,cfg.dir=%s,cfg.name=%s", cfg.Dir, cfg.Name)
		return nil, ErrArgsNotAvailable
	}
	index := new(Index)
	index.Name = cfg.Name

	cfg.Dir = filepath.Clean(cfg.Dir)
	index.IndexPath = filepath.Join(cfg.Dir, index.Name)
	if fileutil.Exist(index.IndexPath) {
		glog.Errorf("[index.go-newIndex]:index file:%v already exists", index.IndexPath)
		return nil, ErrFileExist
	}

	indexFile, err := fileutil.TryLockFile(index.IndexPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, fileutil.PrivateFileMode)
	if err != nil {
		glog.Errorf("[index.go-newIndex]:TryLockFile in newIndex error:error=%s,indexPath=%s", err.Error(), index.IndexPath)
		return nil, err
	}
	index.Position = 0
	index.IndexFile = indexFile

	return index, nil
}

type OpenIndexConfig struct {
	Name          string
	Dir           string
	OpenIndexMode OpenMode
}

//打开索引文件
func openIndex(cfg *OpenIndexConfig) (*Index, error) {
	var err error
	if len(cfg.Dir) == 0 || len(cfg.Name) == 0 {
		return nil, ErrArgsNotAvailable
	}

	cfg.Dir = path.Clean(cfg.Dir)
	indexPath := path.Join(cfg.Dir, cfg.Name)
	exist := IsFileExist(indexPath)
	if exist == false {
		glog.Errorf("[index.go-openIndex]:index file not exist,indexPath=%s", indexPath)
		return nil, ErrFileNotExist
	}

	index := new(Index)
	index.Name = cfg.Name
	index.IndexPath = indexPath
	index.IndexFile, err = fileutil.TryLockFile(indexPath, os.O_RDWR, fileutil.PrivateFileMode)
	if err != nil {
		glog.Errorf("[index.go-openIndex]:TryLockFile in openIndex error:error=%s,indexPath=%s", err.Error(), indexPath)
		return nil, err
	}
	index.Position, err = index.IndexFile.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return index, nil
}

//生成records对应的索引信息，并将其写入index文件
func (index *Index) writeEntriesByRecords(records []*Record, off uint64) error {
	recordsCount := len(records)
	if recordsCount == 0 {
		return nil
	}
	var crc uint32
	buf := make([]byte, 0, IndexEntrySize*recordsCount)
	b := make([]byte, IndexEntrySize)

	for i := 0; i < len(records); i++ {
		Encoding.PutUint64(b[4:12], uint64(records[i].Vindex))
		Encoding.PutUint64(b[12:20], records[i].Rindex)
		Encoding.PutUint64(b[20:28], uint64(off))
		Encoding.PutUint64(b[28:36], uint64(records[i].DataLen))
		crc = crc32.ChecksumIEEE(b[4:])
		Encoding.PutUint32(b[:4], crc)
		off = off + RecordHeaderSize + uint64(records[i].DataLen)
		buf = append(buf, b...)
	}
	_, err := index.IndexFile.WriteAt(buf, index.Position)
	if err != nil {
		glog.Fatalf("[index.go-writeEntriesByRecords]:write index entry error:%s,position=%d",
			err.Error(), index.Position)
	}

	err = myos.Syncfilerange(
		index.IndexFile.Fd(),
		index.Position,
		int64(len(buf)),
		myos.SYNC_FILE_RANGE_WRITE,
	)
	if err != nil {
		glog.Fatalf("[index.go-writeEntriesByRecords]:Syncfilerange error,err=%s, index name:%s, "+
			"start position :%d, len(buf) :%d, data: %s",
			err.Error(), index.Name, index.Position, len(buf), string(buf))
	}

	index.Position += int64(len(buf))

	//FIU test[24]，Leader最后的index文件数据不一致（部分写，将写入index的最后一条数据截断20字节）
	if fiu.IsSyncPointExist(vdlfiu.FiuIndexTornWrite2) {
		MockIndexTornWrite(index, 20)
	}

	return nil
}

//read an index entry by rindex, the index may open with read or write
func readEntryByRindex(entries []*IndexEntry, rindex uint64) (*IndexEntry, error) {
	length := len(entries)
	position := sort.Search(length, func(i int) bool {
		return entries[i].Rindex >= rindex
	})
	if position < length && entries[position].Rindex == rindex {
		return entries[position], nil
	}
	return nil, ErrEntryNotExist
}

type ParseIndexResult struct {
	Entries     []*IndexEntry
	FirstVindex int64
	FirstRindex uint64
	LastVindex  int64
	LastRindex  uint64
}

//读取索引文件，解析成索引项
func (index *Index) parseIndex() (r *ParseIndexResult, err error) {
	var offset int64
	var ie *IndexEntry

	buf := make([]byte, IndexEntrySize)
	r = new(ParseIndexResult)
	r.FirstRindex = 0
	r.LastRindex = 0
	r.FirstVindex = -1
	r.LastVindex = -1
	r.Entries = make([]*IndexEntry, 0, 1024)

	for {
		n, err := index.IndexFile.ReadAt(buf, offset)
		if err != nil {
			//read to the end of file
			if err == io.EOF {
				//index file is unbroken
				if n == 0 {
					break
				} else if 0 < n {
					glog.Errorf("[index.go-parseIndex]:ReadAt offset=%d, exist torn write,"+
						"the last %d bytes is a torn write", offset, n)
					return nil, ErrTornWrite
				}
			}
			glog.Errorf("[index.go-parseIndex]:ReadAt error:%s, indexFileName=%s",
				err.Error(), index.IndexFile.Name())
			return nil, err
		}
		ie, err = binaryToIndex(buf, false)
		if err != nil {
			glog.Errorf("[index.go-parseIndex]:binaryToIndex error:%s, indexFileName=%s,buf=%v",
				err.Error(), index.IndexFile.Name(), buf)
			return nil, err
		}

		if r.FirstRindex == 0 {
			r.FirstRindex = ie.Rindex
		}
		if r.LastRindex < ie.Rindex {
			r.LastRindex = ie.Rindex
		}

		if 0 <= ie.Vindex {
			if r.FirstVindex == -1 {
				r.FirstVindex = ie.Vindex
			}
			if r.LastVindex < ie.Vindex {
				r.LastVindex = ie.Vindex
			}
		}

		r.Entries = append(r.Entries, ie)
		offset = offset + int64(n)
	}

	glog.Infof("[index.go-parseIndex]:parse index file success, indexFileName=%s",
		index.IndexFile.Name())
	return r, nil
}

//从position位置读取count个indexEntry
func (index *Index) getIndexEntries(position int64, count uint64) ([]*IndexEntry, error) {
	buf := make([]byte, count*IndexEntrySize)
	_, err := index.IndexFile.ReadAt(buf, position)
	if err != nil {
		glog.Fatalf("[index.go-parseIndex]:ReadAt error:%s, indexFileName=%s",
			err.Error(), index.IndexFile.Name())
		return nil, err
	}
	ies, err := binaryToIndexs(buf, false)
	if err != nil {
		glog.Errorf("[index.go-parseIndex]:binaryToIndex error:%s, indexFileName=%s,buf=%v",
			err.Error(), index.IndexFile.Name(), buf)
		return nil, err
	}
	return ies, nil
}

func indexToBinary(ie *IndexEntry) []byte {
	b := make([]byte, IndexEntrySize)

	Encoding.PutUint64(b[4:12], uint64(ie.Vindex))
	Encoding.PutUint64(b[12:20], ie.Rindex)
	Encoding.PutUint64(b[20:28], uint64(ie.Position))
	Encoding.PutUint64(b[28:36], uint64(ie.Length))
	ie.Crc = crc32.ChecksumIEEE(b[4:])
	Encoding.PutUint32(b[:4], ie.Crc)

	return b
}

func binaryToIndexs(b []byte, checkCrc bool) ([]*IndexEntry, error) {
	var ie *IndexEntry
	var err error

	count := len(b) / IndexEntrySize
	ies := make([]*IndexEntry, 0, count)
	for i := 0; i < count; i++ {
		//do not check crc
		ie, err = binaryToIndex(b[i*IndexEntrySize:(i+1)*IndexEntrySize], checkCrc)
		if err != nil {
			glog.Fatalf("[index.go-binaryToIndexs]:binaryToIndex error,err=%s,b=%v",
				err.Error(), b[i*IndexEntrySize:(i+1)*IndexEntrySize])
		}
		ies = append(ies, ie)
	}
	return ies, nil
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
			return nil, ErrCrcNotMatch
		}
	}
	return index, nil
}

func (index *Index) truncateFileByPosition(position int64) (err error) {
	if position < 0 {
		glog.Errorf("[index.go-truncateFileByPosition]:truncateFileByPosition args error, position=%d", position)
		return ErrArgsNotAvailable
	}
	err = index.IndexFile.Truncate(position)
	if err != nil {
		return err
	}
	index.Position, err = index.IndexFile.Seek(position, io.SeekStart)
	if err != nil {
		return err
	}
	return nil
}

func (index *Index) sync() error {
	err := index.IndexFile.Sync()
	if err != nil {
		return err
	}
	return nil
}

//Close the index file
func (index *Index) close() error {
	index.IndexFile.Close()
	glog.Infof("[index.go-Close]:index[%s] has been closed", index.IndexPath)
	return nil
}

//just for fiu test,not use in production environment
func MockIndexTornWrite(index *Index, count int64) {
	lastPos := index.Position - count
	if lastPos < 0 {
		glog.Fatal("[util.go-MockIndexTornWrite]:lastPos is negative")
	}
	index.IndexFile.File.Truncate(lastPos)
	glog.Fatalf("[util.go-MockIndexTornWrite]:mock crash after torn write index file in fiu")
}
