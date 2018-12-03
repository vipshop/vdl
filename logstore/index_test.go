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
	"os"
	"path"
	"testing"
)

//32
func TestNewIndex(t *testing.T) {
	InitTmpVdlDir(t)
	cfg := &NewIndexConfig{
		Dir:  TmpVdlDir,
		Name: indexFileName(0),
	}

	index, err := newIndex(cfg)
	if err != nil {
		t.Fatalf("newIndex error:%s", err.Error())
	}
	defer func() {
		index.close()
		err = os.Remove(index.IndexPath)
		if err != nil {
			t.Fatalf("newIndex error:%s", err.Error())
		}
	}()

	if index.Name != indexFileName(0) {
		t.Fatalf("newIndex:index.Name=%s", index.Name)
	}
	if index.IndexPath != path.Join(TmpVdlDir, indexFileName(0)) {
		t.Fatalf("newIndex:index.IndexPath=%s", index.IndexPath)
	}
	if index.Position != 0 {
		t.Fatalf("newIndex:index.Position=%d", index.Position)
	}
}

func newPortionIndexEntry(t *testing.T, rindex uint64) []byte {
	ie := &IndexEntry{
		Vindex:   int64(rindex),
		Rindex:   rindex,
		Position: int64(rindex * 100),
		Length:   100,
	}
	b := make([]byte, IndexEntrySize)

	Encoding.PutUint64(b[4:12], uint64(ie.Vindex))
	Encoding.PutUint64(b[12:20], ie.Rindex)
	Encoding.PutUint64(b[20:28], uint64(ie.Position))
	Encoding.PutUint64(b[28:36], uint64(ie.Length))
	ie.Crc = crc32.ChecksumIEEE(b[4:])
	buf := indexToBinary(ie)
	buf = buf[:len(buf)-IndexEntrySize/2]
	return buf
}

//33
func TestWriteAndReadIndex(t *testing.T) {
	InitTmpVdlDir(t)
	cfg := &NewIndexConfig{
		Dir:  TmpVdlDir,
		Name: indexFileName(0),
	}
	index, err := newIndex(cfg)
	if err != nil {
		t.Fatalf("newIndex error:%s", err.Error())
	}

	records := newRecords(1, 100, t)
	position := uint64(0)
	err = index.writeEntriesByRecords(records, position)
	if err != nil {
		t.Fatalf("writeEntriesByRecords error:%s", err.Error())
	}
	//Close
	index.close()
	//reopen with read
	readCfg := &OpenIndexConfig{
		Dir:           TmpVdlDir,
		Name:          indexFileName(0),
		OpenIndexMode: ReadOnlyMode,
	}
	readIndex, err := openIndex(readCfg)
	if err != nil {
		t.Fatalf("openIndex error:%s", err.Error())
	}
	defer func() {
		readIndex.close()
		err = os.Remove(index.IndexPath)
		if err != nil {
			t.Fatalf("newIndex error:%s", err.Error())
		}
	}()
	parseResult, err := readIndex.parseIndex()
	if err != nil {
		t.Fatalf("parseIndex error:%s", err.Error())
	}
	for i := 1; i <= 100; i++ {
		ie, err := readEntryByRindex(parseResult.Entries, uint64(i))
		if err != nil {
			t.Fatalf("readEntryByRindex error:%s", err.Error())
		}
		if checkIndexEntry(ie, records[i-1]) == false {
			t.Fatalf("ie=%v,but record[i]=%v", ie, records[i])
		}
	}

}

//34
func TestGetIndexEntries(t *testing.T) {
	InitTmpVdlDir(t)
	cfg := &NewIndexConfig{
		Dir:  TmpVdlDir,
		Name: indexFileName(0),
	}
	index, err := newIndex(cfg)
	if err != nil {
		t.Fatalf("newIndex error:%s", err.Error())
	}

	records := newRecords(1, 100, t)
	position := uint64(0)
	err = index.writeEntriesByRecords(records, position)
	if err != nil {
		t.Fatalf("writeEntriesByRecords error:%s", err.Error())
	}
	//50th
	readPosition := 50 * IndexEntrySize
	indexEntries, err := index.getIndexEntries(int64(readPosition), 50)
	if err != nil {
		t.Fatalf("getIndexEntries error:%s", err.Error())
	}
	if len(indexEntries) != 50 {
		t.Fatalf("len(indexEntries) is %d, not is 50", len(indexEntries))
	}
	//check index entry
	for i := 0; i < 50; i++ {
		if checkIndexEntry(indexEntries[i], records[i+50]) == false {
			t.Fatalf("ie=%v,but record[i]=%v", indexEntries[i], records[i])
		}
	}
}

//35
func TestTornWrite(t *testing.T) {
	InitTmpVdlDir(t)
	cfg := &NewIndexConfig{
		Dir:  TmpVdlDir,
		Name: indexFileName(0),
	}
	index, err := newIndex(cfg)
	if err != nil {
		t.Fatalf("newIndex error:%s", err.Error())
	}

	records := newRecords(1, 10, t)
	err = index.writeEntriesByRecords(records, 0)
	if err != nil {
		t.Fatalf("index.IndexFile.File.Write error:%s", err.Error())
	}
	buf := newPortionIndexEntry(t, 11)
	//写入部分index entry
	_, err = index.IndexFile.File.WriteAt(buf, index.Position)
	if err != nil {
		t.Fatalf("index.IndexFile.File.Write error:%s", err.Error())
	}

	//Close
	index.close()
	//reopen with read
	readCfg := &OpenIndexConfig{
		Dir:           TmpVdlDir,
		Name:          indexFileName(0),
		OpenIndexMode: ReadOnlyMode,
	}
	readIndex, err := openIndex(readCfg)
	if err != nil {
		t.Fatalf("openIndex error:%s", err.Error())
	}
	defer func() {
		readIndex.close()
		err = os.Remove(index.IndexPath)
		if err != nil {
			t.Fatalf("newIndex error:%s", err.Error())
		}
	}()
	_, err = readIndex.parseIndex()
	if err != ErrTornWrite {
		t.Fatalf("parseIndex error:%s", err.Error())
	}
}

//36
func TestTruncateFileByPosition(t *testing.T) {
	InitTmpVdlDir(t)
	cfg := &NewIndexConfig{
		Dir:  TmpVdlDir,
		Name: indexFileName(0),
	}
	index, err := newIndex(cfg)
	if err != nil {
		t.Fatalf("newIndex error:%s", err.Error())
	}

	records := newRecords(0, 100, t)
	err = index.writeEntriesByRecords(records, 0)
	if err != nil {
		t.Fatalf("writeEntriesByRecords error:%s", err.Error())
	}
	newPos := 50 * IndexEntrySize
	err = index.truncateFileByPosition(int64(newPos))
	if err != nil {
		t.Fatalf("truncateFileByPosition error:%s", err.Error())
	}
	parseResult, err := index.parseIndex()
	if err != nil {
		t.Fatalf("parseIndex error:%s", err.Error())
	}
	if len(parseResult.Entries) != 50 {
		t.Fatalf("len(parseResult.Entries)=%d", len(parseResult.Entries))
	}
	if index.Position != 50*IndexEntrySize {
		t.Fatalf("index.Position=%d", index.Position)
	}
}

func checkIndexEntry(ie *IndexEntry, record *Record) bool {
	return ie.Rindex == record.Rindex &&
		ie.Vindex == record.Vindex && ie.Length == record.DataLen
}
