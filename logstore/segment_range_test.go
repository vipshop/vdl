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
	"os"
	"path"
	"strconv"
	"testing"
)

//40
func TestGetRangeInfo(t *testing.T) {
	InitTmpVdlDir(t)
	rangeMeta := &RangeFile{
		rangeFilePath: path.Join(TmpVdlDir, segmentRangeFile),
	}

	ranges := RangeInfo{
		NameToRange: make(map[string]*LogRange),
	}
	for i := 0; i < 10; i++ {
		l := &LogRange{
			FirstVindex: int64(0 + i*100),
			FirstRindex: uint64(1 + i*100),
			LastVindex:  int64(10 + i*100),
			LastRindex:  uint64(20 + i*100),
		}
		key := strconv.Itoa(i)
		ranges.NameToRange[key] = l
	}
	for name, r := range ranges.NameToRange {
		err := rangeMeta.AppendLogRange(name, r)
		if err != nil {
			t.Fatalf("WriteRangeFile error:%s", err.Error())
		}
	}

	//get range meta from file
	r, err := rangeMeta.GetRangeInfo()
	if err != nil {
		t.Fatalf("WriteRangeFile error:%s", err.Error())
	}
	if len(r.NameToRange) != len(ranges.NameToRange) {
		t.Fatalf("len(rangeMeta.NameToRange)=%d, but len(r.NameToRange)=%d",
			len(r.NameToRange), len(ranges.NameToRange))
	}
	for k, v := range r.NameToRange {
		if newValue, ok := ranges.NameToRange[k]; ok {
			if equal(newValue, v) == false {
				t.Fatalf("not equal")
			}
		} else {
			t.Fatalf("not exist")
		}
	}
}

//41
//file not exist
func TestGetRangeInfo2(t *testing.T) {
	InitTmpVdlDir(t)
	rangeMeta := &RangeFile{
		rangeFilePath: path.Join(TmpVdlDir, segmentRangeFile),
	}
	if IsFileExist(rangeMeta.rangeFilePath) {
		err := os.Remove(rangeMeta.rangeFilePath)
		if err != nil {
			t.Fatalf("Remove error:%s", err.Error())
		}
	}

	//get range meta from file
	r, err := rangeMeta.GetRangeInfo()
	if err != nil {
		t.Fatalf("WriteRangeFile error:%s", err.Error())
	}
	if len(r.NameToRange) != 0 {
		t.Fatalf("len(r.NameToRange) is not 0,is %d", len(r.NameToRange))
	}
}

//42
func TestAppendLogRange(t *testing.T) {
	InitTmpVdlDir(t)
	rangeMeta := &RangeFile{
		rangeFilePath: path.Join(TmpVdlDir, segmentRangeFile),
	}

	l := &LogRange{
		FirstVindex: int64(0 + 1*100),
		FirstRindex: uint64(1 + 1*100),
		LastVindex:  int64(10 + 1*100),
		LastRindex:  uint64(20 + 1*100),
	}
	err := rangeMeta.AppendLogRange("1", l)
	if err != nil {
		t.Fatalf("AppendLogRange error:%v", err)
	}
	rangeInfo, err := rangeMeta.GetRangeInfo()
	if err != nil {
		t.Fatalf("GetRangeInfo error:%v", err)
	}
	if len(rangeInfo.NameToRange) != 1 ||
		rangeInfo.NameToRange["1"].FirstRindex != l.FirstRindex ||
		rangeInfo.NameToRange["1"].FirstVindex != l.FirstVindex ||
		rangeInfo.NameToRange["1"].LastRindex != l.LastRindex ||
		rangeInfo.NameToRange["1"].LastVindex != l.LastVindex {
		t.Fatalf("len(rangeInfo.NameToRange)=%d,rangeInfo.NameToRange[1]=%v",
			len(rangeInfo.NameToRange), rangeInfo.NameToRange["1"])
	}
}

//43
func TestDeleteLogRanges(t *testing.T) {
	InitTmpVdlDir(t)
	rangeMeta := &RangeFile{
		rangeFilePath: path.Join(TmpVdlDir, segmentRangeFile),
	}

	ranges := RangeInfo{
		NameToRange: make(map[string]*LogRange),
	}
	for i := 0; i < 10; i++ {
		l := &LogRange{
			FirstVindex: int64(0 + i*100),
			FirstRindex: uint64(1 + i*100),
			LastVindex:  int64(10 + i*100),
			LastRindex:  uint64(20 + i*100),
		}
		key := strconv.Itoa(i)
		ranges.NameToRange[key] = l
	}
	for name, r := range ranges.NameToRange {
		err := rangeMeta.AppendLogRange(name, r)
		if err != nil {
			t.Fatalf("WriteRangeFile error:%s", err.Error())
		}
	}

	deleteSegment := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		key := strconv.Itoa(i)
		delete(ranges.NameToRange, key)
		deleteSegment = append(deleteSegment, key)
	}
	err := rangeMeta.DeleteLogRanges(deleteSegment)
	if err != nil {
		t.Fatalf("DeleteLogRanges error:%s", err.Error())
	}
	//get range meta from file
	r, err := rangeMeta.GetRangeInfo()
	if err != nil {
		t.Fatalf("WriteRangeFile error:%s", err.Error())
	}

	if len(r.NameToRange) != len(ranges.NameToRange) {
		t.Fatalf("len(rangeMeta.NameToRange)=%d, but len(r.NameToRange)=%d",
			len(r.NameToRange), len(ranges.NameToRange))
	}
	for k, v := range r.NameToRange {
		if newValue, ok := ranges.NameToRange[k]; ok {
			if equal(newValue, v) == false {
				t.Fatalf("not equal")
			}
		} else {
			t.Fatalf("not exist")
		}
	}
}

func equal(left, right *LogRange) bool {
	return left.FirstRindex == right.FirstRindex &&
		left.LastRindex == right.LastRindex &&
		left.FirstVindex == right.FirstVindex &&
		left.LastVindex == right.LastVindex
}
