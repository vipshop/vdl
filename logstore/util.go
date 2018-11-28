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
	"fmt"

	"strings"

	"os"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/logstream"
	"github.com/vipshop/vdl/pkg/glog"
)

var (
	IndexFileSuffix = ".idx"
	LogFileSuffix   = ".log"
)

//dir not exist or no files in this dir,return false
func existFile(dirpath string) bool {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return false
	}
	return len(names) != 0
}

func indexFileName(index int64) string {
	return fmt.Sprintf("%016x.idx", index)
}

func segmentFileName(index int64) string {
	return fmt.Sprintf("%016x.log", index)
}

func getIndexNameBySegmentName(s string) string {
	if strings.HasSuffix(s, ".log") {
		s := strings.Split(s, ".")
		return s[0] + ".idx"
	}
	return ""
}

func getSegmentByIndexName(s string) string {
	if strings.HasSuffix(s, ".idx") {
		s := strings.Split(s, ".")
		return s[0] + ".log"
	}
	return ""
}

func generateNextSegmentName(str string) string {
	index, err := parseSegmentName(str)
	if err != nil {
		glog.Fatalf("[util.go-generateNextSegmentName]: error : %v, param str: %s", err, str)
	}
	return segmentFileName(index + 1)
}
func parseSegmentName(str string) (int64, error) {
	var index int64
	if !strings.HasSuffix(str, ".log") {
		return 0, ErrBadSegmentName
	}
	_, err := fmt.Sscanf(str, "%016x.log", &index)
	return index, err
}

func parseIndexName(str string) (int64, error) {
	var index int64
	if !strings.HasSuffix(str, ".idx") {
		return 0, ErrBadIndexName
	}
	_, err := fmt.Sscanf(str, "%016x.idx", &index)
	return index, err
}

func binaryToMeta(b []byte) (*FileMeta, error) {
	if len(b) == 0 {
		return nil, ErrArgsNotAvailable
	}
	return &FileMeta{
		VdlVersion: string(b),
	}, nil
}

func metaToBinary(meta *FileMeta) []byte {
	if meta == nil || len(meta.VdlVersion) == 0 {
		return nil
	}
	b := []byte(meta.VdlVersion)
	return b
}

func IsFileExist(filePath string) bool {
	if len(filePath) == 0 {
		return false
	}
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		glog.Errorf("[util.go-IsFileExist]:stat error:error=%s,dir=%s", err.Error(), filePath)
		return false
	}
	return true
}

func getMaxVindex(segments []*Segment) int64 {
	var maxVindex int64
	maxVindex = -1
	for _, segment := range segments {
		lastVindex := segment.GetLastVindex()
		if maxVindex < lastVindex {
			maxVindex = lastVindex
		}
	}
	return maxVindex
}

func getMinVindex(segments []*Segment) int64 {
	var minVindex int64
	minVindex = -1
	for _, segment := range segments {
		firstVindex := segment.GetFirstVindex()
		if 0 <= firstVindex {
			minVindex = firstVindex
			return minVindex
		}
	}
	return minVindex
}

func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func createRaftEntriesSlice(maxCount uint64) []raftpb.Entry {
	if maxSliceSize < maxCount {
		return make([]raftpb.Entry, 0, maxSliceSize)
	} else {
		return make([]raftpb.Entry, 0, maxCount)
	}
}

func createLogStreamEntriesSlice(maxCount uint64) []logstream.Entry {
	if maxSliceSize < maxCount {
		return make([]logstream.Entry, 0, maxSliceSize)
	} else {
		return make([]logstream.Entry, 0, maxCount)
	}
}
