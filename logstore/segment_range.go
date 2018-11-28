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
	"encoding/json"
	"io/ioutil"

	"sync"

	"path"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/vipshop/vdl/pkg/glog"
)

var (
	segmentRangeFile = "segment_range_meta.json"
)

type RangeFile struct {
	// the dir of range metadata file
	rangeFilePath string
	l             sync.Mutex
}

type RangeInfo struct {
	//key is segment name, value is index range
	NameToRange map[string]*LogRange `json:"range"`
}

type LogRange struct {
	FirstVindex int64  `json:"first_vindex"`
	FirstRindex uint64 `json:"first_rindex"`
	LastVindex  int64  `json:"last_vindex"`
	LastRindex  uint64 `json:"last_rindex"`
}

func NewRangeFile(dir string) *RangeFile {
	if len(dir) == 0 {
		glog.Fatal("[segment_range.go-NewRangeFile]:dir is nil")
	}
	r := &RangeFile{
		rangeFilePath: path.Join(dir, segmentRangeFile),
	}
	return r
}

func (m *RangeFile) GetRangeInfo() (*RangeInfo, error) {
	m.l.Lock()
	defer m.l.Unlock()

	var rangeInfo RangeInfo
	rangeInfo.NameToRange = make(map[string]*LogRange)
	if IsFileExist(m.rangeFilePath) {
		data, err := ioutil.ReadFile(m.rangeFilePath)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &rangeInfo); err != nil {
			return nil, err
		}
	}

	return &rangeInfo, nil
}

func (m *RangeFile) AppendLogRange(segmentName string, l *LogRange) error {
	m.l.Lock()
	defer m.l.Unlock()

	if len(segmentName) == 0 {
		glog.Warningf("[segment_range.go-AppendLogRange]:segmentName is nil")
		return nil
	}

	var rangeInfo RangeInfo
	rangeInfo.NameToRange = make(map[string]*LogRange)
	if IsFileExist(m.rangeFilePath) {
		data, err := ioutil.ReadFile(m.rangeFilePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &rangeInfo); err != nil {
			return err
		}
	}

	//追加新的segment range meta
	rangeInfo.NameToRange[segmentName] = l

	//写文件
	resultBuf, err := json.Marshal(rangeInfo)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(m.rangeFilePath, resultBuf, fileutil.PrivateFileMode)
	if err != nil {
		return err
	}
	return nil
}

func (m *RangeFile) DeleteLogRanges(deleteSegments []string) error {
	m.l.Lock()
	defer m.l.Unlock()

	var rangeInfo RangeInfo
	rangeInfo.NameToRange = make(map[string]*LogRange)
	if IsFileExist(m.rangeFilePath) == false {
		glog.Warningf("[segment_range.go-AppendLogRange]: segment range metadata file not exist,path=%s", m.rangeFilePath)
		return nil
	} else {
		data, err := ioutil.ReadFile(m.rangeFilePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &rangeInfo); err != nil {
			return err
		}
	}
	//删除segment metadata
	for _, name := range deleteSegments {
		delete(rangeInfo.NameToRange, name)
	}
	//写文件
	resultBuf, err := json.Marshal(rangeInfo)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(m.rangeFilePath, resultBuf, fileutil.PrivateFileMode)
	if err != nil {
		return err
	}
	return nil
}
