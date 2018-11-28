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
	"errors"
)

var (
	ErrFileNotFound         = errors.New("logstore: file not found")
	ErrArgsNotAvailable     = errors.New("logstore: args not available")
	ErrOutOfRange           = errors.New("logstore:starVindex is out of range")
	ErrFileExist            = errors.New("logstore:file already exists")
	ErrFileNotExist         = errors.New("logstore:the file or dir not exists")
	ErrEntryNotExist        = errors.New("logstore:entry not exists")
	ErrCrcNotMatch          = errors.New("logstore: Crc32 values do not match")
	ErrFieldNotMatch        = errors.New("logstore: index entry field do not match record")
	ErrBadSegmentName       = errors.New("logstore:bad segment name")
	ErrBadIndexName         = errors.New("logstore:bad index name")
	ErrNoFileMeta           = errors.New("logstore:segment has no metadata")
	ErrNotAllowDelete       = errors.New("logstore:segment doesn't allow to delete")
	ErrNotAllowWrite        = errors.New("logstore:segment doesn't allow to write")
	ErrRangeNotExist        = errors.New("logstore:out of the range of logstore")
	ErrTornWrite            = errors.New("logstore: file exist an incomplete write in the end")
	ErrSegmentClosed        = errors.New("logstore: the segment is closed")
	ErrRangeMetadataDestory = errors.New("logstore: the range metadata file is destory")
	ErrNotContinuous        = errors.New("logstore:segments are not continuous")
)
