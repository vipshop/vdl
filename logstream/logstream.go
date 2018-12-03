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

package logstream

type Entry struct {
	Offset int64
	Data   []byte
}

//metadata for snapshot
type SegmentFile struct {
	//the segment raft log name
	SegmentName      string `json:"segment_name"`
	SegmentSizeBytes int64  `json:"segment_size_bytes"` //the max size of segment file
	//this segment is the last segment in snapshot or not
	//true:last segment, false means before last segment
	IsLastSegment bool `json:"is_last_segment"`
	//if IsLastSegment is true,only read EndPos(include).
	//Otherwise read the whole file
	EndPos int64 `json:"end_pos"`

	FirstVindex int64  `json:"first_vindex"`
	FirstRindex uint64 `json:"first_rindex"`
	LastVindex  int64  `json:"last_vindex"`
	LastRindex  uint64 `json:"last_rindex"`

	Checksum string `json:"segment_checksum"`
	Index    *IndexFile
}

type IndexFile struct {
	IndexName string `json:"index_name"`
	//if this index corresponding segment is last segment,only read EndPos(include).
	//Otherwise read the whole file.
	EndPos   int64  `json:"end_pos"`
	Checksum string `json:"index_checksum"`
}
