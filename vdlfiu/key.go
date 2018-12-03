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

package vdlfiu

// stable store
const (
	// mock write blotdb write delay
	FiuStableStoreWriteDelay = "fiu_stable_store_write_delay"
	// mock read blotdb write delay
	FiuStableStoreReadDelay = "fiu_stable_store_read_delay"
	// mock write blotdb write error
	FiuStableStoreWriteError = "fiu_stable_store_write_error"
	// mock read blotdb write error
	FiuStableStoreReadError = "fiu_stable_store_read_error"
)

//logstore
const (
	//entries function call cost increase 3s (more than election-timeout(2s))
	FiuEntriesTimeout = "fiu_entries_timeout"
	//StoreEntries function call cost increase 3s(more than election-timeout(2s))
	FiuLeaderStoreEntries = "fiu_store_entries_timeout"
	//entries function call cost increase 1s - 3s
	FiuFollowerStoreEntries = "fiu_follower_store_entries"
	//mock just write one log into segment,then crash
	FiuSegmentTornWrite = "fiu_segment_torn_write"
	//mock just write log into segment,then crash
	FiuIndexTornWrite = "fiu_index_torn_write"
	//fetch vdl log function call increase 1s-3s
	FiuFetchVdlLog = "fiu_fetch_vdl_log"
	//mock write raft log into segment,but the last log write portion
	FiuSegmentTornWrite2 = "fiu_segment_torn_write_2"
	//mock write index entry into inde file,but the last entry write portion
	FiuIndexTornWrite2 = "fiu_index_torn_write_2"

	// mock log store entries interface return error
	FiuStoreEntriesError = "fiu_logstore_entries_error"
	// mock log store term interface return error
	FiuStoreTermError = "fiu_logstore_term_error"
	// mock log store LastIndex interface return error
	FiuStoreLastIndexError = "fiu_logstore_lastindex_error"
	// mock log store FirstIndex interface return error
	FiuStoreFirstIndexError = "fiu_logstore_firstindex_error"
	// mock log store StoreEntries interface return error
	FiuStoreStoreEntriesError = "fiu_logstore_storeentries_error"
	// mock log store FetchLogStreamMessages interface return error
	FiuStoreStoreFetchStreamMsgError = "fiu_logstore_fetch_logstream_msg_error"
	// mock log store MaxVindex interface return error
	FiuStoreStoreMaxVindexError = "fiu_logstore_max_vindex_error"
	// mock log store MinVindex interface return error
	FiuStoreStoreMinVindexError = "fiu_logstore_min_vindex_error"
	// mock log store GetVindexByRindex interface return error
	FiuStoreGetVindexByRindexError = "fiu_logstore_getvindexbyrindex_error"
)

// raft
const (
	// mock raft process not update the apply term, used to mock not apply no-op
	FiuRaftCannotUpdateApplyTerm = "fiu_raft_cannot_update_apply_term"
	// mock raft process not update the apply index
	FiuRaftCannotUpdateApplyIndex = "fiu_raft_cannot_update_apply_index"
	// mock fms conf change panic before persistent to bolt db
	FiuRaftConfChangePanicBeforePersistent = "fiu_raft_conf_change_panic_before_persistent"
	// mock fms conf change panic before persistent apply index into bolt db (but already persistent membership)
	FiuRaftConfChangePanicBeforeSaveApplyIndex = "fiu_raft_conf_change_panic_before_save_apply_idx"
)
