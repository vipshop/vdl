# Logstore单元测试用例

## logstore接口测试
文件名：log_store_test.go
### 创建logstore
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 1 | 创建Logstore | 无Segment文件的情况下创建Logstore |成功创建Logstore|
| 2 | 创建Logstore | 已经存在Segment文件的情况下创建Logstore |成功创建Logstore|
| 3 | 创建Logstore | 已经存在Segment文件，但不存在segment_range_meta.json的情况下创建Logstore |成功创建logstore,并创建segment_range_meta.json|
| 4 | 创建Logstore | 之前关闭的VDL segment版本信息是1.0，利用2.0版本信息创建logstore |成功创建logstore|

### FetchLogStreamMessages
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 5 | FetchLogStreamMessages | 限制获取vdl log范围，仅从MemCache获取，不限制vdl log size|成功获取对应的vdl log|
| 6 | FetchLogStreamMessages | 获取全部vdl log，磁盘segment+Memcache ，不限制vdl log size|成功获取对应的vdl log|
| 7 | FetchLogStreamMessages | 获取除了Memcache之外的全部vdl log ，不限制vdl log size|成功获取对应的vdl log|
| 18 | FetchLogStreamMessages | 先存储vdl log然后全部读取出来|存取vdl log都正常|

### GetVindexByRindex
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 8 | GetVindexByRindex | 根据rindex获取对应的vindex,分为以下几种情况：1.rindex超出范围。2.rindex正常|1.报错。2.成功获取到vindex|

### Entries
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 9 | Entries | 先存储raft log然后全部读取出来|存取raft log都正常|
| 10 | Entries | 限制获取raft log范围，仅从MemCache获取，不限制raft log size|成功获取对应的raft log|
| 11 | Entries | 获取全部raft log，磁盘segment+Memcache ，不限制raft log size|成功获取对应的vdl log|
| 12 | Entries | 获取除了Memcache之外的全部raft log ，不限制raft log size|成功获取对应的raft log|
| 20 | Entries | 仅从磁盘读取一个segment上的raft log|成功读取|
| 21 | Entries | 读取last segment之前的全部的raft log|成功读取|
| 24 | Entries | Entries读取raft log，限制raft log size|成功读取|

### Term
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 13 | Term | 根据rindex获取对应的Term|成功获取Term|

### DeleteRaftLog
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 14 | DeleteRaftLog | 删除start之后的全部raft log，start位于非最后一个segment|成功删除raft log|
| 15 | DeleteRaftLog | 删除start之后的全部raft log，start位于最后一个segment|成功删除raft log|
### MaxVindex
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 16 | MaxVindex | 获取rindex范围内的最大vindex|成功获取|
### MinVindex
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 17 | MinVindex | 获取rindex范围内的最小vindex|成功获取|
### DeleteFiles
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 19 | DeleteFiles | 删除第一个segment|成功删除|
### CreateSnapshotMeta
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 49 | CreateSnapshotMeta | 生成snapshot数据文件的元信息|成功生成|
### CheckAndRestoreDataDir
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 50 | CheckAndRestoreDataDir | 检查Data目录文件一致性|成功检查并移除不一致文件或信息|
## 其他
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 22 | RebuildIndexFile | 重建只读segment的index文件|成功重建|
| 23 | RebuildIndexFile | 重建读写segment（last segment）的index文件|成功重建|
## segment接口测试
文件名：segment_test.go
### 创建segment
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 24 | NewSegment | 创建segment|成功创建|
### 设置和读取segment meta
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 25 | 设置并读取segment meta | 先设置segment meta，然后在读取|成功设置和读取|
### ReadRecordsByVindex
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 26 | ReadRecordsByVindex | 根据vindex获取对应的record|成功读取|
### ReadRecordsByRindex
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 27 | ReadRecordsByRindex | 根据rindex获取对应的record|成功读取|
### DeleteRecordsByRindex
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 28，32 | DeleteRecordsByRindex | 删除start及之后的全部raft log|成功删除|
### getIndexEntries
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 29,30 | getIndexEntries | 根据范围读取对应的index entry|成功读取|
### GetReadSizeByRindex
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 47 | GetReadSizeByRindex | 测试读取rindex在segment和index中对应记录的结束位置|成功读取|
### GetReadSizeByRindex
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 48 | GetMaxVindexByRindex | 获取rindex对应的最大vindex|成功读取|
### 其他
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 31 | RebuildIndexFile | 重建读写segment（last segment）的index文件|成功重建|
## index接口测试
文件名：index_test.go
### 创建index文件
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 32 | newIndex | 重建读写segment（last segment）的index文件|成功重建|
###index文件读写
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 33,34 | 读写index entry |创建index文件，写入index entry，然后再读取|成功读写|
| 35 | index 文件Torn write检查 |创建index文件，写入index entry，将最后一个index entry截断部分|成功检查到index 文件存在Torn write|
### truncate index文件
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 36 | 根据指定position截断index 文件 | 截断文件后，能够读取出其他index entry|成功截断，并且读取到正确的剩余index entry|
## memory_cache接口测试
文件名：memory_cache_test.go
### memcache读写
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 37 | Memcache的读写 | 写入一批record，然后在读取|成功读取到写入的record|
### LoadRecords
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 38 | LoadRecords | 加载一批record，然后在读取|成功读取到加载的record|
### DeleteRecordsByRindex
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 39 | DeleteRecordsByRindex | 删除从start开始到最后的record，然后在读取|成功读取到剩余的record|
## segment_range接口测试
文件名：segment_range_test.go
### DeleteRecordsByRindex
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 40 | GetRangeInfo | segment_range_meta.json文件存在，读取范围信息|成功读取并解析index范围信息|
| 41 | GetRangeInfo | segment_range_meta.json文件不存在，读取范围信息|读取到的范围信息为空|
### AppendLogRange
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 42 | AppendLogRange | 追加一个segment的log range信息|成功追加并能够读取到追加的segment的log range信息|
###DeleteLogRanges
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 43 | DeleteLogRanges | 删除一个已经存在的segment的log range信息|成功删除该segment的log range信息|
## logstore recover接口测试
文件名：recover_test.go
###NeedRecover
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 44 | NeedRecover | 测试需要恢复的条件：1.文件不存在。2.文件中的flag不是正常退出标识|返回正确的bool类型|
###恢复索引
| ID | 用例 | 说明 |预期结果|
| --- | --- | --- |---|
| 45 | RecoverWithNoExitFlagFile | 无exit_flag_file文件|恢复最后一个index文件|
| 46 | TestRecoverWithUnnormalExit | exit_flag_file文件存在，里面标示为非正常退出|恢复最后一个index文件|

