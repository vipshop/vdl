# Raft Group测试用例

## 列表

### RaftGroup Test
文件名：t_raft_group_test.go

| ID | 分类 | 子分类 | 状态 | 用例 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | 端到端测试 | Propose流程 | 已跑jenkines, stable | 三节点完整流程，默认配置 | 全新启动三个节点，完成5万次Propose，检查数据准备性 |
| 2 | 端到端测试 | Propose流程 | 已跑jenkines, stable | 三节点完整流程，Segmentsize为5M | 测试raft log会rolling的情况下，完成2万次Propose，检查数据准备性 |
| 3 | 端到端测试 | Fetch流程 | 已跑jenkines, stable | Raft Fetch Message时，使用随机MaxFetchBytes | 测试批量获取的正确性 |
| 4 | 端到端测试 | Propose与Fetch同时执行 | 已跑jenkines, stable | 并行Propose与Fetch | 校验Propose与Fetch流程互不干扰 |
| 5 | 端到端测试 | Raft流程 | 已跑jenkines, stable | Follow数据比Leader多 | 检查Follow数据是否被删除并替换（Raft算法的log delete流程） |
| 6 | 端对端测试 | Raft流程 | Unstable | 不断停止随机节点 | 不断停止随机节点,并发送数据,恢复节点,检查数据正常性 |
| 7 | 端对端测试 | Raft流程 | Unstable | 在生产数据的同时,不断停止leader节点 | 测试选举 + 服务正常 + 数据正确性 |
| 8 | 端对端测试 | Raft流程 | Unstable | 停止节点较长的时间,再恢复加入集群 | 测试落后较多的节点加入集群时的正确性 |
| 9 | 端对端测试 | Raft流程 | 已跑jenkines, stable | Snapshot测试 | 测试现VDL没有实现snapshot功能,但遇到需要snapshot功能的情况下,系统的表现 |
| 10 | 端对端测试 | Raft流程 | 已跑jenkines, stable | 发送消息大小超过设置消息大小 | 测试消息限制功能的正确性 |
| 11 | 端对端测试 | Raft流程 | Unstable | 测试发送大消息 | 测试VDL在生产大消息时的正确性 |
| 12 | 端对端测试 | Raft流程 | Unstable | 测试消费不存在的消息 | 测试VDL在消费不存在的消息时的预期表现 |
| 13 | 端对端测试 | Raft流程 | 已跑jenkines, stable | 测试VDL在正确退出时, apply index是否正确存储 | VDL在正常退出时,会存储完apply index再退出 |
| 14 | 端对端测试 | Raft流程 | 已跑jenkines, stable | 测试Batch生产 | 测试Batch生产的正确性 |

### FSM Test
文件名：t_raft_group_fsm_test.go

| ID | 分类 | 子分类 | 用例 | 说明 |
| --- | --- | --- | --- | --- |
| 1 | unit_test | ApplyNormalEntry | Apply Empty Entry | 校验当Apply Entry为空时的正确性 |
| 2 | unit_test | ApplyNormalEntry | Apply One Entry | 校验当Apply Entry为1个时的正确性 |
| 3 | unit_test | ApplyNormalEntry | Apply Multi-Entries | 校验当Apply Entry为多个时的正确性 |
| 4 | unit_test | ApplyNormalEntry | RE-Apply Entry | 校验当Apply Entry为中，有多个之前已经Apply过的情况下的正确性 |
| 5 | unit_test | ApplyNormalEntry | Apply Entry Error | 校验当Apply Entry中的第一个Entry Idx与Raft Next Apply Idx不匹配的情况(会产生Panic) |
| 6 | unit_test | ApplyNormalEntry | Save Apply Index By Count | 测试Apply Index数量大于配置的Save Apply Index Count时，需要保存Apply Index |
| 7 | unit_test | ApplyNormalEntry | Save Apply Index By Time | 测试超过Apply Index Duration间隔时，需要保存Apply Index |

### LogStream Wrapper Test
文件名：t_logstream_wrapper_test.go

| ID | 分类 | 子分类 | 用例 | 说明 |
| --- | --- | --- | --- | --- |
| 1 | 端到端测试 | GetVDLServerInfo | GetVDLServerInfo整体流程 | 校验GetVDLServerInfo正确性（包括正常、异常、Cache流程） |
| 1 | 端到端测试 | GetVDLServerInfo | GetVDLServerInfo并发测试 | 并发下GetVDLServerInfo接口的正确性 |


### Membership Integration Test
文件名：t_raft_group_membership_it_test.go

注: Membership测试是集成测试, 跑多个raft group进行测试,但未启动VDL Server,所以在测试添加节点时,只测试到增加节点命令执行成功,无法启动新Raft Group,因为添Raft Group加入集群需要用到VDL Server相关接口

| ID | 分类 | 子分类 | 用例 | 说明 |
| --- | --- | --- | --- | --- |
| 1 | IT | AddMemberSuccess | 添加节点 | 测试添加节点功能的正确性 |
| 2 | IT | RemoveFollowerSuccess | 删除Follower节点 | 测试删除Follower节点的正确性 |
| 3 | IT | RemoveLeaderSuccess | 删除Leader节点 | 测试删除Leader节点的正确性 |
| 4 | IT | UpdateFollowerSuccess | 更新节点 | 测试更新节点的正确性 |
| 5 | IT | AddMemberForSingleNodeCluster | 添加节点 | 测试是否能在单节点的集群上添加节点 |
| 6 | IT | AddMemberForHealthTwoNodeCluster | 添加节点 | 测试是能正常在健康的,2节点的集群上添加节点 |
| 7 | IT | AddMemberForUnHealthTwoNodeCluster | 添加节点 | 测试不能在不健康的,2节点的集群上添加节点 |
| 8 | IT | RemoveMemberForOneNodeCluster | 删除节点 | 测试不能在单节点的集群上删除节点 |
| 9 | IT | RemoveMemberForTwoNodeCluster | 删除节点 | 测试能在2节点的集群上删除节点 |
| 10 | IT | RemoveMemberForUnHealthThreeNodeCluster | 删除节点 | 测试不能在不健康,3节点的集群上删除节点 |
| 11 | IT | AddMemberWithSameUrl | 增加节点 | 测试要添加节点的url与集群中的相同,不能添加 |
| 12 | IT | UpdateMemberWithLeaderUrl | 更新节点 | 测试要更新节点的url与集群中的其它节点相同,不能更新 |
| 13 | IT | UpdateMemberWithSameUrl | 更新节点 | 测试要更新节点的url与之前的相同,可以更新 |


-------


