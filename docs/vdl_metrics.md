# VDL监控指标

## 1.Metrics分类

 

- 数量(count): 上报间隔差值，比如异常统计
- 瞬时量(count_instant)：上报时瞬时值
- 速率(meter): 单位秒，也就是tps。比如2秒上报一次，2秒内处理1000， tps为500
- 平均值(avg): 比如, 平均每个request的rt
- 累计量(accumulate)：从服务启动起的累计量。服务重启后重新计算



## 2.架构图

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fwa2nm9d0jj213s0md0us.jpg)



## 3.Metrics列表

 

| 模块         | 指标名字                      | 类型          | 指标含义                                                     | 实现情况 | 备注                                                         |
| ------------ | ----------------------------- | ------------- | ------------------------------------------------------------ | -------- | ------------------------------------------------------------ |
| Kafka Server | ks_write_req_tps              | tps           | Kafka Server收到的写请求数                                   | Y        | Batch算一次                                                  |
|              | ks_write_resp_tps             | tps           | Kafka Server回复数                                           | Y        | Batch算一次                                                  |
|              | ks_write_req_log_tps          | tps           | Kafka Server收到的写log请求数                                | Y        | 每条log算一次                                                |
|              | ks_write_req_msg_err_maxsize  | count         | 收到超过maxsize的消息数                                      | Y        | 每次请求，每个partition算一次如一个请求写3个patition，其中一个parition有两条消息异常，则只算一次 |
|              | ks_write_req_msg_err_other    | count         | 收到异常消息的数量（除超maxsize）异常意思是客户端发过来的消息异常，如crc、压缩等 | Y        | 同ks_write_req_msg_err_maxsize                               |
|              | ks_write_req_srv_err_noleader | count         | 非leader收到的写入请求数                                     | Y        | 同ks_write_req_msg_err_maxsize                               |
|              | ks_read_log_tps               | tps           | 消费tps                                                      | Y        | 每条log算一次                                                |
|              | ks_conn_count                 | count         | 连接Kafka Server的次数，累计量                               | Y        |                                                              |
|              | ks_conn_online_connection     | count_instant | 连接到kafka Server的connection数量                           | Y        |                                                              |
|              | ks_write_latency              | avg           | 写入latency                                                  | Y        |                                                              |
|              | ks_read_latency               | avg           | 读取latency                                                  | Y        |                                                              |
|              |                               |               |                                                              |          |                                                              |
| Raft         | raft_role_switch_count        | accumulate    | 主从切换的次数                                               | Y        | 每个raft group单独统计                                       |
|              | raft_latency_per_entry        | avg           | 写入每个entry的平均latency（从propose到commit）              | N        |                                                              |
|              |                               |               |                                                              |          |                                                              |
| Transport    | trans_send_bytes              | count         | 节点发送的网络字节数                                         | N        | 每个raft group单独统计                                       |
|              | trans_recv_bytes              | count         | 节点接收的网络字节数                                         | N        | 每个raft group单独统计                                       |
|              | trans_send_err                | count         | 节点发送失败的次数                                           | N        | 每个raft group单独统计                                       |
|              | trans_recv_err                | count         | 节点接收失败的次数                                           | N        | 每个raft group单独统计                                       |
|              | trans_round_trip_latency      | avg           | 节点间的Round-Trip-Time                                      | N        |                                                              |
|              |                               |               |                                                              |          |                                                              |
| Store        | st_read_from_file             | count         | 读取时，从磁盘读的次数                                       | N        | 用于观察非内存命中读的情况                                   |
|              | st_write_latency              | avg           | 每条log写入磁盘的latency                                     | N        |                                                              |
|              | st_write_bytes                | count         | 写入字节数                                                   | N        |                                                              |