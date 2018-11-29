## 1.简介

VDL(Vip Distributed Log)的定位是：高吞吐、低延时的分布式日志存储，多副本、强一致性是其关键的特征。
这里的Log不是指syslog或者log4j产生的用于跟踪或者问题分析的应用程序日志。Log贯穿于互联网企业应用开发的方方面面，从DB的存储引擎、DB的复制、分布式一致性算法到消息系统。本质上都是Log的存储和分发。
从应用场景来看，VDL的应用包含下面三类：

* 复制状态机（Replicated State Machine） - 这一类应用主要使用VDL作为事务日志。
比如用于存储MySQL的Binlog，形成统一的Binlog服务层，简化数据库的备份、恢复、实例重建、failover等高频流程。
* 消息队列、消息发布订阅、流计算 - 这一类应用主要使用VDL来存储和传递消息。
我们可以基于VDL实现消息发布/订阅系统；同时也可以作为Storm/Spark的输入和输出，用于实时流计算的场景。
* 数据复制 - 这一类应用主要使用VDL来进行数据的复制。这个数据复制可能发生在本地机房，也可能是跨机房。
我们可以基于VDL构建我们强一致的数据库技术方案。

**外部应用通过Kafka协议来发送数据到VDL，VDL通过Raft协议来保证数据的强一致和高可靠。客户端通过Kafka协议来消费VDL中的数据。**

## 2.主要功能

1. 支持kafka协议生产和消费数据。
2. 数据强一致，raft协议保证。
3. 保证线性读一致性。
4. 部署运维简单，不依赖于其他外部组件

## 3.文档

### 3.1 VDL架构设计

[1.VDL架构设计](./docs/vdl_architecture.md)

[2.VDL性能测试](./docs/vdl_test.md)

[3.兼容kafka协议说明](./docs/kafka_protocol.md)

[4.VDL监控指标](./docs/vdl_metrics.md)

### 3.2 VDL部署与运维

[1.VDL的安装与部署](./docs/install_and_depoy.md)

[2.VDL的snapshot操作](./docs/snapshot_manual.md)

[3.VDL的Membership Change操作手册](./docs/member_ship_change.md)

[4.VDL的Leader Transfer操作手册](./docs/leader_transfer.md)

[5.VDL删除Log操作指南](./docs/delete_segment.md)

[6.VDL流控操作手册](./docs/flow_control.md)

## 4.开发与维护

VDL由唯品会基础架构部-数据与中间件组开发和维护。开发成员如下：

- [陈非](https://github.com/flike)
- [汤锦平](https://github.com/tom-tangjp)
- [赵百忠](https://github.com/firnsan)
- [范力彪](https://github.com/libiaofan)
- 龙永超
- 简怀兵

## 5.鸣谢

- 感谢**[etcd](https://github.com/etcd-io/etcd)**，VDL的Raft协议实现使用了[etcd](https://github.com/etcd-io/etcd)的raft库。

- 感谢**[jocko](https://github.com/travisjeffery/jocko)**，VDL使用了jocko解析kafka协议。


## 6.License

VDL 开源协议遵循Apache 2.0 license.详见LICENSE文件。