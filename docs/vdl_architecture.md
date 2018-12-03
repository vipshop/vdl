# VDL架构设计

## 1.背景

VDL的全称是：Vip Distributed Log。VDL的定位是：**高吞吐、低延时的分布式日志存储，多副本、强一致性是其关键的特征。**这里的Log不是指syslog或者log4j产生的用于跟踪或者问题分析的应用程序日志。Log贯穿于互联网企业应用开发的方方面面，从DB的存储引擎、DB的复制、分布式一致性算法到消息系统，本质上都是Log的存储和分发。

从业界技术趋势来看，互联网企业在强一致存储方面也进行了很多尝试，很多基础设施都基于强一致的存储服务：

-  Amazon在2017年的SIGMOD上公开了Aurora的技术细节，其设计哲学是Log Is Database；
-  Etcd，HashiCorp Consul本质上都是基于Raft的强一致分布式日志；
- 腾讯的PhxSQL其核心是基于Multi-Paxos的强一致分布式日志；
- 阿里的PolarDB，其核心的复制算法是Raft；
- PingCAP的TiDB，底层是TiKV，其核心也是基于Raft的强一致分布式日志。

LinkedIn的工程师提出：You can't fully understand databases, NoSQL stores, key value stores, replication, paxos, hadoop, version control, or almost any software system without understanding logs[[1\]](#_参考_10)。

​      可以看出日志是多么地重要。一个高吞吐、低延时、强一致性的分布式日志存储，是很多基础设施的基础，对于公司来说，也具有很重要的战略意义。



## 2.功能需求
### 2.1.需求定义
VDL产品形态说明书，明确了Log Stream的具体要求，总结如下。

明确的需求有：

- 复制状态机(RSM: Replicated State Machine) - 这一类应用主要使用VDL作为事务日志。在VIP RDP(Real-time Data Pipeline)的第二阶段，明确需要VDL做为RDP的强一致日志存储。
- 支持同城三中心、跨DC的部署结构 – 需要满足这种部署模式，并支持Client本DC读写（多DC可读写）。

潜在需求：

- 消息队列、消息发布订阅、流计算 - 这一类应用主要使用VDL来存储和传递消息。我们可以基于VDL实现消息发布/订阅系统；同时也可以作为Storm/Spark的输入和输出，用于实时流计算的场景。



### 2.2.VDL 实现范围

VDL 将实现上述的明确需求，对于潜在需求，将在后续版本考虑。

实现的主要功能包括：

- Log Produce：将Log有序存储到强一致性的分布式日志VDL中。
- Log Consume：根据Offset消费日志。



### 2.3.VDL实现概述

VDL操作的逻辑对象是Log Stream，主要包括对Log Stream进行Log Product与Consume。可以把Log Stream看作是Kafka中只有一个Partition的Topic。VDL采用Raft作为一致性算法，一个Raft Group可以看作一个Log Stream，Log Stream与Raft Group是1:1的关系。

VDL支持Kafka协议。VMS将在后续版本支持Kafka 0.10版本，所以VDL 支持的Kafka协议版本定为0.10，以支持新的VMS Client与Kafka 0.10原生客户端，具体支持部分见Detail Design。
Kafka 0.10版本不支持Exactly Once语义以及Follower读取功能，VDL的后续目标是考虑同时支持0.10/0.11版本（包含exactly once语义），并根据0.11版本的实现情况决定是否支持Follower读取功能。在前期实现时，需要考虑后续扩展。VDL不维护各个Consumer的消费进度，需要Consumer端自己决定自己从哪个Offset开始消费。VDL除了多副本、强一致的关键特性外，对Log的生产定义了一些特性，如下表：

| **分类** | **特性**           | **说明**                                                     |
| -------- | ------------------ | ------------------------------------------------------------ |
| 生产     | 异步发送顺序性保障 | 可以异步生产Log，对于同一个TCP连接，保证只要第N个Log返回异常，> N以后的日志都将返回异常，这个将在详细设计时，根据不同的异常做不同的处理。 |

​    

## 3.架构设计

本章节描述VDL的总体架构以达成总体的认识，各个模块的具体设计与作用在后续Detail Design章节中有详细描述。VDL总体架构如下图：

![a1](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9txlw2xdj20ya0dwt9s.jpg)



### 3.1.Client API

Client API以类库的方式提供。Client的行为可以分为Producer与Consumer，具体的行为在Detail Design中描述。

### 3.2.VDL Replica Set

VDL Replica Set 是用于存储相同Log数据的一组VDL Servers节点（进程），用于数据冗余（多副本）与高可用。VDL Server是单独运行的进程服务。VDL Replica Set通常由3个或5个VDL Server组成，如下图所示（3个节点示例）：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9u3jmvhpj20mw0iut9i.jpg)

VDL 用Raft作为一致性算法，一个VDL Replica Set可以同时存在多个Raft Group，也就是一个VDL Replica Set可以同时服务多个Log Stream。如下图所示：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9u4hbmpyj20qg0mkdhk.jpg)

对于同城三中心的部署结构，VDL Server可以跨DC部署，如下图所示:

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9u5cqyrrj20x40redig.jpg)

每个Raft Group，创建时可以指定默认的DC，并通过Config Servers组件，监控Raft Group的Leader是否有发生变化，若有则尝试恢复成默认的DC（Leader Transfer）。

总体端到端的拓扑图如下所示：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9u74to3rj20z70tx787.jpg)

### 3.3.Config Servers

Config Servers用于管理VDL Cluster，包括

1.	Raft Group的创建
2.	接收VDL Server的上报请求，存储VDL集群信息
3.	接收Management的管理请求，管理VDL集群
4.	在跨DC部署模式下，监控Leader所在DC，并进行Leader Transfer。

一个VDL集群由一个Config Servers管理，一个Config Servers由三个节点的Config Server组成，通过Raft算法选Leader，只有Leader进行服务。



## 4.数据流
VDL 的Data Flow可以分为Log Produce与Log Consume部分。Log Produce：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9u9tjdfcj20z40sfacz.jpg)

1. Producer生产Log，包括如下流程：

   a)   Producer使用Boot Strap Server连接到VDL Server，将使用Kafka协议中的Metadata协议，获取集群的路由信息。

   b)   根据路由信息与需要发送的Log Stream名字（上图例子为A），查找到对应Raft Group的Leader，并连接到Leader所在的VDL Server。

   c)   连接成功后，开始生产Log。

   d)  若路由信息不正确，VDL Server收到生产Log的请求后，将返回NotLeaderForPartition的错误（见Kafka协议），Client将重新获取路由信息，并偿试上述步骤。

    

2. VDL Server收到请求后，交由RPC Interface解析之后，将运行Raft的流程。Raft流程包括：写本地Log，数据复制等。

3. 在Raft流程成功结束后（收到多数派的响应），ACK给客户端。

Log Consume流程如下图所示：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9uccoq3tj20wu0qa405.jpg)

与Produce流程不同的是，Consume流程，只要在Leader读取，而不需要与Follower进行网络通讯。具体流程如下：

1. Consumer发送请求到对应的Leader。

2. Leader根据Offset读取相应的Log。

3. Leader返回对应的数据。

由于Kafka由于Kafka Client的限制，使用Kafka Client时，Consumer只能从Leader进行消费。后续如果提供VDL Client，则不受此限制。



## 5.Detail Design

### 5.1.组件设计
#### 5.1.1.Client
##### 5.1.1.1.Kafka协议支持
VDL支持Kafka 0.10版本的部分协议，达到可以直接使用Kafka原生客户端或者新VMS客户端的目的。支持的协议表如下：

| **分类**                  | Kafka协议说明                                                | **是否**   **支持** | **备注**                                                     |
| ------------------------- | ------------------------------------------------------------ | ------------------- | ------------------------------------------------------------ |
| Metadata   API            | 描述可用的brokers，包括他们的主机和端口信息，并给出了每个broker上分别存有哪些Topic与Partition | 是                  | 返回Log Stream的路由信息。                                   |
| Produce API               | 发送消息接口                                                 | 是                  | N/A                                                          |
| Fetch   API               | 获取消息接口                                                 | 是                  | 实现长轮询模型                                               |
| Offset API                | 用于获取Topic中的Offset有效范围                              | 部分                | 支持获取最后一个offset与最早的有效offset。不支持请求具体时间的offset获取？？ |
| Offset   Commit/Fetch API | Kafka   0.8.2版本开始，提供Broker存储Consumer Offset功能     | 不支持              | 由Consumer存储消费的Offset                                   |
| Group Membership API      | 用于消费组管理                                               | 是                  | 达到原生客户端可以直接访问VDL的级别                          |
| Administrative API        | 管理接口                                                     | 部分                | 符合原生客户端要求，创建、删除Topic由Management处理。        |

##### 5.1.1.2.	Producer 

Kafka使用基于TCP的二进制协议，Produce是典型的Request/Response模式。Kafka的Produce协议中，一个Request可以包含一个或多个Log。所以Produce方式原理上可以支持下面几种（不同的Kafka客户端实现可能不一样）：

1.	单条发送：一个Request发送一个Log。
2.	批量发送：一个Request请求发送多个Log，VDL不保证一个Request中多个Log的原子性。
3.	同步发送：客户端实现同步发送功能。
4.	异步发送：单条或批量异步发送。VDL需要保证在同一TCP连接的Log顺序性。

对于Produce的Response，作如下定义：
1.	成功：如果是单条发送，则此条发送成功。如果是批量发送，则全部成功。
2.	失败：如果是单条发送，则此条发送失败。如果是批量发送，则全部失败。一旦VDL Server返回异常，则此TCP连接的后续所有请求都将异常，客户端需要重新连接。
3.	Timeout：客户端逻辑Timeout，意思是在指定时间内，没有等到Response响应。
      a)	Timeout不保证对应的Request操作的成功与失败，对于批量发送，不保证原子性，可能部分成功或部分失败（如其中一部分Log同步到Follower后，Leader Crash等情况），或者全部成功或全部失败。
      b)	出现Timeout，客户端不需要重新连接。

##### 5.1.1.3.Consumer

Kafka 0.10可以使用Broker存储Consumer的消费进度(Offset)，但现阶段VDL并不支持。Consumer端需要自已保存Offset。

VDL的Log Stream可以当作是Kafka中只有一个Partition的Topic，Kafka会为Consumer Group分配Partition，但VDL的Log Stream只有一个Partition，所以同一个Consumer Group，同时只能由一个Consumer实例进行消费。

#### 5.1.2.VDL Replica Set 

如Architecture章节描述，VDL Replica Set 是用于存储相同Log数据的一组VDL Servers节点（进程）组成，VDL Server主要由RPC Interface模块、Log Stream Handler模块、一致性模块（Raft）组成。如下图所示：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9ukjgn0vj20z60f1tb5.jpg)

### 5.2.Raft关键点设计 

#### 5.2.1.存储设计 

##### 5.2.1.1.	Log存储设计

VDL Server中，存在两个日志逻辑视图。一个是Raft的日志（对内），一个是Log（对外）。Raft日志除了存储Log信息外，还会存储Raft算法产生的信息 。在存储设计中，需要考虑合并这两个日志，将两次IO合并成一次IO。

下图是一个Raft Group的存储结构图：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9um5mjqrj20z80kcn07.jpg)

**Raft Log**

Raft算法使用的日志，用于存储Raft信息与Client发送的Log。Raft Log保证落盘与Raft Commit后才会ACK。

Raft Log由多个固定大小的Segments组成，这便于日志数据的删除。每个Segment由多个日志组成，每个日志由Raft Info与Log Data组成。

1. Raft Info：包括日志类型与Raft的信息，如Term、Index等。

2. Log Data：根据日志类型，存放Raft的日志数据或者Client发送的Log数据。

VDL Server会在内存中缓存一段最新的Raft Log，便于Consumer能快速地Tailing Read，并且不会产生磁盘IO。

 

**Raft Index**

用于存储Raft Log索引的文件。在Raft算法中，需要根据Index定位Raft Log。Raft Index也是由多个Index Log Segments组成，每个Segment由多个Idx组成。Idx有两个关键的字段，一个是File Position，指向Raft Log的Segment文件，另一个是Inner File Position，指向Segment文件中的偏移，这个偏移是Raft Log的在Segment文件中的偏移量。

每个Segment是固定大小，假设为M bytes，Idx也是固定长度的，假设为N bytes，若要定位Index为X的Raft Log，由用X * N / M得到具体的Segment，假设为Y，然后再由(X * N – M * Y) / N得到索引在Segment中的位置。

对于Raft Index，不是实时落盘的，VDL Server会在内存维护一段最新的Index数据，当Index数据超过PageSize的N倍时，会刷新到磁盘。

 

**Log Index**

对Client Log的索引文件。不同于Raft Index，Log Index只是索引Client Log的日志，并不对Raft算法产生的日志进行索引。

假设X为Raft产生的日志，Y为Client发送的日志，Raft Log为 YYXYY，那么，对于Raft Index，其1/2/3/4/5条日志为Y/Y/X/Y/Y，对于Log Index，只有四条日志，全是Y。Consumer会根据Log Index进行消费。

Log Index与Raft Index的组织形式一样，不同的是，Log Index中的Inner File Position指向的是Raft Log中Log Data的起启位置，这样便于使用ZeroCopy技术快速发送到Consumer。

与Raft Index一样，Log Index也是异步落盘。在实现时，Log Index更像是FSM的应用，应该与Raft Log/Raft Index独立出来。

##### 5.2.1.2.配置信息存储设计

配置信息包括两个：Config Servers需要存储VDL的配置信息，VDL Server需要存储Raft算法产生的配置信息。

Config Servers存储VDL的配置信息，包括：
1.	VDL Cluster的拓扑信息：有多少个Replica Set
2.	Replica Set的拓扑信息：每个Replica Set有多少个VDL Server，每个VDL Server的IP端口、状态等信息
3.	Raft Group的信息：每个Replica Set跑多少个Raft Group，每个Raft Group的拓扑信息

VDL Server需要存储Raft算法产生的配置信息，包括：
1.	Raft Membership信息：如Raft中的Peer信息
2.	Vote信息：在Leader Election中所涉及的Vote信息需要持久化

由于配置信息数据量很少，不需要考虑Log Compaction，所以只需要在Raft的FSM引入Key-Value存储或者直接使用文件存储配置信息即可。

##### 5.2.1.3.存储介质选型

VDL的存储设计是针对SSD的。原因如下：

对于单个Log Stream，一般有三种操作：
1.	写入：顺序写入Raft Log文件，产生顺序写入IO。
2.	Tailing Read：在尾端消费，由于VDL会在内存缓存一段日志，所以Tailing Read不会产生磁盘IO。
3.	Catchup Read：从非尾端开始消费，会产生顺序读IO。

如果一个VDL Server同时有多个Log Stream，并存在单个磁盘，会产生随机读写IO。 如果一个VDL Server同时有多个Log Stream，每个Log Stream一个磁盘，则会产生顺序写IO，随机读IO（多个Consumer消费）。

综上所述，随着Consumer个数的变化与是否Catchup Read，都会带来随机读IO，所以建议使用SSD设备，避免随机的读IO带来较大的影响，使用SSD还会在多个Log Stream的情况下，为随机写IO带来更高的性能。

还有一种存储的实现方式，将读与写的IO分离，像Distributed Log，原理是一份数据，会写两份，每份独立磁盘，这样读IO就不会影响写IO。这种方式对机械磁盘会带来较高的写入TPS，但Catchup Read操作则优化不大。综合考虑两份数据的存储成本、SSD的普及程度与VDL实现成本，VDL不采用这种方式。

5.2.1.4.	存储性能优化
在实现中，为了提高Raft Log写入性能，应该使用align overwrite + overlapped + sync_file_range方式写入，如下图所示：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9uyvmuq3j20z60lbn1e.jpg)

#### 5.2.2.Log Compaction

配置信息存储的数据量很少，不需要Compaction，这章节讨论的是Log的Compaction。对于Log Stream形态的VDL，Log Compaction主要指的是日志的删除，包括Raft Log/Raft Index/Log Index日志的删除。

VDL通过外围脚本，手工触发删除日志，从前向后删除日志文件。

#### 5.2.3.	Snapshot

Snapshot用于日常备份与新节点加入时的同步。由于Log Stream形态的VDL存储的是Log，而Log又是分Segments存储的，所以Snapshot可以是已结束的Log Segments（非正在写入的Segment）。

在新节点加入时，Leader可以直接发送已结束的Segments到Follower。也可以开发VDL同步工具，自动将已结束的Segments拷贝到新的节点。

#### 5.2.4.	Membership Change

无论是ETCD的Raft Lib还是HashiCorp的Raft Lib，均实现了Membership Changes，这部分确定性比较高，不会影响整体方案，使用现有实现即可。

#### 5.2.5.	Raft性能优化

##### 5.2.5.1.	Raft Pipeline

在Raft性能优化中，收益较大的是Raft Pipeline优化。核心思想是Leader的刷盘与AppendEntries To Follower同时进行。
在Raft算法前期的研调中，Pipeline优化使得整体TPS有一倍的增长，Latency是原来的1/2（SATA盘）。在VDL的实现中，需要实现这个优化。

##### 5.2.5.2.	TCP Pipeline + Group Commit

TCP Pipeline的核心思想是：Client可以Pipeline发送（异步）数据，Leader与与Follower的TCP也可以是Pipeline通信。Group Commit的核心思想是：批量将Log刷盘，结果存储设计章节的性能优化，最小化刷盘的消耗。总体如下图所示：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9v4mlw7jj20z80glgnz.jpg)

#### 5.2.7.Raft与Log可见性

在某一个时刻，一个Log Stream（Raft Group）会有如下的情况：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fw9v6cdm4dj20z609vwex.jpg)

1.	First Log：指的是这个Log Stream的第一个Log，随着Log Compaction等操作，这个值会变化。
2.	Commit：指的是Log Stream里面已得到多数派响应的Log Index。
3.	Append：指的是Log Stream现在入写的Index

VDL承诺Log可见区间为First Log到Commit(E-H)，未Commit的Log不可见（>H的Log）。

#### 5.2.8.FSM的考虑

VDL Server的FSM主要有两个，一个是Log Index，一个是用于存储Membership等Raft信息。
Config Servers的FSM主要用于存储VDL集群信息。

Raft信息与VDL集群信息的数据量不大，所以不用考虑太高的性能与Log Compaction，引入Key-Value Store即可，如Bolt-DB，Level-DB。Log Index主要用于做Log的索引，在Raft Commit之后，通知Log Index FSM，Log Index FSM在内存生成新的Index，在达到刷盘条件，异步通知刷盘线程落盘。

### 5.3.测试

VDL的测试分为单元测试与集成测试。

**单元测试**

VDL使用Go语言开发，Go Test是Go语言自带的测试工具，VDL的单元测试将使用Go Test进行。

**集成测试**

VDL是个分布式的系统，集成测试是分布式系统测试的难点。VDL的集成测试，将在代码中插入断点，模拟节点无响应，Crash等异常流程，使用测试架框（自研），根据测试用例，向VDL Server发送断点请求，测试分布式环境下各种异常。

