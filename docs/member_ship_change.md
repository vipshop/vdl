# Membership Changes操作手册

## 1.概述

VDL可以使用管理命令：VDLCTL，对Membership进行管理，提供了四种不同的操作：

1. 查看Member List
2. 增加Member
3.  删除Member
4. 更新Member

VDLCTL需要提供endpoint地址，endpoint地址为VDL配置文件中的listen-admin-url项，查看member list可以是VDL集群中任意一个节点的endpoint，但增加/删除/更新则需要从leader进行(指定leader的endpoint)

VDL的成员变更，没有使用joint consensus的算法，而是使用"one by one"的方式，也就是每次只能变更一个成员，在变更成功后，才能继续更新另一个成员。

 VDLCTL可以使用–help查看相关的命令使用，如

```shell
#查看所有命令
./vdlctl --help
 
#查看member功能用法
./vdlctl member --help
 
#查看member子命令list的用法
./vdlctl member list --help
```

 

 

## 2.查看Member List

```shell
./vdlctl member list <logstream name> --endpoints <endpoint>
 
例子
./vdlctl member list logstream1 --endpoints=http://127.0.0.1:9001
 
结果输出
ID: 4eff15a7b59548ca, Name: vdl1, PeerAddrs: http://127.0.0.1:2381, IsLeader: false
ID: e8a2d2fe302cd27b, Name: vdl0, PeerAddrs: http://127.0.0.1:2380, IsLeader: true
```

其中:

1) ID： log stream对应的Raft Group的Node ID

2) Name：Raft Group所在的VDL节点名字

3) PeerAddrs：Raft Group的通讯 IP 与端口

4) IsLeader：是否Leader

 

## 3.增加Member

往现有集群添加一个新的节点。添加新节点会有两种情况：

1) 直接添加：集群运行不久，数据量不多，这时候可以直接添加节点，让新节点直接与leader同步数据。如果Leader已删除过数据，则不适合此方式添加新节点。

2) 从snapshot重建后添加：这种情况适合需要添加一个新节点到运行较久、数据量较多的集群，这时间，需要从集群中的某个Follower dump出shapshot，应用到新节点，再加入集群

### 3.1直接添加

分为三步：

1. 准备好新节点：资源准备好，并做好相关配置
2. 使用VDLCTL执行add member操作
3. 启动新节点

下面以一个例子，分步介绍

假设原集群：

VDL0, LogStream名称:logstream1 ，PeerAddrs: [http://127.0.0.1:2380](http://127.0.0.1:2380/)

VDL1, LogStream名称:logstream1，PeerAddrs: [http://127.0.0.1:2381](http://127.0.0.1:2381/)

现添加新节点

VDL2， LogStream名称:logstream1，PeerAddrs: [http://127.0.0.1:238](http://127.0.0.1:2381/)2

#### A: 准备好新节点

准备好相关的资源(机器)，部署好VDL，并参考配置说明，做好相关配置。其中需要特别关注以下配置：

```yaml
# initial-cluster-state一定要为existing，意思为加入已有集群
<logstream>-initial-cluster-state: existing
 
#existing-admin-urls，需要配置已有集群中某个节点的admin url
<logstream>-existing-admin-urls: http://127.0.0.1:9000
 
#initial-cluster，需要配置已有集群的peer addrs + 自己的peer addrs
<logstream>-initial-cluster: vdl0=http://127.0.0.1:2380,vdl1=http://127.0.0.1:2381,vdl2=http://127.0.0.1:2382
```

#### B: 使用VDLCTL执行add member操作

```yaml
# 执行add member操作
./vdlctl member add logstream1 vdl2 --peer-urls=http://127.0.0.1:2382 --endpoints=http://127.0.0.1:9001
 
# 结果： 
Member 6dfedeb1b19d5bc3 added to logstream logstream1 in cluster [460d709c038402fe]
 
# 使用member list查看
./vdlctl member list logstream1 --endpoints=http://127.0.0.1:9001
 
# 结果： 
ID: 4eff15a7b59548ca, Name: vdl1, PeerAddrs: http://127.0.0.1:2381, IsLeader: false
ID: 6dfedeb1b19d5bc3, Name: vdl2, PeerAddrs: http://127.0.0.1:2382, IsLeader: false
ID: e8a2d2fe302cd27b, Name: vdl0, PeerAddrs: http://127.0.0.1:2380, IsLeader: true
```

 

#### C: 启动新节点

使用vdl2的配置启动新节点

```
./vdl start -f vdl2.conf.yml
```

观察VDL是否启动成功

### 从snapshot重建后添加

  参考: [VDL的snapshot操作](./docs/snapshot_manual.md)

## 4. 删除Member

从集群中移除掉一个节点。通常出现在

1) 节点永久性故障：建议先删除节点，再新建另一个新节点加入（使用snapshot方式）

2) 节点暂时故障，但恢复需要较长的时间：建议先删除节点，再创建新节点加入（使用snapshot方式）。因为故障较长时间离线，再接入集群时，需要从leader上追较多的数据，会影响Leader性能。

3) 其它部署/运维需求

删除节点，直接使用VDLCTL进行删除即可:

```shell
假设原集群：
ID: 4eff15a7b59548ca, Name: vdl1, PeerAddrs: http://127.0.0.1:2381, IsLeader: false
ID: 6dfedeb1b19d5bc3, Name: vdl2, PeerAddrs: http://127.0.0.1:2382, IsLeader: false
ID: e8a2d2fe302cd27b, Name: vdl0, PeerAddrs: http://127.0.0.1:2380, IsLeader: true
现需要删除VDL2，则使用VDLCTL
 
./vdlctl member remove logstream1 6dfedeb1b19d5bc3 --endpoints=http://127.0.0.1:9001
 
执行结果
 
Member 6dfedeb1b19d5bc3 removed from logstream logstream1 in cluster [460d709c038402fe]
```

执行完后，在VDL2中，logstream1对应的raft group将自动停止

## 5.更新Member

主要用于更新member的raft group的通信IP与端口，修改步骤

1) 停止要更新的节点，并更新对应的配置文件

2) 使用VDLCTL更新节点信息 

3) 启动更新的节点

假设现有集群

```shell
./vdlctl member list logstream1 --endpoints=http:``//127.0.0.1:9001`` ``ID: 49bc1e27d0ae19a6, Name: vdl2, PeerAddrs: http:``//127.0.0.1:2382, IsLeader: false``ID: 4eff15a7b59548ca, Name: vdl1, PeerAddrs: http:``//127.0.0.1:2381, IsLeader: false``ID: e8a2d2fe302cd27b, Name: vdl0, PeerAddrs: http:``//127.0.0.1:2380, IsLeader: true
```

现在要更新vdl2, logstream1的raft通信信息。

首先停止vdl2，更新节点的端口信息，然后使用VDLCTL更新

```shell
# 以下将vdl2(49bc1e27d0ae19a6)的端口从 2382->2383
./vdlctl member update logstream1 49bc1e27d0ae19a6 --peer-urls=http://127.0.0.1:2383 --endpoints=http://127.0.0.1:9001
 
Member 49bc1e27d0ae19a6 updated in logstream logstream1 in cluster [460d709c038402fe]
```

更新完后，启动vdl2

 

## 6.FAQ

##### 1. 如果使用更新命令，更新节点的端口，但忘记配置或者重启，会怎么样？

**- 在生产中禁止出现此情况**

从技术上分析：

比如A与B节点，A是Leader，B是Follower，将B更新端口1023->1024，若B还是以1023的端口运行，那么

1.  B还是Follower，那么A的同步还是通过stream的方式同步数据，B的Resp则通过 http post的方式回包，日志同步正常，但snapshot功能无法使用，且A会不断出现B  health check的告警
2.  B是Leader，那么A推数据到B，则以http post方式，同步较慢 

##### 2. 新增节点时为什么会出现current cluster have x nodes, but just have x nodes healthy, cluster cannot run when add new node, please recover the bad node first

VDL配置文件中，每个log stream可以配置strict-member-changes-check，默认为true

此配置意思是在做成员变更时，是否需要严格校验，校验原理是：若新节点加入会导致现有集群无法正常服务，则无法添加节点，例如：

原集群有3个节点，若添加一个新节点，则变为4，新的quorum数变为3，若原集群只有2个节点正常，增加节点后，集群则无法正常服务，所以无法添加。

##### 3. 删除节点时为什么会出现current cluster have x nodes, except remove member, have x nodes healthy, cluster cannot run when remove this node, please recover the bad node first

与新增节点类似，这里的校验原理是：若删除节点，会导致集群无法正常服务，则无法删除节点，例如：

原集群有3个节点，只有2个节点是健康的，若删除的节点刚好是其中的健康节点，则无法删除，因为删除后，只有一个节点是健康的，而集群需要2个节点才能正常服务。