# snapshot使用手册

## 1.背景

Snapshot的应用场景主要有以下几种：

1. 支持新加入节点集群。新节点加入集群时，是不存在任何数据的。从VDL集群中的一个Follower节点上通过snapshot工具备份全量数据，并发送到新节点上，在新节点上利用接收的文件和元信息来将其快速恢复到一个历史状态。主要步骤如下所示：
   1. 通过备份工具，在Follower节点上生成snapshot数据，并通过流式的方式发送到新节点上。
   2. 在新节点上，通过VDL内部命令行工具加载snapshot，并恢复VDL数据到一个历史一致性状态。
   3. 启动新节点上的VDL。
2. 支持落后很久的Follower节点重新加入集群。在对VDL系统进行版本升级的时候，如果Follower落后数据较多时，也可以通过备份/恢复snapshot的方式，使得Follower节点上的数据快速恢复到接近Leader节点。

本手册的目的就是指导用户完成snapshot全部操作

## 2.snapshot文件和组件说明

snapshot文件由以下三种文件组成：

- segment文件
- index文件
- metadata文件

segment文件和index文件是VDL数据文件和索引文件，metadata文件记录了VDL的相关元信息，例如：raft库中的vote信息和membership信息。

snapshot流程由三个组件配合完成，这三个组件分别是：

- send_server，从VDL Follower上读取数据文件和元信息文件，并发送给receive_agent。该组件部署在VDL Follower节点上。
- receive_agent，向send_server发起获取文件请求，并不断接收send_server发送过来的文件。
- restore_agent，将snapshot文件加载到VDL的数据目录，并根据metadata文件中的数据，设置VDL相关元信息。

三个组件部署示意图如下所示：

![](http://ww1.sinaimg.cn/large/6e5705a5gy1fwa1f8qv5vj20iu0gydgr.jpg)

## 3.snapshot操作流程

snapshot操作主要分为三个流程：

1. 在Follower节点上启动send_server
2. 在待恢复节点启动receive_agent，接收send_server发送过来的segment文件、index文件和metadata文件。
3. 在待恢复节点启动restore_agent，导入数据文件和元信息文件到待恢复节点。
   下面分别阐述这三个组件使用方式

### 3.1 启动send_server

在VDL集群中选择一个Follower节点，作为snapshot操作的数据源，并在该节点上部署send_server，启动命令如下所示：

```shell
nohup ./send_server 
--addr=192.168.0.1:7000 \
--vdl_admin=http://192.168.0.1:8500 \
--base_dir=/home/apps/flike/vdl/base \
--rate_limit=10 \
--data_dir=/home/apps/flike/vdl/data  &
```

 

```shell
addr:send_server对外服务的IP和端口
vdl_admin:send_server所在节点的VDL实例的管理端地址
data_dir:VDL实例的data目录，与VDL配置中raftGroupName-log-dir项设置相同
base_dir:send server工作目录
rate_limit:发送文件限速，发送文件最大速度10MB/s，如果不设置该参数表示不限速。
```

 

### 3.2 启动receive_agent

启动send_server后，就可以在待恢复节点启动receive_agent，用于请求并接收segment文件、index文件和metadata文件。启动命令如下：

```shell
./receive_agent 
--server_addr=192.168.0.2:7000 \
--raft_group=logstream1 \
--boot_mode=new \
--snap_dir=/home/apps/flike/vdl/receive_agent_snap
```

 

参数介绍

```shell
server_addr:send_server的地址
raft_group:待恢复的VDL对应的raft group。
boot_mode:启动receive_agent的模式，有new和resume两种模式，new模式表示重新下载snapshot所有文件，会清空snap_dir中已有的文件。resume用于继续上一次中断的传输过程，可以避免不必要的重传文件操作。当第一次启动receive_agent时，肯定是new模式。如果在传输过程中被中断，则可以用resume模式重新启动。
snap_dir:receive_agent接收的snapshot文件，存储在该目录下。
```

###  3.3 启动restore_agent

restore_agent利用receive_agent接收到的文件来恢复VDL，命令如下所示:

```shell
./restore_agent
--vdl_admin=http://192.168.0.1:8500 \
--data_dir=/home/apps/flike/vdl/data \
--stable_dir=/home/apps/flike/vdl/bolt \
--node_status=old \
--snap_dir=/home/apps/flike/vdl/receive_agent_snap \
--raft_group=logstream1 \
--restore_mode=mv \
--server_name=vdl3 \
--peer_url=http://192.168.0.1:2380
```

 

参数说明

```
vdl_admin:vdl集群的Leader节点的管理地址，注意：这里必须是Leader节点的管理地址。用于添加新节点的membership信息。
data_dir:待恢复节点的VDL实例的data目录，与VDL配置中raftGroupName-log-dir项设置相同。
stable_dir:待恢复节点的VDL实例的元信息目录，与VDL配置中stable-dir设置相同。
node_status:待恢复VDL的状态信息，如果待恢复节点是旧节点，则该项设置为old，
如果待恢复节点是新节点，需要加入VDL集群，则该设置项为new。新旧节点区别就在于metadata中的membership是否包含该节点信息，如果包含则是旧节点，如果不包含则是新节点。
snap_dir:receive_agent接收的snapshot文件，存储在该目录下。restore_agent从该目录读取snapshot文件。
raft_group：待恢复的VDL对应的raft group。
restore_mode：restore_agent通过move文件还是copy文件到数据目录，由该配置项决定。mv表示通过move方式将segment文件和index文件移动到data_dir，cp表示通过copy方式将segment文件和index文件移动到data_dir。
server_name：待恢复VDL的name，与VDL配置文件中name项设置相同。
peer_url:待恢复VDL的rafGroupName-listen-peer-url，与VDL配置文件中rafGroupName-listen-peer-url项设置相同。
```

 

### 3.4 启动VDL

通过restore_agent加载snapshot文件，恢复VDL后，VDL就可以启动了。对于旧节点，直接利用原有配置启动即可。但对于新节点启动并加入集群，有几项配置需要做相应调整：

```yaml
# initial-cluster-state一定要为existing，意思为加入已有集群
<logstream>-initial-cluster-state: existing
  
#existing-admin-urls，需要配置已有集群中某个节点的admin url
<logstream>-existing-admin-urls: http://192.168.0.4:8500
  
#initial-cluster，需要配置已有集群的peer addrs + 自己的peer addrs
<logstream>-initial-cluster: vdl0=http://192.168.0.1:2380,vdl1=http://192.168.0.2:2380,vdl2=http://192.168.0.3:2380
```

 

### 3.5 agent.sh文件说明

上面介绍了snapshot三个组件的使用方式，在VDL实际部署中，已经将receive_agent和restore_agent两个组件的启动都封装在agent.sh脚本文件中，除此之外agent.sh实现了保存snapshot个数和禁止在传输过程中删除segment文件等功能，在实际部署中，应该通过agent.sh来下载和加载snapshot文件。agent.sh文件中每个配置项有详细的注释，使用之前请参考注释说明。

## 4.总结

通过本文档，用户可以正确地使用VDL的snapshot功能。