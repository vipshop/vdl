#!/bin/bash

#####common config begin######################

#raft group name
#待恢复VDL对应的raft group
raft_group='logstream1'

#snapshot file dir
#receive_agent存储文件的路径
snap_dir=/Users/longdandan/persion/snap/vdl3/snap_dir

#snapshot count,must use cp for restoring snapshot
#保存snapshot的个数，超过会删除，保存多个snapshot时移动文件方式只有copy模式才有意义。
keep_snap_count=2

#agent bin dir
#receive_agent和restore_agent的所在目录
agent_bin_dir=$(pwd)

#vdlctl bin
#vdlctl路径
vdlctl=$(pwd)/../../vdlctl

#vdl config file,using for check if vdl exist
#待恢复节点使用的配置文件，主要用于检查是否该VDL实例正在运行，不允许restore正在运行的VDL实例
vdl_config_file='vdl3.conf.yml'
####common config end#########################

####agent config begin########################

#snapshot send server addr
#send server地址
snap_server_addr='127.0.0.1:7000'

#boot agent in new or resume mode
#new:delete the snap_dir, retransmit all the files
#resume:continue to retransmit the receiving process of the last interrupt
#receive_agent启动模式，
#new:重新下载全部文件
#resume:继续上次中断的传输过程
boot_mode='new'
####agent config end#############################

####restore config begin########################

#human-readable name for vdl server
#待恢复VDL的name
server_name=vdl3

#raft group listen peer url
#待恢复VDL的raft group listen peer url
peer_url=http://127.0.0.1:2383

#vdl leader admin addr
#vdl集群的Leader管理端地址
vdl_leader_admin=http://127.0.0.1:9000
#vdl admin addr which generate snapshot
#send_server所在节点的VDL管理端地址，用于禁用（或开启）delete segment操作
vdl_snapshot=http://127.0.0.1:9001

#vdl data dir
#待恢复VDL的数据目录
data_dir=/Users/longdandan/persion/temp/data/vdl3

#vdl stable store dir
#待恢复VDL的stable目录
stable_dir=/Users/longdandan/persion/temp/rocksdb/data3

#node status for restore:new or old
#new:the node not in snapshot membership meta.
#old:the node in snapshot membership meta.
#待恢复VDL的状态信息，如果待恢复节点是旧节点，则该项设置为old，
#如果待恢复节点是新节点，需要加入VDL集群，则该设置项为new。
#新旧节点区别就在于metadata中的membership是否包含该节点信息，如果包含则是旧节点，如果不包含则是新节点。
node_status='old'

#move or copy snapshot file into data dir,mv or cp
#if you want to save multiple copies, you must use cp
#restore_agent通过move文件还是copy文件到数据目录，由该配置项决定。
#mv表示通过move方式将segment文件和index文件移动到data_dir，
#cp表示通过copy方式将segment文件和index文件移动到data_dir。
restore_mode=mv
###restore config end#############################