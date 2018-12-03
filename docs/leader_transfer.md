# Leader Transfer操作手册

## 1.背景

Leader Transfer功能可以人为地把集群的Leader节点迁移到集群的其它节点。

## 2.操作说明

使用Leader Transfer，需要明确需要将新Leader Transfer到现有的哪个Follower节点（下文称Transferee），需要获取对应的ID。

通过vdlctl的member list命令，可以查看集群状态，并获取对应的ID。例如：

```shell
./vdlctl member list logstream1 --endpoints=http://localhost:9000
 
输出：
 
ID: 4eff15a7b59548ca, Name: vdl1, PeerAddrs: http://127.0.0.1:2381, IsLeader: false
ID: 5677ac1a2fa9eed9, Name: vdl2, PeerAddrs: http://127.0.0.1:2382, IsLeader: false
ID: e8a2d2fe302cd27b, Name: vdl0, PeerAddrs: http://127.0.0.1:2380, IsLeader: true
```

通过member list，可以看到现在集群leader是vdl0，假设现需要将leader transfer到vdl2，可以执行下述命令

```shell
./vdlctl logstream leader_transfer logstream1 5677ac1a2fa9eed9 --endpoints=http://localhost:9000
 
其中， 5677ac1a2fa9eed9为vdl2对应的ID，endpoints则指向现在的leader节点
```

 

Leader Transfer命令不保证Leader一定Transfer成功，因为他受Transferee与旧Leader的数据之差制约

输出结果如下：

```shell

#Leader Transfer成功则输出
logstream1 leader transfer success!
 
#Leader Transfer失败则输出异常，一般为"超时异常"
Error:  rpc error: code = DeadlineExceeded desc = context deadline exceeded
```

 