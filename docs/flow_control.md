# VDL流控操作手册

## 1.概述

VDL流控分为写流控、end up read流控与catch up read流控，每种流控又分为request次数流控与流量(byte)流控。通过vdlctl，可以打开/关闭VDL的流控，也可以设置对应的流控值。

现阶段，vdlctl流控的操作对象是VDL进程，所以现阶段，对集群的流控设置需要分别对集群中每个VDL过程分别进行设置。

 

## 2.查看流控设置

使用vdlctl，可以查看VDL进程的流控设置。

```shell
./vdlctl rate list --endpoints=http://localhost:9000
 
结果：
IsEnableRateQuota      : true    //注释：否打开流控 ,默认关闭
WriteRequestRate       : 50000   //注释：写入request数量控制
WriteByteRate          : 5000000 //注释：写入byte流量控制
CatchupReadRequestRate : 10000   //注释：catchup read request数量控制
CatchupReadByteRate    : 500000  //注释：catchup read byte流量控制
EndReadRequestRate     : 1000    //注释：end read request数量控制
EndReadByteRate        : 524288000 //注释：end read byte流量控制
```

其中，指令中的endpoint为需要查看的VDL进程对应的listen-admin-url

 

## 3.设置流控

使用vdlctl，可以更新VDL进程的流控设置。

```shell
./vdlctl rate update EndReadRequestRate=1,IsEnableRateQuota=true,CatchupReadRequestRate=1 --endpoints=http://localhost:9000
 
结果：
========Update Rate Success! Updated Config========
IsEnableRateQuota      : true
WriteRequestRate       : 50000
WriteByteRate          : 5000000
CatchupReadRequestRate : 1
CatchupReadByteRate    : 500000
EndReadRequestRate     : 1
EndReadByteRate        : 524288000
```

在rate update中，可以一次更新多个流控参数，如上述例子所述，每个流控参数通过逗号分开。VDL会预先对所有流控参数进行检查，若流控参数不正确，则直接返回错误，不会做任何更新。如：

```shell
./vdlctl rate update EndReadRequestRate=1000,IsEnableRateQuota=true,CatchupReadRequestRatexxxx=1000 --endpoints=http://localhost:9000
 
结果：
Error:  rpc error: code = Unknown desc = Cannot found any config for CatchupReadRequestRatexxxx
 
VDL不会对EndReadRequestRate与IsEnableRateQuota进行更新
```

