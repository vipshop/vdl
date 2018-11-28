# VDL删除segment操作指南

VDL的log compaction都是通过命令来删除segment和对应的index文件，删除操作会涉及到三个命令：

设置删除权限，可以通过命令临时地关闭或打开某个raft group的删除权限。当禁用删除时，通过删除命令删除VDL中的segment会返回错误。只有打开删除权限后，才能允许删除segment。**默认是启用删除操作。**

## 1.查看删除权限

查看删除权限的命令格式如下所示：

```shell

./vdlctl logstream get_delete_perm <logstream name> --endpoints <endpoint>
例如：
[apps@localhost bin]$ ./vdlctl logstream get_delete_perm fiutopic --endpoints http://127.0.0.1:8500
allow to delete segment in this raft group
```

 

## 2.设置删除权限

禁止删除权限命令格式如下所示：

```shell

#禁用删除
./vdlctl logstream set_delete_perm forbid <logstream name> --endpoints <endpoint>
例如：
./vdlctl logstream set_delete_perm forbid fiutopic --endpoints http://192.168.0.1:8500
```

 

打开删除权限命令格式如下：

```
./vdlctl logstream set_delete_perm allow <logstream name> --endpoints <endpoint>
例如：
./vdlctl logstream set_delete_perm allow fiutopic --endpoints http://192.168.0.1:8500
```

## 3.删除segment文件

删除Segment命令格式如下：

```shell
./vdlctl logstream delete <logstream name> <segment name> --endpoints <endpoint>
```

 

如果禁用了删除操作，则会出现如下错误：

```shell

./vdlctl logstream delete fiutopic 0000000000000000.log --endpoints http://192.168.0.1:8500
Error:  rpc error: code = PermissionDenied desc = VDL: segment files not all allow to delete
```

 

如果禁用了删除操作，需要先打开删除权限，再执行删除操作：

```shell
./vdlctl logstream set_delete_perm allow fiutopic --endpoints http://192.168.0.1:8500
./vdlctl logstream delete fiutopic 0000000000000000.log --endpoints http://192.168.0.1:8500
```

