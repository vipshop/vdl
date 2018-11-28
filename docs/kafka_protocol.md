# VDL支持kafka协议说明

| API         | VDL      | 备注                                                         |
| ----------- | -------- | :----------------------------------------------------------- |
| Produce     | 部分支持 | VDL支持了版本:0-2 版本，2对应kafka 0.10，忽略ack和ThrottleTime |
| Fetch       | 部分支持 | VDL支持版本:0-2                                              |
| ListOffsets | 部分支持 | VDL支持版本:0                                                |
| Metadata    | 部分支持 | VDL支持版本:0-2                                              |
| ApiVersions | 部分支持 | VDL支持版本:0                                                |

其他协议不支持