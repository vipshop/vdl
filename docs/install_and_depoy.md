#  VDL集群搭建指南

## 1.主要组件
VDL集群搭建正常情况下需要三台机器，组件包括以下几个部分：

1. VDL server，有三个节点，一个Leader，两个Follower。
2. filebeat，负责采集metrics信息，并写入influxdb。
3. influxdb，存储metrics信息。
4. grafana，展示metrics信息。

拓扑图如下所示：

![enter image description here](http://ww1.sinaimg.cn/large/6e5705a5gy1fw8xi5dy4vj20ow0cg3zc.jpg)

## 2.安装过程
### 2.1安装和配置VDL server

1. 安装go环境及设置GOPATH和GOROOT,例如本文档中

```
GOROOT=/home/apps/yclong/go
GOPATH=/home/apps/flike/go_code
```

2. 下载vdl源码到指定目录 

```
git clone git@gitlab.tools.vipshop.com:distributedstorage/ds-vdl.git /home/apps/flike/go_code/src/gitlab.tools.vipshop.com/distributedstorage/ds-vdl
```
3. 编译vdl 

```
cd /home/apps/flike/go_code/src/gitlab.tools.vipshop.com/distributedstorage/ds-vdl 
make
```

4. 配置vdl 
在本文中，先在/home/apps/flike/vdl目录下建立以下目录:

 ```
drwxrwxr-x. 2 apps apps 4096 12月 11 14:21 alarm
drwxrwxr-x. 2 apps apps 4096 12月 11 16:12 bolt
drwxrwxr-x. 2 apps apps 4096 12月 11 16:11 config
drwx------. 2 apps apps 4096 12月 11 16:12 data
drwxrwxr-x. 3 apps apps 4096 12月 11 16:06 filebeat
drwxrwxr-x. 2 apps apps 4096 12月 11 16:12 log
drwxrwxr-x. 2 apps apps 4096 12月 12 10:00 metrics
drwxrwxr-x. 2 apps apps 4096 12月 11 16:23 script
 
 ```

配置文件应该放在/home/apps/flike/vdl/config ,内容参考如下:

```
# This is the configuration file for the vdl server.

# Human-readable name for vdl server.
name: 'fiu90'

# Path to the stable store directory (rocksdb path).
stable-dir: /home/apps/fiu_vdl/bolt
# metrics log file path
metrics-dir: /home/apps/fiu_vdl/metrics
# Client URL to listen on for client traffic (TCP)
listen-client-url: 192.168.0.90:8200

# Metrics URL to expose VDL metrics information (must include http://)
listen-metrics-url: http://192.168.0.90:8300

# admin traffic URL to VDL management.(must include http://)
listen-admin-url: http://192.168.0.90:8500

# Time (in milliseconds) of a heartbeat interval.
heartbeat-interval: 100
# Time (in milliseconds) for an election to timeout.
election-timeout: 2000
# Idle connections timeout(ms): the server socket processor threads close the connections that idle more than this
connections-max-idle: 600000
# List of comma separated initial log stream configuration for bootstrapping.
initial-logstream: fiutopic
fiutopic-snapshot-meta-dir: /home/apps/fiu_vdl/snap_meta
# Use for initial-logstream, all log stream should be specified
fiutopic-initial-cluster: fiu90=http://192.168.0.90:2490,fiu91=http://192.168.0.91:2490,fiu92=http://192.168.0.92:2490
# Initial cluster state ('new' or 'existing').
fiutopic-initial-cluster-state: new
fiutopic-strict-member-changes-check: true 
# List of comma separated admin urls for exists cluster, use for initial-cluster-state = existing
# this will use in add new member to exists cluster, can be empty when initial-cluster-state is new
fiutopic-existing-admin-urls:
#the logstore resever segment count,must large than 4
fiutopic-reserve-segment-count: 5

# If true, Raft runs an additional election phase
# to check whether it would get enough votes to win
# an election, thus minimizing disruptions.
fiutopic-prevote: true

#the logstore memcache size,must large than 512MB
#unit:MB
fiutopic-memcache-size: 1024
# diff for each log stream.
fiutopic-listen-peer-url: http://192.168.0.90:2490

# data dir for log stream
fiutopic-log-dir: /home/apps/fiu_vdl/data

glog-dir: /home/apps/fiu_vdl/log 
#output debug log
debug: false

#alarm script path
alarm-script-path: /home/apps/fiu_vdl/alarm/alarm.sh

```

5.启动vdl

```
cd /home/apps/yclong/go/src/gitlab.tools.vipshop.com/distributedstorage/ds-vdl
nohup ./bin/vdl start -f /home/apps/flike/vdl/config/vdl.conf.yml 2>/home/apps/flike/vdl/log/stderr.txt &
```

### 2.2 安装和配置filebeat

1. 下载源码

```
git clone git@gitlab.tools.vipshop.com:ds-vdl/beats.git /home/apps/flike/go_code/src/github.com/elastic/beats
```


2. 编译安装

```
cd /home/apps/flike/go_code/src/github.com/elastic/beats/filebeat
go build

```

将生成的filebeat可执行文件，放在/home/apps/flike/vdl/filebeat目录 

3. 设置配置文件 
在/home/apps/flike/vdl/filebeat目录下设置配置文件(filebeat.yml)，内容参考如下:

```
 
###################### Filebeat Configuration Example #########################
 
 
# This file is an example configuration file highlighting only the most common
# options. The filebeat.reference.yml file from the same directory contains all the
# supported options with more comments. You can use it as a reference.
#
# You can find the full configuration reference here:
# https://www.elastic.co/guide/en/beats/filebeat/index.html
 
 
# For more available modules and options, please see the filebeat.reference.yml sample
# configuration file.
 
 
#=========================== Filebeat prospectors =============================
 
 
filebeat.prospectors:
 
 
# Each - is a prospector. Most options can be set at the prospector level, so
# you can use different prospectors for various configurations.
# Below are the prospector specific configurations.
 
 
- type: log
enabled: true
 
 
# Paths that should be crawled and fetched. Glob based paths.
paths:
- /home/apps/flike/vdl/metrics/*
#- ./rdp_syncer.metrics
#- c:\programdata\elasticsearch\logs\*
 
 
# Exclude lines. A list of regular expressions to match. It drops the lines that are
# matching any regular expression from the list.
#exclude_lines: ['^DBG']
 
 
# Include lines. A list of regular expressions to match. It exports the lines that are
# matching any regular expression from the list.
#include_lines: ['^ERR', '^WARN']
 
 
# Decode JSON options. Enable this if your logs are structured in JSON.
json.message_key:
 
 
# By default, the decoded JSON is placed under a "json" key in the output document.
json.keys_under_root: true
json.overwrite_keys: true
 
 
#================================ Processors ===================================
processors:
- drop_fields:
fields: ["meta", "offset", "prospector", "json", "message"]
 
 
#================================ Outputs =====================================
 
 
# Configure what outputs to use when sending the data collected by the beat.
# Multiple outputs may be used.
 
 
#-------------------------- Elasticsearch output ------------------------------
#output.elasticsearch:
# Array of hosts to connect to.
# hosts: ["localhost:9200"]
 
 
# Optional protocol and basic auth credentials.
#protocol: "https"
#username: "elastic"
#password: "changeme"
 
 
#----------------------------- Logstash output --------------------------------
#output.logstash:
# The Logstash hosts
#hosts: ["localhost:5044"]
 
 
# Optional SSL. By default is off.
# List of root certificates for HTTPS server verifications
#ssl.certificate_authorities: ["/etc/pki/root/ca.pem"]
 
 
# Certificate for SSL client authentication
#ssl.certificate: "/etc/pki/client/cert.pem"
 
 
# Client Certificate Key
#ssl.key: "/etc/pki/client/cert.key"
 
 
#----------------------------- Stdout output -------------------------------------------
output.console:
enabled: false
#codec.json:
#pretty: true
codec.format:
string: '%{[metricName]} value=%{[longValue]}'
 
 
#----------------------------- Influxdb output -------------------------------------------
output.influxdb:
enabled: true
addr: 'http://192.168.0.1:8086'
username: 'vdl'
password: 'vdl'
db: 'vdl_binlog_test'
measurement: 'metrics'
send_as_tags: ["endpoint", "metricName"]
send_as_time: "timestamp"
time_precision: 'ms'
 
 
#================================ Logging =====================================
 
 
# Sets log level. The default log level is info.
# Available log levels are: critical, error, warning, info, debug
#logging.level: debug
 
 
# At debug level, you can selectively enable logging only for some components.
# To enable all selectors use ["*"]. Examples of other selectors are "beat",
# "publish", "service".
#logging.selectors: ["*"]

```

4.运行filebeat

```
cd /home/apps/flike/vdl/filebeat
nohup ./filebeat -e -c filebeat.yml >/home/apps/flike/vdl/filebeat/filebeat.log 2>&1 &
```

### 3.配置influxdb和grafana
由于vdl是对接已有的influxdb和grafana，这里就不再讲述如何搭建两个组件，只描述如何对接这两个组件

#### 3.1在influxdb中创建数据库并配置数据过期时间
influx默认会永久保存写入的metrics，这会导致磁盘满问题，所以在创建数据库时，需要设置数据的过期时间(4 weeks)，命令如下所示：

```
> create database vdl_binlog_test
> use vdl_binlog_test
> show retention policies
> ALTER RETENTION POLICY default ON vdl_binlog_test DURATION 4w REPLICATION 1 DEFAULT
```

#### 3.2配置grafana
配置grafana过程基本都是操作UI界面，主要过程包括： 
1.创建一个Data Sources。（可以参考已有数据源） 
2.创建一个dashboard。 
3.创建各种仪表盘。