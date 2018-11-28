#!/bin/sh

#filebeat启动和停止脚步
# filebeat_run.sh start|stop|restart

## 服务所在目录
SERVICE_DIR="/apps/svr/filebeat"
CONFIG_FILE="/apps/conf/filebeat/filebeat.yml"
SERVICE_NAME="filebeat"
LOG_DIR="/apps/logs/filebeat"

KILL_TYPE="-9"

case "$1" in

    start)
        cd $SERVICE_DIR
        count=`ps aux|grep ${CONFIG_FILE}|grep -v grep|wc -l`
        if [ $count -gt 0 ];then
            echo "=== $SERVICE_NAME is running, please use restart command"
        else
            ##nohup &  以守护进程启动
            nohup ./bin/filebeat -e -c ${CONFIG_FILE} >${LOG_DIR}/filebeat.log 2>&1 &
            sleep 2
            #检查是否启动成功
            count_2=`ps aux|grep ${CONFIG_FILE}|grep -v grep|wc -l`
            if [ ${count_2} -eq 0 ];then
                echo "=== start $SERVICE_NAME failed"
            else
                echo "=== start $SERVICE_NAME success"
            fi
        fi
        ;;

    stop)
        ps aux|grep ${CONFIG_FILE}|grep -v grep|awk '{print $2}'|xargs kill ${KILL_TYPE}
        echo "=== stopping $SERVICE_NAME ..."

        ## 停止5秒
        sleep 5
        ##
        P_ID=`ps -aux | grep ${CONFIG_FILE}|grep -v grep| awk '{print $2}'`
        if [ "$P_ID" == "" ]; then
            echo "=== stop $SERVICE_NAME success"
        else
            echo "=== stop $SERVICE_NAME failed, process pid is:$P_ID"
        fi
        ;;

    restart)
        $0 stop
        sleep 2
        $0 start
        echo "=== restart $SERVICE_NAME success"
        ;;

    *)
        echo "Usage: filebeat_run.sh {start|stop|restart}"
        ;;

esac
exit 0