#!/bin/sh

#vdl启动和停止脚步
# vdl_run.sh start|stop|restart

## 服务所在目录
SERVICE_DIR="/apps/svr/vdl"
CONFIG_FILE="/apps/conf/vdl/vdl.conf.yml"
SERVICE_NAME="vdl"
STDERR_LOG="/apps/log/vdl/stderr.log"
#VDL会响应该退出信号
KILL_TYPE="-15"

case "$1" in

    start)
        cd $SERVICE_DIR
        count=`ps aux|grep ${CONFIG_FILE}|grep -v grep|wc -l`
        if [ $count -gt 0 ];then
            echo "=== $SERVICE_NAME is running, please use restart command"
        else
            #nohup &  以守护进程启动
            nohup ./bin/vdl start -f ${CONFIG_FILE} 2 > ${STDERR_LOG} &
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
        echo "Usage: vdl_run.sh {start|stop|restart}"
        ;;

esac
exit 0