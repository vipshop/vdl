#!/usr/bin/env bash
producer='/home/apps/librdkafka/librdkafka-0.11.0/examples/rdkafka_example_cpp'
src_msg_file='/home/apps/functest/src_msg_file.txt'
offset_file='/home/apps/functest/offset.txt'
i=0
broker='192.168.0.1:8000'
key="test_key_"
value="test_message_"
separator=","

#if offset meta file exist
if [ ! -f ${offset_file} ]
then
  touch ${offset_file}
  echo "0" > ${offset_file}
fi

if [ ! -f ${src_msg_file} ]
then
  touch ${src_msg_file}
else
  echo > ${src_msg_file}
fi

while true
do
    i=`expr $i + 1`;
    message=${key}${i}${separator}${value}${i}
    echo ${message}|$producer -P -t logstream1 -p 0 -b ${broker}
    echo ${message} >> ${src_msg_file}

    file_size=`ls -l ${src_msg_file} | awk '{ print $5 }'`
    #发送消息文件超过1GB，则重置一次
    if [[ $file_size -gt 1000000000 ]];then
    echo "" > ${src_msg_file}
    fi

    echo ${i} >> ${offset_file}
    offset_file_size=`ls -l ${offset_file} | awk '{ print $5 }'`
    #offset文件超过1GB，则重置一次
    if [[ ${offset_file_size} -gt 1000000000 ]];then
    echo "" > ${offset_file}
    fi

    #10ms
    usleep 50000
done



