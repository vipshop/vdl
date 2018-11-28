#!/usr/bin/env bash

consumer='/home/apps/librdkafka/librdkafka-0.11.0/examples/rdkafka_example_cpp'
dst_msg_file='/home/apps/functest/dst_msg_file.txt'

while true
do
    dst_msg_file_size=ls -l ${dst_msg_file} | awk '{ print $5 }'
    #dst_msg_file文件超过1GB，则重置一次
    if [[ ${dst_msg_file} -gt 1000000000 ]];then
        echo "" > ${dst_msg_file}
    fi
    sleep 120
done