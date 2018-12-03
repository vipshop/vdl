#!/usr/bin/env bash

consumer='/home/apps/librdkafka/librdkafka-0.11.0/examples/rdkafka_example_cpp'
dst_msg_file='/home/apps/functest/dst_msg_file.txt'
broker='192.168.0.1:8000'
if [ ! -f ${dst_msg_file} ]; then
  touch ${dst_msg_file}
fi

${consumer} -C -t logstream1 -b ${broker} -o 0 -p 0 >> ${dst_msg_file}
