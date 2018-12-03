#!/usr/bin/env bash

consumer='/Users/flike/Downloads/kafka_2.11-0.10.2.0/bin/kafka-console-consumer.sh'
dst_msg_file='/Users/flike/Downloads/kafka_2.11-0.10.2.0/bin/dst_msg_file.txt'

if [ ! -f ${dst_msg_file} ]; then
  touch ${dst_msg_file}
fi

${consumer} --topic logstream1 \
--bootstrap-server 127.0.0.1:8000 \
--consumer-property enable.auto.commit=false \
--offset 0 --partition 0 \
--property print.key=true \
--property key.separator=, >> ${dst_msg_file}