#!/usr/bin/env bash
producer='/Users/flike/Downloads/kafka_2.11-0.10.2.0/bin/kafka-console-producer.sh'
src_msg_file='/Users/flike/Downloads/kafka_2.11-0.10.2.0/bin/src_msg_file.txt'
offset_file='/Users/flike/Downloads/kafka_2.11-0.10.2.0/bin/offset.txt'
message_count=10000
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

base_offset=`cat $offset_file`
max_offset=`expr ${base_offset} + ${message_count}`

echo "send message from ${base_offset} to ${max_offset}"
#for ((i = ${base_offset}; i < ${max_offset}; i++)); do
for i in `seq -f "%g" ${base_offset} ${max_offset}`;do
    message=${key}${i}${separator}${value}${i}
   echo ${message}|$producer --broker-list 127.0.0.1:8000 --topic logstream1 \
       --property parse.key=true \
       --property key.separator=,
   echo ${message} >> ${src_msg_file}
   #10ms
   usleep 10000
done
echo "set ${max_offset} into ${offset_file}"
echo ${max_offset} > ${offset_file}
