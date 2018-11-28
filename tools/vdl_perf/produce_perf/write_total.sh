#!/bin/sh
result_dir='/home/apps/vdl/vdlserver/src/gitlab.tools.vipshop.com/distributedstorage/perf_result'
bin_dir='/home/apps/kafka/kafka_2.11-0.10.2.0'

echo "========================================100==============================================">> $result_dir/vdl_produce_result.txt

echo "============begin produce 100 batch=1==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=1 -rtt=0.03 -message_size=100 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 100 batch=1==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 100 batch=10==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=10 -rtt=0.03 -message_size=100 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 100 batch=10==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 100 batch=25==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=25 -rtt=0.03 -message_size=100 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 100 batch=25==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 100 batch=50==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=50 -rtt=0.03 -message_size=100 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 100 batch=50==========" >> $result_dir/vdl_produce_result.txt

echo "========================================500==============================================">> $result_dir/vdl_produce_result.txt

echo "============begin produce 500 batch=1==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=1 -rtt=0.03 -message_size=500 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 500 batch=1==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 500 batch=10==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=10 -rtt=0.03 -message_size=500 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txtt
echo "============end produce 500 batch=10==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 500 batch=25==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=25 -rtt=0.03 -message_size=500 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 500 batch=25==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 500 batch=50==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=50 -rtt=0.03 -message_size=500 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 500 batch=50==========" >> $result_dir/vdl_produce_result.txt


echo "========================================1024==============================================">> $result_dir/vdl_produce_result.txt

echo "============begin produce 1024 batch=1==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=1 -rtt=0.03 -message_size=1024 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 1024 batch=1==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 1024 batch=10==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=10 -rtt=0.03 -message_size=1024 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 1024 batch=10==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 1024 batch=25==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=25 -rtt=0.03 -message_size=1024 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 1024 batch=25==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 1024 batch=50==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=50 -rtt=0.03 -message_size=1024 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 1024 batch=50==========" >> $result_dir/vdl_produce_result.txt

echo "========================================4096==============================================">> $result_dir/vdl_produce_result.txt

echo "============begin produce 4096 batch=1==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=1 -rtt=0.03 -message_size=4096 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 4096 batch=1==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 4096 batch=10==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=10 -rtt=0.03 -message_size=4096 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 4096 batch=10==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 4096 batch=25==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=25 -rtt=0.03 -message_size=4096 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 4096 batch=25==========" >> $result_dir/vdl_produce_result.txt

echo "============begin produce 4096 batch=50==========" >> $result_dir/vdl_produce_result.txt
$bin_dir/bin/produce_perf -messages=1000000 -batch=50 -rtt=0.03 -message_size=4096 -topic=logstream1 -broker=192.168.0.1:8000 -sampling=10000 >> $result_dir/vdl_produce_result.txt
echo "============end produce 4096 batch=50==========" >> $result_dir/vdl_produce_result.txt

