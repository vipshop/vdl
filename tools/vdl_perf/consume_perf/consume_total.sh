#!/bin/sh

result_dir='/home/apps/vdl/vdlserver/src/github.com/vipshop/vdl/tools/vdl_perf/consume_perf'
bin_dir='/home/apps/vdl/vdlserver/src/github.com/vipshop/vdl/tools/vdl_perf/consume_perf'
echo "============================consume 100======================" >> $result_dir/vdl_consume_result.txt
echo "============begin consume 100 batchSize=32KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=327680 -rtt=0.031 -message_size=100 -offset=0 >> $result_dir/vdl_consume_result.txt
echo "============end consume 100 batchSize=32KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 100 batchSize=64KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=655360 -rtt=0.031 -message_size=100 -offset=1000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 100 batchSize=64KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 100 batchSize=256KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=2621440 -rtt=0.031 -message_size=100 -offset=2000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 100 batchSize=256KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 100 batchSize=1024KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=10485760 -rtt=0.031 -message_size=100 -offset=3000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 100 batchSize=1024KB==========" >> $result_dir/vdl_consume_result.txt


echo "============================consume 500======================" >> $result_dir/vdl_consume_result.txt
echo "============begin consume 500 batchSize=32KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=327680 -rtt=0.031 -message_size=500 -offset=4000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 500 batchSize=32KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 500 batchSize=64KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=655360 -rtt=0.031 -message_size=500 -offset=5000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 500 batchSize=64KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 500 batchSize=256KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=2621440 -rtt=0.031 -message_size=500 -offset=6000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 500 batchSize=256KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 500 batchSize=1024KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=10485760 -rtt=0.031 -message_size=500 -offset=7000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 500 batchSize=1024KB==========" >> $result_dir/vdl_consume_result.txt



echo "============================consume 1024======================" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 1024 batchSize=32KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=327680 -rtt=0.031 -message_size=1024 -offset=8000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 1024 batchSize=32KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 1024 batchSize=64KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=655360 -rtt=0.031 -message_size=1024 -offset=9000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 1024 batchSize=64KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 1024 batchSize=256KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=2621440 -rtt=0.031 -message_size=1024 -offset=10000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 1024 batchSize=256KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 1024 batchSize=1024KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=10485760 -rtt=0.031 -message_size=1024 -offset=11000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 1024 batchSize=1024KB==========" >> $result_dir/vdl_consume_result.txt


echo "============================consume 4096======================" >> $result_dir/vdl_consume_result.txt
echo "============begin consume 4096 batchSize=32KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=327680 -rtt=0.031 -message_size=4096 -offset=12000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 4096 batchSize=32KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 4096 batchSize=64KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=655360 -rtt=0.031 -message_size=4096 -offset=13000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 4096 batchSize=64KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 4096 batchSize=256KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=2621440 -rtt=0.031 -message_size=4096 -offset=14000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 4096 batchSize=256KB==========" >> $result_dir/vdl_consume_result.txt

echo "============begin consume 4096 batchSize=1024KB==========" >> $result_dir/vdl_consume_result.txt
$bin_dir/consume_perf -messages=1000000 -broker=192.168.0.1:8000 -fetch_size=10485760 -rtt=0.031 -message_size=4096 -offset=15000000 >> $result_dir/vdl_consume_result.txt
echo "============end consume 4096 batchSize=1024KB==========" >> $result_dir/vdl_consume_result.txt
