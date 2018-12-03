#!/usr/bin/env bash

metric_dir=/data/vdl_dev/vdl_work/metrics
keep_file_count=10

while true
do
    cd $metric_dir
    file_count=`ls $metric_dir|wc -l`
    if [[ $file_count -gt $keep_file_count ]];then
      delete_file=`ls $metric_dir|sort|head -1`
      rm $metric_dir/$delete_file
    else
      break
    fi
done