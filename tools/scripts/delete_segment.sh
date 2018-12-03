segment_dir='/home/apps/vdl/vdldata/log'
end_point='http://192.168.0.1:i9000'
keep_segment_count='50'
vdl_ctl='/home/apps/vdl/vdlserver/src/github.com/vipshop/vdl/bin/vdlctl'
logstream="logstream1"

#check delete_segment.sh if exist
#count=`ps aux|grep "delete_segment.sh"|grep -v grep|awk '{print $2}'|wc -l`
#if [[ $count -gt 2 ]]; then
#  echo "delete_segment.sh exist"
#  exit
#fi

cd $segment_dir
segment_count=`ls|grep "log"|wc -l`
let delete_count=$segment_count-$keep_segment_count
if [[ $delete_count -gt 0 ]]; then
    for delete_segment in $( ls | grep "log"|head "-$delete_count");
    do
        echo "delete segment:" $delete_segment
        ${vdl_ctl} logstream delete ${logstream}  ${delete_segment} --endpoints ${end_point}
        sleep 3
    done
fi

