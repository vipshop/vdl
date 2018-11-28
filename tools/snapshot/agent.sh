#!/bin/bash
. $(dirname "$0")/config.sh

#create snapshot sub dir
snap_count=`find ${snap_dir}/${raft_group} -mindepth 1 -maxdepth 1 -type d -name "2*"|wc -l`

#delete unnecessary backups
while [[ ${snap_count} -ge ${keep_snap_count} ]]
do
    oldest_version=`find ${snap_dir}/${raft_group} -mindepth 1 -maxdepth 1 -type d -name "2*"|sort|head -1`
    rm -rf ${oldest_version}
    snap_count=`find ${snap_dir}/${raft_group} -mindepth 1 -maxdepth 1 -type d -name "2*"|wc -l`
done

#create snapshot dir
sub_dir_name=`date "+%Y%m%d%H%M%S"`
snap_dir_current=${snap_dir}/${raft_group}/${sub_dir_name}
mkdir -p ${snap_dir_current}

#disable delete segment

msg=`${vdlctl} logstream set_delete_perm forbid ${raft_group} --endpoints ${vdl_snapshot}`
if [[ "${msg}"x != "OK"x ]];then
    echo "set forbid permission of deleting segment failed"
    exit 1
fi

#######receive snapshot file
${agent_bin_dir}/receive_agent \
--server_addr=${snap_server_addr} \
--raft_group=${raft_group} \
--boot_mode=new \
--snap_dir=${snap_dir_current}

download_status=`cat ${snap_dir_current}/download_status.txt`
if [[ "${download_status}"x != "download_success"x ]];then
    echo "Fail:receive_agent receive snapshot file failed, please try again"
    exit 1
else
    echo "Success:receive_agent receive snapshot file success"
fi

#check node status
exist_vdl=`ps aux|grep ${vdl_config_file}|grep -v grep|wc -l`
if [[ ${exist_vdl} -gt 0 ]];then
    echo "vdl has existed, stop it before restore snapshot"
    exit 1
fi

#restore snapshot

${agent_bin_dir}/restore_agent \
--vdl_leader_admin=${vdl_leader_admin} \
--server_name=${server_name} \
--node_status=${node_status} \
--peer_url=${peer_url} \
--raft_group=${raft_group} \
--restore_mode=${restore_mode} \
--snap_dir=${snap_dir_current} \
--data_dir=${data_dir} \
--stable_dir=${stable_dir}

restore_status=`cat ${snap_dir_current}/restore_status.txt`
if [[ "${restore_status}"x != "restore_success"x ]];then
    echo "Fail:restore_agent restore snapshot failed, please try again"
    exit 1
else
    echo "Success:restore_agent restore snapshot success"
fi

msg=`${vdlctl} logstream set_delete_perm allow ${raft_group} --endpoints ${vdl_snapshot}`
if [[ "${msg}"x != "OK"x ]];then
    echo "set allow permission of deleting segment failed"
    exit 1
fi