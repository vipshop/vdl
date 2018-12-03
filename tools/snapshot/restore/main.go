package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"path"
)

var (
	stableStoreDir *string = flag.String("stable_dir", "", "boltdb dir")
	dataDir        *string = flag.String("data_dir", "", "the raft group data dir")
	vdlAdminAddr   *string = flag.String("vdl_leader_admin", "", "VDL cluster leader admin server address<ip:port>")
	nodeStatus     *string = flag.String("node_status", "new", "new or old node to restore snapshot")
	snapDir        *string = flag.String("snap_dir", "", "snapshot file path,should not equal data dir")
	raftGroup      *string = flag.String("raft_group", "", "raft group name for snapshot")
	restoreMode    *string = flag.String("restore_mode", "cp", "mv or cp snap file to data dir")
	serverName     *string = flag.String("server_name", "", "if this node is new,need a server name for this node,"+
		"old node doesn't need set")
	peerURL *string = flag.String("peer_url", "", "if this node is new,need peer url for this node,"+
		"old node doesn't need set")
)

var (
	RestoreStatusName = "restore_status.txt"
	FailStatus        = "restore_fail"
	SuccessStatus     = "restore_success"
)

func main() {
	flag.Parse()
	conf := &RestorerConfig{
		DataDir:         *dataDir,
		StableStoreDir:  *stableStoreDir,
		VdlAdminUrl:     *vdlAdminAddr,
		RestoreDataMode: *restoreMode,
		SnapDir:         *snapDir,
		RaftGroup:       *raftGroup,
		NodeStatus:      *nodeStatus,
		ServerName:      *serverName,
		PeerURL:         *peerURL,
	}
	err := checkConfig(conf)
	if err != nil {
		fmt.Printf("restorer config error,err=%s, --help for help\n", err.Error())
		return
	}

	restorer := NewRestorer(conf)
	statusFilePath := path.Join(conf.SnapDir, RestoreStatusName)

	err = writeRestoreStatus(statusFilePath, FailStatus)
	if err != nil {
		fmt.Printf("writeRestoreStatus error,err=%s,filePath=%s", err.Error(), statusFilePath)
		return
	}

	err = restorer.Start()
	if err != nil {
		fmt.Printf("Start errro,err=%s\n", err.Error())
		return
	} else {
		err = writeRestoreStatus(statusFilePath, SuccessStatus)
		if err != nil {
			fmt.Printf("writeRestoreStatus error,err=%s,filePath=%s", err.Error(), statusFilePath)
			return
		}
	}
}

func checkConfig(conf *RestorerConfig) error {
	if len(conf.DataDir) == 0 || len(conf.StableStoreDir) == 0 ||
		len(conf.VdlAdminUrl) == 0 || len(conf.RestoreDataMode) == 0 ||
		len(conf.SnapDir) == 0 ||
		len(conf.NodeStatus) == 0 || len(conf.ServerName) == 0 ||
		len(conf.RaftGroup) == 0 || len(conf.PeerURL) == 0 {
		return errArgsIsNil
	}

	if (conf.NodeStatus != NewNode && conf.NodeStatus != OldNode) ||
		(conf.RestoreDataMode != MoveType && conf.RestoreDataMode != CopyType) {
		return errArgsNotAvail
	}

	return nil
}

func writeRestoreStatus(filePath string, status string) error {
	err := ioutil.WriteFile(filePath, []byte(status), FilePermission)
	if err != nil {
		fmt.Printf("WriteFile in writeRestoreStatus error,err=%s,filePath=%s,flag=%s",
			err.Error(), filePath, status)
		return err
	}
	return nil
}
