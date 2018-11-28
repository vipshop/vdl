package main

import (
	"flag"
	"fmt"
)

var (
	serverAddr *string = flag.String("server_addr", "", "snapshot send server address<ip:port>")
	snapDir    *string = flag.String("snap_dir", "", "snapshot file path,should not equal data dir")
	raftGroup  *string = flag.String("raft_group", "", "raft group name for snapshot")
	bootMode   *string = flag.String("boot_mode", "new", "agent boot mode<new/resume>,new will clean snapDir,"+
		"resume will continue transfer files after the last interrupt transfer")
)

func main() {
	flag.Parse()
	conf := &AgentConfig{
		SnapDir:    *snapDir,
		ServerAddr: *serverAddr,
		RaftGroup:  *raftGroup,
		BootMode:   *bootMode,
	}
	err := checkConfig(conf)
	if err != nil {
		fmt.Printf("agent config error,err=%s, --help for help\n", err.Error())
		return
	}
	snapAgent, err := NewSnapReceiveAgent(conf)
	if err != nil {
		fmt.Printf("NewSnapReceiveAgent errro,err=%s,conf=%v\n",
			err.Error(), *conf)
		return
	}

	err = snapAgent.Start()
	if err != nil {
		fmt.Printf("snapAgent start error,err=%s\n", err.Error())
	}
	return
}

func checkConfig(conf *AgentConfig) error {
	if len(conf.SnapDir) == 0 || len(conf.ServerAddr) == 0 ||
		len(conf.BootMode) == 0 || len(conf.RaftGroup) == 0 ||
		(conf.BootMode != DownloadSnapNewMode && conf.BootMode != DownloadSnapResumeMode) {
		return errArgsNotAvail
	}
	return nil
}
