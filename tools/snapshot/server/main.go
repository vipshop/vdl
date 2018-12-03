package main

import (
	"flag"
	"fmt"
)

var (
	//metaFileDir  *string = flag.String("meta_dir", "", "snapshot metadata file path,should not equal data dir")
	dataFileDir  *string = flag.String("data_dir", "", "the raft group data dir")
	serverAddr   *string = flag.String("addr", "", "server address<ip:port>")
	vdlAdminAddr *string = flag.String("vdl_admin", "", "VDL admin server address<ip:port>")
	baseDir      *string = flag.String("base_dir", "", "send server work dir,should not equal data dir")
	rateLimit    *int    = flag.Int("rate_limit", 0, "send file rate limit,MB per second,0 is not limited")
)

func main() {
	//解析配置
	flag.Parse()

	conf := &ServerConfig{
		Addr:        *serverAddr,
		VdlAdminUrl: *vdlAdminAddr,
		DataDir:     *dataFileDir,
		BaseDir:     *baseDir,
		RateLimit:   *rateLimit,
	}

	err := checkConfig(conf)
	if err != nil {
		fmt.Printf("config error,err=%s, --help:for help\n", err.Error())
		return
	}
	//初始化http server
	snapServer, err := NewSnapSendServer(conf)
	if err != nil {
		fmt.Printf("NewSnapSendServer errro,err=%s,conf=%v\n",
			err.Error(), *conf)
		return
	}
	err = snapServer.Start()
	if err != nil {
		fmt.Printf("snapServer start error,err=%s", err.Error())
		return
	}
}

func checkConfig(conf *ServerConfig) error {
	if len(conf.DataDir) == 0 || conf.RateLimit < 0 ||
		len(conf.BaseDir) == 0 || len(conf.Addr) == 0 {
		return errArgsNotAvail
	}
	if conf.BaseDir == conf.DataDir {
		return errDirNotAvail
	}
	return nil
}
