// Copyright 2018 vip.com.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"net"
	"net/url"
	"sync"

	"encoding/binary"
	defaultLog "log"
	"math/rand"
	"time"

	"io/ioutil"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vipshop/vdl/consensus/raftgroup"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/jsonutil"
	apiutil "github.com/vipshop/vdl/server/adminapi/util"
	"github.com/vipshop/vdl/server/kafka"
	"github.com/vipshop/vdl/server/metricsserver"
	"github.com/vipshop/vdl/server/runtimeconfig"
	"github.com/vipshop/vdl/server/servererror"
	"github.com/vipshop/vdl/stablestore"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	stable_key_raft_group_key = "vdlserver-raftgroup"
	stable_key_server_id      = "vdlserver-id"
)

var (
	VDLServerGlobal *VDLServer
)

type VDLServer struct {
	rwLock    *sync.RWMutex
	streamMap map[string]*raftgroup.RaftGroup

	// listen url for admin
	AdminListener *listener

	// listen url for metrics
	MetricsListener *listener

	//server config
	ServerConfig *ServerConfig

	//Runtime config
	RuntimeConfig      *runtimeconfig.RuntimeConfig
	RuntimeConfigStore *runtimeconfig.RuntimeConfigStore

	//persistent for membership/voted id/etc..
	StableStore stablestore.StableStore

	// server id, which is generate random
	ServerID int32

	// chan for shutdown
	ShutdownCh chan struct{}

	// is new server
	IsNewServer bool

	// wait group for shutdown
	Wg sync.WaitGroup

	// kafka server
	kafkaServer *kafka.Server

	// metrics server
	metricsServer *metricsserver.MetricsServer

	// use to stop method to close once
	closeOnce sync.Once

	stoped chan struct{}
}

type listener struct {
	net.Listener
	serve func() error
	close func(context.Context) error
}

func NewVDLServer(conf *ServerConfig, metricsConfig *metricsserver.MetricsServerConfig) (*VDLServer, error) {

	if err := conf.ValidateServerConfig(); err != nil {
		return nil, err
	}

	// create stable store
	stableStore, err := stablestore.NewBoltDBStore(&stablestore.BoltDBStoreConfig{Dir: conf.StableStoreDir})
	if err != nil {
		return nil, err
	}

	// get or create server id
	serverID, isCreate, err := getOrCreateServerID(stableStore)
	if err != nil {
		return nil, err
	}

	VDLServerGlobal = &VDLServer{
		rwLock:       new(sync.RWMutex),
		streamMap:    make(map[string]*raftgroup.RaftGroup),
		ServerConfig: conf,
		StableStore:  stableStore,
		ServerID:     serverID,
		IsNewServer:  isCreate,
		ShutdownCh:   make(chan struct{}),
		stoped:       make(chan struct{}),
	}

	// get or create runtime config
	runtimeConfigStore := runtimeconfig.NewRuntimeConfigStore(stableStore)
	runtimeConfig := runtimeConfigStore.GetOrCreateRuntimeConfig()
	VDLServerGlobal.RuntimeConfigStore = runtimeConfigStore
	VDLServerGlobal.RuntimeConfig = runtimeConfig

	// new kafka server
	kafkaServer, err := kafka.NewServer(conf.ClientListenerAddress, conf.ConnectionsMaxIdle, VDLServerGlobal, VDLServerGlobal.RuntimeConfig)
	if err != nil {
		return nil, err
	}
	VDLServerGlobal.kafkaServer = kafkaServer

	// new metrics server
	metricsServer := metricsserver.NewMetricsServer(metricsConfig)
	VDLServerGlobal.metricsServer = metricsServer

	return VDLServerGlobal, nil

}

func (l *VDLServer) Start() (err error) {

	defer func() {
		if err != nil {
			if l.AdminListener != nil && l.AdminListener.Listener != nil {
				l.AdminListener.Close()
				glog.Info("stopping listening for admin requests on ", l.AdminListener)

			}

			if l.MetricsListener != nil && l.MetricsListener.Listener != nil {
				l.MetricsListener.Close()
				glog.Info("stopping listening for metrics requests on ", l.MetricsListener)
			}
		}
	}()

	//start client listener
	if err = l.startAdminListener(); err != nil {
		return err
	}

	//start metrics listener
	if err = l.startMetricsListener(); err != nil {
		return err
	}
	return nil
}

// stop should be call from kill signal or stop cmd
func (l *VDLServer) Stop() {
	l.closeOnce.Do(func() { close(l.ShutdownCh) })
	<-l.stoped
}

// wait until stop
func (l *VDLServer) Serve() error {

	// store server ID
	if l.IsNewServer {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(l.ServerID))
		err := l.StableStore.Put([]byte(stable_key_server_id), b)
		if err != nil {
			return err
		}
	}

	// start metrics server
	l.Wg.Add(1)
	go func() {
		l.metricsServer.Start()
		l.Wg.Done()
	}()

	// if kafka server cannot start, shutdown first and then return to stop vdl server
	kafkaStartErrorChan := make(chan error)
	l.Wg.Add(1)
	go func() {
		startErr := l.kafkaServer.Start()
		l.Wg.Done()
		if startErr != nil {
			kafkaStartErrorChan <- startErr
		}
	}()

	select {
	case err := <-kafkaStartErrorChan:
		l.shutdown()
		return err

	case <-l.ShutdownCh:
		l.shutdownKafka()
		l.shutdown()
		return nil
	}

	return nil
}

func (l *VDLServer) shutdownKafka() {
	glog.Info("shutting down kafka server")
	if err := l.kafkaServer.Close(); err != nil {
		glog.Warningf("can not close AdminListener [%s] ",
			l.AdminListener.Listener.Addr().String())
	}
	glog.Info("kafka server shutdown")
	glog.Flush()
}

func (l *VDLServer) shutdown() {

	glog.Info("shutting down Metrics Server")
	l.metricsServer.Stop()
	glog.Info("Metrics Server shutdown")

	if err := l.AdminListener.close(context.Background()); err != nil {
		glog.Warningf("can not close AdminListener [%s] ",
			l.AdminListener.Listener.Addr().String())
	}
	glog.Info("AdminListener shutdown")

	glog.Info("shutting down MetricsListener")
	if err := l.MetricsListener.close(context.Background()); err != nil {
		glog.Warningf("can not close MetricsListener [%s] ",
			l.MetricsListener.Listener.Addr().String())
	}
	glog.Info("MetricsListener shutdown")

	l.Wg.Wait()

	l.rwLock.Lock()
	defer l.rwLock.Unlock()

	for _, v := range l.streamMap {
		glog.Info("shutting down raftgroup:", v.GroupConfig.GroupName)
		if err := v.Stop(); err != nil {
			glog.Warning("can not close raft group", err)
		}
		glog.Info("raftgroup[", v.GroupConfig.GroupName, "] shutdown:")
	}

	glog.Info("shutting down StableStore")
	if err := l.StableStore.Close(); err != nil {
		glog.Warning("can not close StableStore", err)
	}
	glog.Info("StableStore shutdown")
	glog.Flush()
	l.stoped <- struct{}{}
}

func (l *VDLServer) AddRaftGroup(groupName string, conf *raftgroup.GroupConfig) (newGroup *raftgroup.RaftGroup, err error) {

	l.rwLock.Lock()
	defer l.rwLock.Unlock()

	if l.streamMap[groupName] != nil {
		return nil, servererror.ErrStreamAlreadyExists
	}

	conf.VDLServerName = l.ServerConfig.ServerName
	conf.VDLServerID = l.ServerID

	addr, err := net.ResolveTCPAddr("tcp", l.ServerConfig.ClientListenerAddress)
	if err != nil {
		return nil, err
	}

	conf.VDLClientListenerHost = addr.IP.String()
	conf.VDLClientListenerPort = int32(addr.Port)

	if newGroup, err = raftgroup.NewRaftGroup(conf, l.StableStore); err != nil {
		return nil, err
	}

	if err = newGroup.Start(); err != nil {
		return nil, err
	}

	l.streamMap[groupName] = newGroup

	l.StableStore.Put([]byte(stable_key_raft_group_key), jsonutil.MustMarshal(l.streamMap))

	return
}

func (l *VDLServer) GetRaftGroup(groupName string) (*raftgroup.RaftGroup, error) {

	l.rwLock.RLock()
	defer l.rwLock.RUnlock()

	if l.streamMap[groupName] == nil {
		return nil, servererror.ErrStreamNotExists
	}

	return l.streamMap[groupName], nil
}

func (l *VDLServer) GetLogStreamWrapper(groupName string) (raftgroup.LogStreamWrapper, error) {

	l.rwLock.RLock()
	defer l.rwLock.RUnlock()

	if l.streamMap[groupName] == nil {
		return nil, servererror.ErrStreamNotExists
	}

	return l.streamMap[groupName], nil
}

func (l *VDLServer) GetAllLogStreamNames() []string {
	l.rwLock.RLock()
	defer l.rwLock.RUnlock()
	logStreamNames := make([]string, 0, 8)
	for name, _ := range l.streamMap {
		logStreamNames = append(logStreamNames, name)
	}
	return logStreamNames
}

func (l *VDLServer) GetRuntimeConfig() *runtimeconfig.RuntimeConfig {
	return l.RuntimeConfig
}

func (l *VDLServer) GetRuntimeConfigStore() *runtimeconfig.RuntimeConfigStore {
	return l.RuntimeConfigStore
}

func (l *VDLServer) startAdminListener() (err error) {

	adminListenerUrl, _ := url.Parse(l.ServerConfig.AdminListenerAddress)

	var adminListener net.Listener
	if adminListener, err = net.Listen("tcp", adminListenerUrl.Host); err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	apiutil.RegisterServer(grpcServer, l, l)

	l.AdminListener = &listener{
		Listener: adminListener,
		close: func(context.Context) error {
			grpcServer.GracefulStop()
			return nil
		},
		serve: func() error {
			return grpcServer.Serve(adminListener)
		},
	}
	glog.Info("listening for admin requests on ", adminListenerUrl)
	l.Wg.Add(1)
	go func() {
		l.AdminListener.serve()
		l.Wg.Done()
	}()

	return nil
}

func (l *VDLServer) startMetricsListener() (err error) {

	metricsListenerListenerUrl, _ := url.Parse(l.ServerConfig.MetricsListenerAddress)

	var metricsListener net.Listener
	if metricsListener, err = net.Listen("tcp", metricsListenerListenerUrl.Host); err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Handler:     mux,
		ReadTimeout: 5 * time.Minute,
		ErrorLog:    defaultLog.New(ioutil.Discard, "", 0), // do not log user error
	}

	l.MetricsListener = &listener{
		Listener: metricsListener,
		close: func(ctx context.Context) error {
			return server.Shutdown(ctx)
		},
		serve: func() error {
			return server.Serve(metricsListener)
		},
	}

	glog.Info("listening for metrics requests on ", metricsListenerListenerUrl.Host)

	l.Wg.Add(1)
	go func() {
		l.MetricsListener.serve()
		l.Wg.Done()
	}()

	return nil
}

func getOrCreateServerID(stableStore stablestore.StableStore) (serverID int32, isCreate bool, err error) {

	serverID = 0
	isCreate = false
	var b []byte

	// get from store
	b, err = stableStore.Get([]byte(stable_key_server_id))
	if err != nil {
		return serverID, isCreate, err
	}

	// not exists in store
	if b == nil || len(b) == 0 {
		isCreate = true
		random := rand.New(rand.NewSource(time.Now().UnixNano()))
		serverID = random.Int31()
		return serverID, isCreate, nil
	} else {
		return int32(binary.BigEndian.Uint32(b)), isCreate, nil
	}

}
