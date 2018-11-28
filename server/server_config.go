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
	"errors"
	"net"
	"net/url"
	"time"
)

type ServerConfig struct {

	// VDL Admin Listener
	AdminListenerAddress string

	// client listener address
	ClientListenerAddress string

	// metrics listener address
	MetricsListenerAddress string

	// vdl server name, one vdl server have only one server name
	ServerName string

	// currently rocksdb dir
	StableStoreDir string

	// Millisecond for raft heartbeat
	HeartbeatMs time.Duration
	//Millisecond for idle connections timeout
	ConnectionsMaxIdle time.Duration
	// HeartbeatTick is 1, ElectionTicks is N times than HeartbeatTick
	ElectionTicks int
	//glog dir
	GlogDir string
	//glog level
	Debug bool
	//alarm script path
	AlarmScriptPath string
}

//
func (conf *ServerConfig) ValidateServerConfig() error {

	adminListenerUrl, err := url.Parse(conf.AdminListenerAddress)
	if err != nil {
		return err
	}

	if adminListenerUrl.Scheme != "http" {
		return errors.New("AdminListenerUrl should be http")
	}

	metricsListenerListenerUrl, err := url.Parse(conf.MetricsListenerAddress)
	if err != nil {
		return err
	}
	if metricsListenerListenerUrl.Scheme != "http" {
		return errors.New("MetricsListenerAddress should be http")
	}

	_, err = net.ResolveTCPAddr("tcp", conf.ClientListenerAddress)
	if err != nil {
		return err
	}
	return nil
}
