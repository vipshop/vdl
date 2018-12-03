// Copyright 2018 The etcd Authors. All rights reserved.
// Use of this source code is governed by a Apache
// license that can be found in the LICENSES/etcd-LICENSE file.
//
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

package raftgroup

import (
	"io/ioutil"
	defaultLog "log"
	"net"
	"net/http"
	"time"

	"github.com/vipshop/vdl/consensus/raft"
	"github.com/vipshop/vdl/consensus/raft/raftpb"
	"github.com/vipshop/vdl/consensus/raftgroup/api/apiserver"
	"github.com/vipshop/vdl/consensus/raftgroup/stats"
	"github.com/vipshop/vdl/consensus/rafthttp"
	raftPeerHandler "github.com/vipshop/vdl/consensus/rafthttp/handler"
	"github.com/vipshop/vdl/consensus/rafthttp/httperror"
	"github.com/vipshop/vdl/consensus/rafthttp/transport"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/types"
	"golang.org/x/net/context"
)

type peerListener struct {
	net.Listener
	serve func() error
	close func(context.Context) error
}

func (r *RaftGroup) setPeerHandler() {

	mux := http.NewServeMux()
	// configure peer handlers after rafthttp.Transport started
	raftPeerHandler.AppendRaftPeerHandler(r.transport, mux)
	appendRaftGroupAPIHandler(mux, r)

	for i := range r.peerListener {
		srv := &http.Server{
			Handler:     mux,
			ReadTimeout: 5 * time.Minute,
			ErrorLog:    defaultLog.New(ioutil.Discard, "", 0), // do not log user error
		}
		r.peerListener[i].serve = func() error {
			return srv.Serve(r.peerListener[i].Listener)
		}
		r.peerListener[i].close = func(ctx context.Context) error {
			// gracefully shutdown http.Server
			// close open listeners, idle connections
			// until context cancel or time-out
			return srv.Shutdown(ctx)
		}
	}
}

func (r *RaftGroup) startPeerListener() (err error) {

	peers := make([]*peerListener, len(r.GroupConfig.ListenerUrls))
	defer func() {
		if err == nil {
			return
		}
		for i := range peers {
			if peers[i] != nil && peers[i].close != nil {
				glog.Info("stopping listening for peers on ", r.GroupConfig.ListenerUrls[i].String())
				peers[i].close(context.Background())
			}
		}
	}()

	for i, u := range r.GroupConfig.ListenerUrls {

		peers[i] = &peerListener{close: func(context.Context) error { return nil }}
		peers[i].Listener, err = rafthttp.NewListener(u, nil)
		if err != nil {
			return err
		}
		// once serve, overwrite with 'http.Server.Shutdown'
		peers[i].close = func(context.Context) error {
			return peers[i].Listener.Close()
		}
		glog.Info("listening for peers on ", u.String())
	}
	r.peerListener = peers
	return nil
}

func (r *RaftGroup) startPeerHandler() {
	for _, pl := range r.peerListener {
		go func(l *peerListener) {
			r.peerListenerErrHandler(l.serve())
		}(pl)
	}
}

func (r *RaftGroup) startPeerTransport() (err error) {

	sstats := stats.NewServerStats(r.GroupConfig.VDLServerName, r.Membership.GetSelfMemberID().String())
	lstats := stats.NewLeaderStats(r.Membership.GetSelfMemberID().String())

	// TODO: move transport initialization near the definition of remote
	r.transport = &rafthttp.Transport{
		TLSInfo:     transport.TLSInfo{},
		DialTimeout: r.GroupConfig.peerDialTimeout(),
		ID:          r.Membership.GetSelfMemberID(),
		//URLs:        cfg.PeerURLs, //TODO.
		ClusterID: r.Membership.ClusterID,
		Raft:      &peerTransportRaftHandler{raftGroup: r},
		//Snapshotter: ss, //TODO.
		ServerStats: sstats,
		LeaderStats: lstats,
		ErrorC:      r.errorChanForStop,
	}
	if err = r.transport.Start(); err != nil {
		return err
	}

	return nil
}

type peerTransportRaftHandler struct {
	raftGroup *RaftGroup
}

func (t *peerTransportRaftHandler) Process(ctx context.Context, m raftpb.Message) error {
	if t.raftGroup.Membership.IsIDRemoved(types.ID(m.From)) {
		glog.Warningf("reject message from removed member %s", types.ID(m.From).String())
		return httperror.NewHTTPError(http.StatusForbidden, "cannot process message from removed member")
	}
	//TODO.
	//if m.Type == raftpb.MsgApp {
	//	s.stats.RecvAppendReq(types.ID(m.From).String(), m.Size())
	//}
	return t.raftGroup.raftNode.Step(ctx, m)
}

func (t *peerTransportRaftHandler) IsIDRemoved(id uint64) bool {
	return t.raftGroup.Membership.IsIDRemoved(types.ID(id))
}

func (t *peerTransportRaftHandler) ReportUnreachable(id uint64) {
	t.raftGroup.raftNode.ReportUnreachable(id)
}

// ReportSnapshot reports snapshot sent status to the raft state machine,
// and clears the used snapshot from the snapshot store.
func (t *peerTransportRaftHandler) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	t.raftGroup.raftNode.ReportSnapshot(id, status)
}

func appendRaftGroupAPIHandler(serveMux *http.ServeMux, r *RaftGroup) {

	serveMux.Handle(apiserver.VDLServerInfoPrefix, apiserver.NewVDLServerInfoHandler(
		uint64(r.Membership.GetSelfMemberID()), r.GroupConfig.VDLServerID,
		r.GroupConfig.VDLClientListenerHost, r.GroupConfig.VDLClientListenerPort))
}
