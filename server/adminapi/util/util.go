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

package util

import (
	"github.com/vipshop/vdl/server/adminapi/apipb"
	"github.com/vipshop/vdl/server/adminapi/apiserver"
	"google.golang.org/grpc"
)

func RegisterServer(grpcServer *grpc.Server, l apiserver.RaftGroupGetter, r apiserver.RateInfoGetter) {
	apipb.RegisterMembershipServer(grpcServer, apiserver.NewMembershipAdminServer(l))
	apipb.RegisterLogStreamAdminServer(grpcServer, apiserver.NewLogStreamAdminServer(l))
	apipb.RegisterDevToolServer(grpcServer, apiserver.NewDevToolServer())
	apipb.RegisterSnapshotAdminServer(grpcServer, apiserver.NewSnapshotAdminServer(l))
	apipb.RegisterRateAdminServer(grpcServer, apiserver.NewRateAdminServer(r))
}
