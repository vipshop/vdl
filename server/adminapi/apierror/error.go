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

package apierror

import (
	"github.com/vipshop/vdl/logstore"
	"github.com/vipshop/vdl/server/servererror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	ErrGRPCMemberBadURLs       = grpc.Errorf(codes.InvalidArgument, "VDL: given member URLs are invalid")
	ErrGRPCStreamAlreadyExists = grpc.Errorf(codes.InvalidArgument, "VDL: given stream already exists")
	ErrGRPCStreamNotExists     = grpc.Errorf(codes.InvalidArgument, "VDL: given stream not exists")
	ErrSegmentNotAllowDelete   = grpc.Errorf(codes.PermissionDenied, "VDL: segment files not all allow to delete")
	ErrDebugSwitchArgs         = grpc.Errorf(codes.InvalidArgument, "VDL: given args not allow(only be 0/1)")
	ErrMemberIDNotExists       = grpc.Errorf(codes.InvalidArgument, "VDL: given member ID not exists")
	ErrNoLeader                = grpc.Errorf(codes.InvalidArgument, "VDL: Please execute this cmd at leader node")
	ErrLogstreamNotStart       = grpc.Errorf(codes.InvalidArgument, "VDL: Log stream is not start at this node")
)

func ToRPCError(err error) error {
	switch err {

	case servererror.ErrStreamAlreadyExists:
		return ErrGRPCStreamAlreadyExists
	case servererror.ErrStreamNotExists:
		return ErrGRPCStreamNotExists
	case logstore.ErrNotAllowDelete:
		return ErrSegmentNotAllowDelete
	default:
		return grpc.Errorf(codes.Unknown, err.Error())
	}
}
