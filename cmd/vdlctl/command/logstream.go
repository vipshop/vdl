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

package command

import (
	"fmt"

	"strings"

	"github.com/spf13/cobra"
	"github.com/vipshop/vdl/cmd/cmdutil"
	"github.com/vipshop/vdl/server/adminapi/apipb"
	"google.golang.org/grpc"
	"strconv"
)

const (
	AllowDeletePerm  = "allow"
	ForbidDeletePerm = "forbid"
)

// NewLogStream returns the cobra command for "member".
func NewLogStreamCommand() *cobra.Command {
	mc := &cobra.Command{
		Use:   "logstream <subcommand>",
		Short: "logstream related commands",
	}

	mc.AddCommand(StartLogStreamCommand())
	mc.AddCommand(StopLogStreamCommand())
	mc.AddCommand(DeleteLogStreamFiles())
	mc.AddCommand(GetDeletePermission())
	mc.AddCommand(SetDeletePermission())
	mc.AddCommand(LeaderTransferCommand())
	return mc
}

func StartLogStreamCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "start <logStreamName>",
		Short: "Start a logstream",

		Run: startFunc,
	}
	return cc
}

func StopLogStreamCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "stop <logStreamName>",
		Short: "stop a logstream",

		Run: stopFunc,
	}
	return cc
}

func DeleteLogStreamFiles() *cobra.Command {
	cc := &cobra.Command{
		Use:   "delete <logStreamName> <file1,file2>",
		Short: "delete logstream segment files by name",

		Run: deleteFileFunc,
	}
	return cc
}

func LeaderTransferCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "leader_transfer <logStreamName> <transferee>",
		Short: "leader transfer for logstream",

		Run: doLeaderTransfer,
	}
	return cc
}

func doLeaderTransfer(cmd *cobra.Command, args []string) {

	if len(args) != 2 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("args count must be 2"))
	}

	logstreamName := args[0]
	transferee, err := strconv.ParseUint(args[1], 16, 64)
	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	ctx, cancel := commandCtx(cmd)
	client := MustClient()

	resp, err := client.LeaderTransfer(ctx, &apipb.LogStreamLeaderTransferRequeset{
		LogstreamName: logstreamName,
		Transferee:    transferee,
	}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	Display.LogStreamLeaderTransfer(resp)

}

func deleteFileFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("args count must be 2"))
	}
	client := MustClient()
	logstreamName := args[0]
	segments := strings.Split(args[1], ",")
	ctx, cancel := commandCtx(cmd)
	resp, err := client.DeleteFile(ctx, &apipb.DeleteFileRequest{
		LogstreamName: logstreamName,
		SegmentFiles:  segments,
	}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}
	Display.DeleteLogStreamFile(resp)
}

func GetDeletePermission() *cobra.Command {
	cc := &cobra.Command{
		Use:   "get_delete_perm <logStreamName>",
		Short: "get the delete logstream segment permission",

		Run: getDeletePermission,
	}
	return cc
}

func getDeletePermission(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("args count must be 1"))
	}
	client := MustClient()
	logstreamName := args[0]
	ctx, cancel := commandCtx(cmd)
	resp, err := client.GetDeletePermission(ctx, &apipb.GetDeletePermissionReq{
		LogstreamName: logstreamName,
	}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}
	Display.GetDeletePermission(resp)
}

func SetDeletePermission() *cobra.Command {
	cc := &cobra.Command{
		Use:   "set_delete_perm <allow or forbid> <logStreamName>",
		Short: "set the delete logstream segment permission",

		Run: setDeletePermission,
	}
	return cc
}

func setDeletePermission(cmd *cobra.Command, args []string) {
	var deleteSwitch uint64
	if len(args) != 2 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("args count must be 2"))
	}

	client := MustClient()

	if args[0] != AllowDeletePerm && args[0] != ForbidDeletePerm {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("arg must be 'allow' or 'forbid'"))
	}
	if args[0] == AllowDeletePerm {
		deleteSwitch = 1
	} else {
		deleteSwitch = 0
	}
	logstreamName := args[1]
	ctx, cancel := commandCtx(cmd)
	resp, err := client.SetDeletePermission(ctx, &apipb.SetDeletePermissionReq{
		LogstreamName: logstreamName,
		Switch:        deleteSwitch,
	}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}
	Display.SetDeletePermission(resp)
}

func startFunc(cmd *cobra.Command, args []string) {

	if len(args) != 1 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("logstream name not provided."))
	}

	logstreamName := args[0]

	client := MustClient()

	ctx, cancel := commandCtx(cmd)
	resp, err := client.LogStreamStart(ctx, &apipb.LogStreamStartRequest{
		LogstreamName: logstreamName,
	}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	Display.LogStreamStart(resp)

}

func stopFunc(cmd *cobra.Command, args []string) {

	if len(args) != 1 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("logstream name not provided."))
	}

	logstreamName := args[0]

	client := MustClient()

	ctx, cancel := commandCtx(cmd)
	resp, err := client.LogStreamStop(ctx, &apipb.LogStreamStopRequest{
		LogstreamName: logstreamName,
	}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	Display.LogStreamStop(resp)
}
