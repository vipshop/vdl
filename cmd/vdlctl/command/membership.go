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
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/vipshop/vdl/cmd/cmdutil"
	"github.com/vipshop/vdl/server/adminapi/apipb"
	"google.golang.org/grpc"
)

var memberPeerURLs string

// NewMemberCommand returns the cobra command for "member".
func NewMemberCommand() *cobra.Command {
	mc := &cobra.Command{
		Use:   "member <subcommand>",
		Short: "Membership related commands",
	}

	mc.AddCommand(newMemberAddCommand())
	mc.AddCommand(newMemberRemoveCommand())
	mc.AddCommand(newMemberUpdateCommand())
	mc.AddCommand(newMemberListCommand())

	return mc
}

// NewMemberAddCommand returns the cobra command for "member add".
func newMemberAddCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "add <logStreamName> <serverName> [options]",
		Short: "Adds a member into the log stream cluster",

		Run: memberAddCommandFunc,
	}

	cc.Flags().StringVar(&memberPeerURLs, "peer-urls", "", "comma separated peer URLs for the new member.")

	return cc
}

// NewMemberRemoveCommand returns the cobra command for "member remove".
func newMemberRemoveCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "remove <logStreamName> <memberID>",
		Short: "Removes a member from the log stream cluster",

		Run: memberRemoveCommandFunc,
	}

	return cc
}

// NewMemberUpdateCommand returns the cobra command for "member update".
func newMemberUpdateCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "update <logStreamName> <memberID> [options]",
		Short: "Updates a member in the log stream cluster",

		Run: memberUpdateCommandFunc,
	}

	cc.Flags().StringVar(&memberPeerURLs, "peer-urls", "", "comma separated peer URLs for the updated member.")

	return cc
}

// NewMemberListCommand returns the cobra command for "member list".
func newMemberListCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "list <logStreamName>",
		Short: "Lists all members in the log stream cluster",

		Run: memberListCommandFunc,
	}

	return cc
}

func memberListCommandFunc(cmd *cobra.Command, args []string) {

	checkEndpoints()

	if len(args) != 1 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("logstream name not provided."))
	}

	logstreamName := args[0]

	ctx, cancel := commandCtx(cmd)
	client := MustClient()

	resp, errList := client.MemberList(ctx, &apipb.MemberListRequest{LogstreamName: logstreamName}, grpc.FailFast(false))
	cancel()

	if errList != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, errList)
	}

	Display.MemberList(resp)
}

// memberUpdateCommandFunc executes the "member update" command.
// Use:   "update <logStreamName> <memberID> --peer-urls"
func memberUpdateCommandFunc(cmd *cobra.Command, args []string) {

	checkEndpoints()
	checkPeerUrl()

	if len(args) != 2 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("logStreamName and memberID not provided"))
	}

	logstreamName := args[0]
	memberId, err := strconv.ParseUint(args[1], 16, 64)
	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	ctx, cancel := commandCtx(cmd)
	client := MustClient()

	resp, err := client.MemberUpdate(ctx, &apipb.MemberUpdateRequest{
		LogstreamName: logstreamName,
		ID:            memberId,
		PeerURLs:      strings.Split(memberPeerURLs, ","),
	}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	Display.MemberUpdate(memberId, resp)

}

// memberRemoveCommandFunc executes the "member remove" command.
// Use:   "remove <logStreamName> <memberID>",
func memberRemoveCommandFunc(cmd *cobra.Command, args []string) {

	checkEndpoints()

	if len(args) != 2 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("logStreamName and memberID not provided"))
	}

	logstreamName := args[0]
	memberId, err := strconv.ParseUint(args[1], 16, 64)
	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	ctx, cancel := commandCtx(cmd)
	client := MustClient()

	resp, err := client.MemberRemove(ctx, &apipb.MemberRemoveRequest{
		LogstreamName: logstreamName,
		ID:            memberId,
	}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	Display.MemberRemove(memberId, resp)
}

// memberAddCommandFunc executes the "member add" command.
// Use:   "add <logStreamName> --peer-urls",
func memberAddCommandFunc(cmd *cobra.Command, args []string) {

	checkEndpoints()
	checkPeerUrl()

	if len(args) != 2 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, cmd.Help())
	}

	logstreamName := args[0]
	serverName := args[1]

	ctx, cancel := commandCtx(cmd)
	client := MustClient()

	resp, errAdd := client.MemberAdd(ctx, &apipb.MemberAddRequest{
		ServerName:    serverName,
		LogstreamName: logstreamName,
		PeerURLs:      strings.Split(memberPeerURLs, ","),
	}, grpc.FailFast(false))

	cancel()

	if errAdd != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, errAdd)
	}

	Display.MemberAdd(resp)

}

func checkPeerUrl() {
	if len(memberPeerURLs) == 0 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("peer-urls not provided"))
	}
}
