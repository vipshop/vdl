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

	"github.com/spf13/cobra"
	"github.com/vipshop/vdl/cmd/cmdutil"
	"github.com/vipshop/vdl/server/adminapi/apipb"
	"google.golang.org/grpc"
)

func NewSnapshotMetaCommand() *cobra.Command {
	mc := &cobra.Command{
		Use:   "snapshot <subcommand>",
		Short: "snapshot related commands",
	}

	mc.AddCommand(newSnapshotMetaCreateCommand())

	return mc
}

func newSnapshotMetaCreateCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "create <raftGroupName>",
		Short: "create the snapshot meta of raftGroupName",

		Run: createSnapshotMetaFunc,
	}

	return cc
}

func createSnapshotMetaFunc(cmd *cobra.Command, args []string) {

	checkEndpoints()

	if len(args) != 1 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("raftgroup name not provided."))
	}

	raftGroupName := args[0]

	ctx, cancel := commandCtx(cmd)
	client := MustClient()

	resp, err := client.CreateSnapshotMeta(ctx, &apipb.SnapshotRequeest{RaftGroup: raftGroupName}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	Display.CreateSnapshotMeta(resp)
}
