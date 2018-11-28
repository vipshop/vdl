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

	"github.com/spf13/cobra"
	"github.com/vipshop/vdl/cmd/cmdutil"
	"github.com/vipshop/vdl/server/adminapi/apipb"
	"google.golang.org/grpc"
)

// NewDebugCommand,enable debug
func NewDebugSwitchCommand() *cobra.Command {
	mc := &cobra.Command{
		Use:   "debug <1/0>",
		Short: "debug log switch",
		Run:   debugSwitch,
	}
	return mc
}

func debugSwitch(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("args count must be 1"))
	}
	client := MustClient()
	debugSwitch, err := strconv.ParseUint(args[0], 16, 64)
	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}
	ctx, cancel := commandCtx(cmd)
	resp, err := client.DebugSwitch(ctx, &apipb.DebugSwitchRequest{
		Switch: debugSwitch,
	}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}
	Display.DebugSwitch(resp)
}
