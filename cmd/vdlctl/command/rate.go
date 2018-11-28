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
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/vipshop/vdl/cmd/cmdutil"
	"github.com/vipshop/vdl/server/adminapi/apipb"
	"github.com/vipshop/vdl/server/runtimeconfig"
	"google.golang.org/grpc"
)

// NewRateCommand returns the cobra command for "rate".
func NewRateCommand() *cobra.Command {
	mc := &cobra.Command{
		Use:   "rate <subcommand>",
		Short: "rate related commands",
	}
	mc.AddCommand(listRateCommand())
	mc.AddCommand(updateRateCommand())
	return mc
}

// listRateCommand returns the cobra command for "rate list".
func listRateCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "list",
		Short: "list all the configuration relate to rate",

		Run: listRateCommandFunc,
	}
	return cc
}

// updateRateCommand returns the cobra command for "rate update".
func updateRateCommand() *cobra.Command {
	cc := &cobra.Command{
		Use: "update <updateString>",
		Short: "update rate configuration, updateString using: key=value[,key=value], " +
			"key can be found by rate list command, " +
			"eg: IsEnableRateQuota=true,WriteRequestRate=100,WriteByteRate=100",

		Run: updateRateCommandFunc,
	}
	return cc
}

func updateRateCommandFunc(cmd *cobra.Command, args []string) {
	checkEndpoints()

	if len(args) != 1 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("updateString not provided."))
	}

	updateString := args[0]

	ctx, cancel := commandCtx(cmd)
	client := MustClient()

	// server side will check the request
	resp, err := client.UpdateRateInfo(ctx, &apipb.RateUpdateRequest{
		Msg: []byte(updateString),
	}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	rateConfig := &runtimeconfig.RuntimeRateConfig{}

	if err = json.Unmarshal(resp.Msg, rateConfig); err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	Display.UpdateRate(rateConfig)
}

// listRateCommandFunc executes the "rate add" command.
func listRateCommandFunc(cmd *cobra.Command, args []string) {

	checkEndpoints()

	ctx, cancel := commandCtx(cmd)
	client := MustClient()

	resp, err := client.ListRateInfo(ctx, &apipb.RateListRequest{}, grpc.FailFast(false))
	cancel()

	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	rateConfig := &runtimeconfig.RuntimeRateConfig{}

	if err = json.Unmarshal(resp.Msg, rateConfig); err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	Display.ListRate(rateConfig)

}
