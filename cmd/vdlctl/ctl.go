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

package main

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/vipshop/vdl/cmd/vdlctl/command"
)

const (
	cliName        = "vdlctl"
	cliDescription = "A simple command line client for VDL."

	defaultDialTimeout    = 2 * time.Second
	defaultCommandTimeOut = 5 * time.Second
)

var (
	rootCmd = &cobra.Command{
		Use:        cliName,
		Short:      cliDescription,
		SuggestFor: []string{"vdlctl"},
	}
)

func init() {
	rootCmd.PersistentFlags().StringSliceVar(&command.GlobalFlagsInstance.Endpoints, "endpoints", nil, "gRPC endpoints")
	rootCmd.PersistentFlags().DurationVar(&command.GlobalFlagsInstance.CommandTimeOut, "command-timeout", defaultCommandTimeOut, "timeout for short running command (excluding dial timeout), usage: --command-timeout=20s, default: 5s")
	rootCmd.PersistentFlags().DurationVar(&command.GlobalFlagsInstance.DialTimeout, "dial-timeout", defaultDialTimeout, "dial timeout for client connections, usage: --dial-timeout=5s, default: 2s")

	rootCmd.AddCommand(
		command.NewMemberCommand(),
		command.NewLogStreamCommand(),
		command.NewDebugSwitchCommand(),
		command.NewSnapshotMetaCommand(),
		command.NewVersionCommand(),
		command.NewRateCommand(),
	)
}
