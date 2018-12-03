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
	"github.com/vipshop/vdl/version"
)

// NewDebugCommand,enable debug
func NewVersionCommand() *cobra.Command {
	mc := &cobra.Command{
		Use:   "version",
		Short: "show vdl version",
		Run:   showVersion,
	}
	return mc
}

func showVersion(cmd *cobra.Command, args []string) {
	fmt.Printf("Version:%s\n", version.Version)
	fmt.Printf("GitLog:%s\n", version.GitLog)
	fmt.Printf("Compile:%s\n", version.Compile)
}
