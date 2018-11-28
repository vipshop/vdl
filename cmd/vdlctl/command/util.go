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
	"github.com/vipshop/vdl/server/adminapi/apiclient"
	"golang.org/x/net/context"
)

func commandCtx(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), GlobalFlagsInstance.CommandTimeOut)
}

func checkEndpoints() {
	if len(GlobalFlagsInstance.Endpoints) == 0 {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("endpoint not provided."))
	}
}

func MustClient() *apiclient.Client {

	client, err := apiclient.NewClient(GlobalFlagsInstance.Endpoints, GlobalFlagsInstance.DialTimeout)
	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}
	return client

}
