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
	"time"

	"os"

	"github.com/coreos/etcd/pkg/osutil"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vipshop/vdl/cmd/cmdutil"
	"github.com/vipshop/vdl/consensus/raftgroup"
	logstreampkg "github.com/vipshop/vdl/logstream"
	"github.com/vipshop/vdl/pkg/alarm"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/pkg/types"
	"github.com/vipshop/vdl/server"
	"github.com/vipshop/vdl/server/metricsserver"
	"vipshop.com/distributedstorage/fiu"
)

func NewStartCommand() *cobra.Command {
	mc := &cobra.Command{
		Use:   "start",
		Short: "Start VDL Server",
		Run:   startFunc,
	}
	return mc
}

func startFunc(cmd *cobra.Command, args []string) {

	if ConfigFile == "" {
		cmdutil.ExitWithError(cmdutil.ExitBadArgs, fmt.Errorf("config file must provide."))
	}

	viper.SetConfigFile(ConfigFile)

	if err := viper.ReadInConfig(); err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	checkConfig(cmd, args)

	serverConfig := &server.ServerConfig{
		ServerName:             viper.GetString("name"),
		StableStoreDir:         viper.GetString("stable-dir"),
		AdminListenerAddress:   viper.GetString("listen-admin-url"),
		HeartbeatMs:            time.Duration(viper.GetInt64("heartbeat-interval")) * time.Millisecond,
		ElectionTicks:          int(viper.GetInt64("election-timeout") / viper.GetInt64("heartbeat-interval")),
		ClientListenerAddress:  viper.GetString("listen-client-url"),
		MetricsListenerAddress: viper.GetString("listen-metrics-url"),
		ConnectionsMaxIdle:     time.Duration(viper.GetInt64("connections-max-idle")) * time.Millisecond,
		GlogDir:                viper.GetString("glog-dir"),
		Debug:                  viper.GetBool("debug"),
		AlarmScriptPath:        viper.GetString("alarm-script-path"),
	}

	err := serverConfig.ValidateServerConfig()
	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	//init glog
	glog.InitGlog(serverConfig.GlogDir, serverConfig.Debug)
	defer glog.Flush()

	//set alarm script path
	alarm.SetScriptPath(serverConfig.AlarmScriptPath)

	// FIU
	fiu.CreateNonFIUEngine(&FiuLog{})
	defer fiu.Close()

	hostName, herr := os.Hostname()
	if herr != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, herr)
	}

	metricsConfig := &metricsserver.MetricsServerConfig{
		MetricsFileDir: viper.GetString("metrics-dir"),
		EndPoint:       hostName + "-" + serverConfig.ServerName,
		VDLName:        serverConfig.ServerName,
	}

	if viper.GetInt("metrics-report-duration") != 0 {
		metricsConfig.ReportDuration = viper.GetInt("metrics-report-duration")
	}
	if viper.GetInt("metrics-log-reserved-hour") != 0 {
		metricsConfig.LogReservedHour = viper.GetInt("metrics-log-reserved-hour")
	}

	vdlServer, err := server.NewVDLServer(serverConfig, metricsConfig)
	if err != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	errStart := vdlServer.Start()
	if errStart != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, err)
	}

	configLogstream := viper.GetString("initial-logstream")
	if len(configLogstream) != 0 {
		logstreams := strings.Split(configLogstream, ",")
		for _, logstream := range logstreams {
			listenerUrls, err := types.NewURLs([]string{viper.GetString(logstream + "-listen-peer-url")})
			if err != nil {
				cmdutil.ExitWithError(cmdutil.ExitError, err)
			}
			initialPeerURLsMap, err := types.NewURLsMap(viper.GetString(logstream + "-initial-cluster"))
			if err != nil {
				cmdutil.ExitWithError(cmdutil.ExitError, err)
			}

			initState := viper.GetString(logstream + "-initial-cluster-state")
			var existsClusterAdminUrls types.URLs
			if initState == "existing" {
				var err error
				existsClusterAdminUrls, err = types.NewURLs([]string{viper.GetString(logstream + "-existing-admin-urls")})
				if err != nil {
					cmdutil.ExitWithError(cmdutil.ExitError, err)
				}
			}

			strictCheck := true
			if len(viper.GetString(logstream+"-strict-member-changes-check")) != 0 {
				strictCheck = viper.GetBool(logstream + "-strict-member-changes-check")
			}

			var maxSizePerAppendEntries uint64 = 0
			if len(viper.GetString(logstream+"-max-size-per-append-entries")) != 0 {
				maxSizePerAppendEntries = uint64(viper.GetInt64(logstream + "-max-size-per-append-entries"))
			}

			var maxInflightAppendEntriesRequest int = 0
			if len(viper.GetString(logstream+"-max-inflight-append-entries-request")) != 0 {
				maxInflightAppendEntriesRequest = viper.GetInt(logstream + "-max-inflight-append-entries-request")
			}

			var reserveSegmentCount int = 0
			if len(viper.GetString(logstream+"-reserve-segment-count")) != 0 {
				reserveSegmentCount = viper.GetInt(logstream + "-reserve-segment-count")
			}

			var memCacheSizeByte uint64 = 0
			if len(viper.GetString(logstream+"-memcache-size")) != 0 {
				memCacheSizeByte = uint64(viper.GetInt64(logstream+"-memcache-size")) * 1024 * 1024
			}

			prevote := false
			if len(viper.GetString(logstream+"-prevote")) != 0 {
				prevote = viper.GetBool(logstream + "-prevote")
			}

			conf := &raftgroup.GroupConfig{
				ListenerUrls:                    listenerUrls,
				GroupName:                       logstream,
				VDLServerName:                   serverConfig.ServerName,
				InitialPeerURLsMap:              initialPeerURLsMap,
				DataDir:                         viper.GetString(logstream + "-log-dir"),
				ElectionTicks:                   serverConfig.ElectionTicks,
				HeartbeatMs:                     serverConfig.HeartbeatMs,
				IsInitialCluster:                initState == "new",
				ExistsClusterAdminUrls:          existsClusterAdminUrls,
				MaxSizePerMsg:                   viper.GetInt(logstream + "-maxsize-per-msg"),
				StrictMemberChangesCheck:        strictCheck,
				MaxSizePerAppendEntries:         maxSizePerAppendEntries,
				MaxInflightAppendEntriesRequest: maxInflightAppendEntriesRequest,
				ReserveSegmentCount:             reserveSegmentCount,
				MemCacheSizeByte:                memCacheSizeByte,
				PreVote:                         prevote,
			}

			if _, err := vdlServer.AddRaftGroup(logstream, conf); err != nil {
				cmdutil.ExitWithError(cmdutil.ExitError, err)
			}
		}
	}

	osutil.RegisterInterruptHandler(vdlServer.Stop)
	osutil.HandleInterrupts()

	errServe := vdlServer.Serve()
	if errServe != nil {
		cmdutil.ExitWithError(cmdutil.ExitError, errServe)
	}
}

func checkConfig(cmd *cobra.Command, args []string) {

	// check base config
	if len(viper.GetString("name")) == 0 {
		cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf("name in config file must provide."))
	}

	if len(viper.GetString("stable-dir")) == 0 {
		cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf("stable-dir in config file must provide."))
	}

	if len(viper.GetString("metrics-dir")) == 0 {
		cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf("metrics-dir in config file must provide."))
	}

	if len(viper.GetString("listen-admin-url")) == 0 {
		cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf("listen-admin-url in config file must provide."))
	}

	if len(viper.GetString("listen-metrics-url")) == 0 {
		cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf("listen-metrics-url in config file must provide."))
	}

	if viper.GetInt64("heartbeat-interval") == 0 {
		cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf("heartbeat-interval in config file must provide and not zero."))
	}

	if viper.GetInt64("election-timeout") == 0 {
		cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf("election-timeout in config file must provide and not zero."))
	}
	if viper.GetInt64("connections-max-idle") == 0 {
		cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf("connections-max-idle in config file must provide and not zero."))
	}

	if viper.GetInt64("heartbeat-interval")*5 > viper.GetInt64("election-timeout") {
		cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf("election-timeout in config file should be 5 times bigger than heartbeat-interval"))
	}

	configLogstream := viper.GetString("initial-logstream")
	if len(configLogstream) != 0 {
		logstreams := strings.Split(configLogstream, ",")
		for _, logstream := range logstreams {
			if !logstreampkg.IsLogStreamNameValid(logstream) {
				cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf("logstream name invalid: ["+logstream+"]"))
			}
			if len(viper.GetString(logstream+"-initial-cluster")) == 0 {
				cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf(logstream+"-initial-cluster in config file must provide."))
			}
			if len(viper.GetString(logstream+"-initial-cluster-state")) == 0 {
				cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf(logstream+"-initial-cluster-state in config file must provide."))
			}
			if len(viper.GetString(logstream+"-listen-peer-url")) == 0 {
				cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf(logstream+"-listen-peer-url in config file must provide."))
			}
			if len(viper.GetString(logstream+"-log-dir")) == 0 {
				cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf(logstream+"-log-dir in config file must provide."))
			}

			//check logstream1-maxsize-per-msg is wrong
			if len(viper.GetString(logstream+"-maxsize-per-msg")) > 0 {
				if viper.GetInt(logstream+"-maxsize-per-msg") == 0 {
					cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf(logstream+"-maxsize-per-msg in config file must integer."))
				}
			}

			initState := viper.GetString(logstream + "-initial-cluster-state")
			if len(initState) == 0 {
				cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf(logstream+"-initial-cluster-state in config file must provide."))
			} else {
				if initState != "new" && initState != "existing" {
					cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf(logstream+"-initial-cluster-state in config file wrong"))
				}

				if initState == "existing" {
					if len(viper.GetString(logstream+"-existing-admin-urls")) == 0 {
						cmdutil.ExitWithError(cmdutil.ExitBadConfig, fmt.Errorf(logstream+"-existing-admin-urls in config file must provide."))
					}
				}
			}
		}
	}
}

type FiuLog struct {
}

func (l *FiuLog) Fatalf(format string, args ...interface{}) {
	glog.Fatalf(format, args...)

}
func (l *FiuLog) Infof(format string, args ...interface{}) {
	glog.Infof(format, args...)

}
func (l *FiuLog) Errorf(format string, args ...interface{}) {
	glog.Errorf(format, args...)
}

func (l *FiuLog) Debugf(format string, args ...interface{}) {
	glog.Infof(format, args...)
}

func (l *FiuLog) IsDebug() bool {
	return bool(glog.V(1))
}
