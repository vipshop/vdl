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

package raftgroupmetrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vipshop/vdl/conf"
)

var (

	// need change both
	MetricsRaftRoleSwitchName = "raft_role_switch_count"
	MetricsRaftRoleSwitch     = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Subsystem: "role",
		Name:      "switch_count",
		Help:      "The Leader/Follower switch count",
	},
		[]string{"RaftGroupName"},
	)

	MetricsRaftApplyIndexName = "raft_apply_index"
	MetricsRaftApplyIndex     = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "apply",
		Name:      "index",
		Help:      "The raft apply index",
	},
		[]string{"RaftGroupName"},
	)

	MetricsRaftLatency = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "raft",
		Subsystem: "entry",
		Name:      "latency",
		Help:      "Raft Latency",
		MaxAge:    conf.DefaultMetricsConfig.RaftGroupEntryLatencySummaryDuration,
	},
		[]string{"RaftGroupName"},
	)
)

func init() {
	prometheus.MustRegister(MetricsRaftRoleSwitch)
	prometheus.MustRegister(MetricsRaftLatency)
	prometheus.MustRegister(MetricsRaftApplyIndex)
}
