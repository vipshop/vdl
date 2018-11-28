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

package metricsserver

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_model/go"
	"github.com/vipshop/vdl/consensus/raftgroup/raftgroupmetrics"
)

func (ms *MetricsServer) writeRaftMetrics(timestamp int64) {

	gather, _ := prometheus.DefaultGatherer.Gather()
	for i := 0; i < len(gather); i++ {

		// raft_role_switch_count
		if *gather[i].Name == raftgroupmetrics.MetricsRaftRoleSwitchName {
			// for each raft group,
			for j := 0; j < len(gather[i].Metric); j++ {
				metric := gather[i].Metric[j]
				raftgroupName := *metric.Label[0].Value
				ms.doWriteRaftMetricsWithDTO("raft_role_switch_count", timestamp, metric, raftgroupName,
					func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
						return *metricDTO.Counter.Value
					})
			}
		}

		// raft_apply_index
		if *gather[i].Name == raftgroupmetrics.MetricsRaftApplyIndexName {
			// for each raft group,
			for j := 0; j < len(gather[i].Metric); j++ {
				metric := gather[i].Metric[j]
				raftgroupName := *metric.Label[0].Value
				ms.doWriteRaftMetricsWithDTO("raft_apply_index", timestamp, metric, raftgroupName,
					func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
						return *metricDTO.Gauge.Value
					})
			}
		}

	}
}

func (ms *MetricsServer) doWriteRaftMetrics(namespace string, timestamp int64, metric prometheus.Metric,
	metricEndPoint string, callback metricsValueCalculateFunc) {

	ms.doWriteMetrics(namespace, timestamp, metric, callback, "counter", metricEndPoint)
}

func (ms *MetricsServer) doWriteRaftMetricsWithDTO(namespace string, timestamp int64, metricDTO *io_prometheus_client.Metric,
	metricEndPoint string, callback metricsValueCalculateFunc) {

	ms.doWriteMetricsWithDTO(namespace, timestamp, metricDTO, callback, "counter", metricEndPoint)
}
