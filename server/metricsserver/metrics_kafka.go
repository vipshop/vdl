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
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_model/go"
	"github.com/vipshop/vdl/server/kafka/kafkametrics"
)

func (ms *MetricsServer) writeKafkaMetrics(timestamp int64) {

	// ks_write_req_tps
	ms.doWriteKafkaMetrics("ks_write_req_tps", timestamp, kafkametrics.MetricsKsWriteReqTps,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := org.calculateTps(org.lastReport.ksWriteReqTps, *metricDTO.Counter.Value)
			org.lastReport.ksWriteReqTps = *metricDTO.Counter.Value
			return result
		})

	// ks_write_resp_tps
	ms.doWriteKafkaMetrics("ks_write_resp_tps", timestamp, kafkametrics.MetricsKsWriteRespTps,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := org.calculateTps(org.lastReport.KsWriteRespTps, *metricDTO.Counter.Value)
			org.lastReport.KsWriteRespTps = *metricDTO.Counter.Value
			return result
		})

	// ks_write_req_log_tps
	ms.doWriteKafkaMetrics("ks_write_req_log_tps", timestamp, kafkametrics.MetricsKsWriteReqLogTps,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := org.calculateTps(org.lastReport.KsWriteReqLogTps, *metricDTO.Counter.Value)
			org.lastReport.KsWriteReqLogTps = *metricDTO.Counter.Value
			return result
		})

	// ks_write_req_msg_err_maxsize
	ms.doWriteKafkaMetrics("ks_write_req_msg_err_maxsize", timestamp, kafkametrics.MetricsKsWriteReqMsgErrMaxSize,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := *metricDTO.Counter.Value - org.lastReport.KsWriteReqMsgErrMaxSize
			org.lastReport.KsWriteReqMsgErrMaxSize = *metricDTO.Counter.Value
			return result
		})

	// ks_write_req_msg_err_other
	ms.doWriteKafkaMetrics("ks_write_req_msg_err_other", timestamp, kafkametrics.MetricsKsWriteReqMsgErrOther,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := *metricDTO.Counter.Value - org.lastReport.KsWriteReqMsgErrOther
			org.lastReport.KsWriteReqMsgErrOther = *metricDTO.Counter.Value
			return result
		})

	// ks_write_req_srv_err_noleader
	ms.doWriteKafkaMetrics("ks_write_req_srv_err_noleader", timestamp, kafkametrics.MetricsKsWriteReqSrvErrNoLeader,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := *metricDTO.Counter.Value - org.lastReport.KsWriteReqSrvErrNoLeader
			org.lastReport.KsWriteReqSrvErrNoLeader = *metricDTO.Counter.Value
			return result
		})

	// ks_read_log_tps
	ms.doWriteKafkaMetrics("ks_read_log_tps", timestamp, kafkametrics.MetricsKsReadLogTps,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := org.calculateTps(org.lastReport.KsReadLogTps, *metricDTO.Counter.Value)
			org.lastReport.KsReadLogTps = *metricDTO.Counter.Value
			return result
		})

	// ks_conn_count
	ms.doWriteKafkaMetrics("ks_conn_count", timestamp, kafkametrics.MetricsKsConnectTimes,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := *metricDTO.Counter.Value - org.lastReport.KsConnCount
			org.lastReport.KsConnCount = *metricDTO.Counter.Value
			return result
		})

	// ks_conn_online_connection
	ms.doWriteKafkaMetrics("ks_conn_online_connection", timestamp, kafkametrics.MetricsKsOnlineConnections,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			return *metricDTO.Gauge.Value
		})

	// ks_write_latency
	ms.doWriteKafkaMetrics("ks_write_latency", timestamp, kafkametrics.MetricsKsWriteLatency,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			if len(metricDTO.Summary.Quantile) > 0 {
				if math.IsNaN(*metricDTO.Summary.Quantile[0].Value) {
					return 0
				}
				return *metricDTO.Summary.Quantile[0].Value
			}
			return 0
		})

	// ks_read_latency
	ms.doWriteKafkaMetrics("ks_read_latency", timestamp, kafkametrics.MetricsKsReadLatency,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			if len(metricDTO.Summary.Quantile) > 0 {
				if math.IsNaN(*metricDTO.Summary.Quantile[0].Value) {
					return 0
				}
				return *metricDTO.Summary.Quantile[0].Value
			}
			return 0
		})
}

func (ms *MetricsServer) doWriteKafkaMetrics(namespace string, timestamp int64, metric prometheus.Metric,
	callback metricsValueCalculateFunc) {

	ms.doWriteMetrics(namespace, timestamp, metric, callback, "counter", "kafka")
}
