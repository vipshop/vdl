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
	"github.com/vipshop/vdl/logstore"
)

func (ms *MetricsServer) writeStoreMetrics(timestamp int64) {
	//logstore_read_tps
	ms.doWriteLogstoreMetrics("logstore_read_tps", timestamp, logstore.LogstoreReadTps,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := org.calculateTps(org.lastReport.LogstoreReadCount, *metricDTO.Counter.Value)
			org.lastReport.LogstoreReadCount = *metricDTO.Counter.Value
			return result
		})
	//logstore_read_latency
	ms.doWriteLogstoreMetrics("logstore_read_latency", timestamp, logstore.LogstoreReadLatency,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			if len(metricDTO.Summary.Quantile) > 0 {
				if math.IsNaN(*metricDTO.Summary.Quantile[0].Value) {
					return 0
				}
				return *metricDTO.Summary.Quantile[0].Value
			}
			return 0
		})

	//logstore_write_tps
	ms.doWriteLogstoreMetrics("logstore_write_tps", timestamp, logstore.LogstoreWriteTps,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := org.calculateTps(org.lastReport.LogstoreWriteCount, *metricDTO.Counter.Value)
			org.lastReport.LogstoreWriteCount = *metricDTO.Counter.Value
			return result
		})
	//logstore_write_latency
	ms.doWriteLogstoreMetrics("logstore_write_latency", timestamp, logstore.LogstoreWriteLatency,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			if len(metricDTO.Summary.Quantile) > 0 {
				if math.IsNaN(*metricDTO.Summary.Quantile[0].Value) {
					return 0
				}
				return *metricDTO.Summary.Quantile[0].Value
			}
			return 0
		})
	//logstore_delete_segment_latency
	ms.doWriteLogstoreMetrics("logstore_delete_segment_latency", timestamp, logstore.LogstoreDeleteSegmentLatency,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			if len(metricDTO.Summary.Quantile) > 0 {
				if math.IsNaN(*metricDTO.Summary.Quantile[0].Value) {
					return 0
				}
				return *metricDTO.Summary.Quantile[0].Value
			}
			return 0
		})

	//segment_read_tps
	ms.doWriteLogstoreMetrics("segment_read_tps", timestamp, logstore.SegmentReadTps,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			result := org.calculateTps(org.lastReport.SegmentReadCount, *metricDTO.Counter.Value)
			org.lastReport.SegmentReadCount = *metricDTO.Counter.Value
			return result
		})
	//segment_cut
	ms.doWriteLogstoreMetrics("segment_cut", timestamp, logstore.SegmentCutCounter,
		func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64 {
			//捕获segment cut的条件：上一次统计为0，本次统计为1
			//if int(org.lastReport.SegmentCutCount) == 0 && int(*metricDTO.Counter.Value) == 1 {
			//	org.lastReport.SegmentCutTotalCount += *metricDTO.Counter.Value
			//	org.lastReport.SegmentCutCount = *metricDTO.Counter.Value
			//}
			//if int(*metricDTO.Counter.Value) == 0 {
			//	org.lastReport.SegmentCutCount = 0
			//}
			//return org.lastReport.SegmentCutTotalCount
			if int(*metricDTO.Counter.Value) == 0 {
				return 0
			}
			return *metricDTO.Counter.Value
		})
}

func (ms *MetricsServer) doWriteLogstoreMetrics(namespace string, timestamp int64, metric prometheus.Metric,
	callback metricsValueCalculateFunc) {
	ms.doWriteMetrics(namespace, timestamp, metric, callback, "counter", "logstore")
}
