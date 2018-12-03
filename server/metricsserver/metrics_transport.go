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
)

func (ms *MetricsServer) writeTransportMetrics(timestamp int64) {
	// do noting now
}

func (ms *MetricsServer) doWriteTransportMetrics(namespace string, timestamp int64, metric prometheus.Metric,
	metricEndPoint string, callback metricsValueCalculateFunc) {

	ms.doWriteMetrics(namespace, timestamp, metric, callback, "transport", metricEndPoint)
}

func (ms *MetricsServer) doWriteTransportMetricsWithDTO(namespace string, timestamp int64, metricDTO *io_prometheus_client.Metric,
	metricEndPoint string, callback metricsValueCalculateFunc) {

	ms.doWriteMetricsWithDTO(namespace, timestamp, metricDTO, callback, "transport", metricEndPoint)
}
