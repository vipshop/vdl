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

package kafkametrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vipshop/vdl/conf"
)

var (
	MetricsKsWriteReqTps = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ks",
		Subsystem: "write",
		Name:      "req_tps",
		Help:      "kafka server received write request count(1 for each batch)",
	})

	MetricsKsWriteRespTps = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ks",
		Subsystem: "write",
		Name:      "resp_tps",
		Help:      "kafka server response write request count",
	})

	MetricsKsWriteReqLogTps = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ks",
		Subsystem: "write",
		Name:      "req_log_tps",
		Help:      "kafka server received write log count(per log)",
	})

	MetricsKsWriteReqMsgErrMaxSize = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ks",
		Subsystem: "write",
		Name:      "req_msg_err_maxsize",
		Help:      "kafka server received msgs which exceed max size",
	})

	MetricsKsWriteReqMsgErrOther = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ks",
		Subsystem: "write",
		Name:      "req_msg_err_other",
		Help:      "kafka server received msgs which have error(exclude exceed max size error)",
	})

	MetricsKsWriteReqSrvErrNoLeader = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ks",
		Subsystem: "write",
		Name:      "req_srv_err_no_leader",
		Help:      "the req count for no leader kafka server received (1 for each batch)",
	})

	MetricsKsWriteLatency = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "ks",
		Subsystem:  "write",
		Name:       "write_latency",
		Help:       "kafka server write latency",
		MaxAge:     conf.DefaultMetricsConfig.KafkaWriteLatencySummaryDuration,
		Objectives: map[float64]float64{0.99: 0.001},
	})

	MetricsKsReadLogTps = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ks",
		Subsystem: "read",
		Name:      "log_tps",
		Help:      "kafka server consume tps",
	})

	MetricsKsReadLatency = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "ks",
		Subsystem:  "read",
		Name:       "read_latency",
		Help:       "kafka server write latency",
		MaxAge:     conf.DefaultMetricsConfig.KafkaReadLatencySummaryDuration,
		Objectives: map[float64]float64{0.99: 0.001},
	})

	MetricsKsConnectTimes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ks",
		Subsystem: "conn",
		Name:      "connect_times",
		Help:      "kafka server accept client connect times",
	})

	MetricsKsOnlineConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "ks",
		Subsystem: "conn",
		Name:      "online_connections",
		Help:      "kafka server online client connects",
	})
)

func init() {
	prometheus.MustRegister(MetricsKsWriteReqTps)
	prometheus.MustRegister(MetricsKsWriteRespTps)
	prometheus.MustRegister(MetricsKsWriteReqLogTps)
	prometheus.MustRegister(MetricsKsWriteReqMsgErrMaxSize)
	prometheus.MustRegister(MetricsKsWriteReqMsgErrOther)
	prometheus.MustRegister(MetricsKsWriteReqSrvErrNoLeader)
	prometheus.MustRegister(MetricsKsWriteLatency)
	prometheus.MustRegister(MetricsKsReadLogTps)
	prometheus.MustRegister(MetricsKsReadLatency)
	prometheus.MustRegister(MetricsKsConnectTimes)
	prometheus.MustRegister(MetricsKsOnlineConnections)
}
