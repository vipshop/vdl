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

package logstore

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/vipshop/vdl/conf"
)

var (
	//logstore interface metric
	LogstoreReadTps = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "logstore",
		Subsystem: "read",
		Name:      "read_tps",
		Help:      "logstore read tps",
	})
	LogstoreReadLatency = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "logstore",
		Subsystem:  "read",
		Name:       "read_latency",
		Help:       "logstore read latency",
		MaxAge:     conf.DefaultMetricsConfig.LogstoreReadLatencySummaryDuration,
		Objectives: map[float64]float64{0.99: 0.001},
	})

	LogstoreWriteTps = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "logstore",
		Subsystem: "write",
		Name:      "write_tps",
		Help:      "logstore write tps",
	})
	LogstoreWriteLatency = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "logstore",
		Subsystem:  "write",
		Name:       "write_latency",
		Help:       "logstore write latency",
		MaxAge:     conf.DefaultMetricsConfig.LogstoreWriteLatencySummaryDuration,
		Objectives: map[float64]float64{0.99: 0.001},
	})

	LogstoreDeleteSegmentLatency = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "logstore",
		Subsystem:  "delete",
		Name:       "delete_segment_latency",
		Help:       "logstore delete segment latency(ms)",
		MaxAge:     conf.DefaultMetricsConfig.LogstoreDSLatencySummaryDuration,
		Objectives: map[float64]float64{0.99: 0.001},
	})

	//segment metric
	SegmentReadTps = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "logstore",
		Subsystem: "segment",
		Name:      "read_tps",
		Help:      "segment read tps",
	})

	SegmentCutCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "logstore",
		Subsystem: "segment",
		Name:      "segment_cut_Interval",
		Help:      "The count of segment cut",
	})
)

func init() {
	//logstore interface metric
	prometheus.MustRegister(LogstoreReadTps)
	prometheus.MustRegister(LogstoreReadLatency)

	prometheus.MustRegister(LogstoreWriteTps)
	prometheus.MustRegister(LogstoreWriteLatency)
	prometheus.MustRegister(LogstoreDeleteSegmentLatency)
	//segment metric
	prometheus.MustRegister(SegmentReadTps)
	prometheus.MustRegister(SegmentCutCounter)
}
