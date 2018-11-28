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

package conf

import "time"

// put all the metrics config here,
// later can consider which need use whether dynamic mode
type MetricsConfig struct {

	// Duration for entry latency summary
	RaftGroupEntryLatencySummaryDuration time.Duration

	// ============ Kafka =============//
	// Write Latency
	KafkaWriteLatencySummaryDuration time.Duration

	// Read Latency
	KafkaReadLatencySummaryDuration time.Duration

	//=============Logstore===========//
	// Logstore Write Latency
	LogstoreWriteLatencySummaryDuration time.Duration

	// Logstore Read Latency
	LogstoreReadLatencySummaryDuration time.Duration
	// Logstore Delete segment latency
	LogstoreDSLatencySummaryDuration time.Duration
}

var (
	DefaultMetricsConfig = &MetricsConfig{
		RaftGroupEntryLatencySummaryDuration: 2 * time.Second,
		KafkaWriteLatencySummaryDuration:     2 * time.Second,
		KafkaReadLatencySummaryDuration:      2 * time.Second,
		LogstoreWriteLatencySummaryDuration:  2 * time.Second,
		LogstoreReadLatencySummaryDuration:   2 * time.Second,
		LogstoreDSLatencySummaryDuration:     30 * time.Second,
	}
)
