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

package runtimeconfig

type RuntimeConfig struct {
	RuntimeRateConfig
}

type RuntimeRateConfig struct {

	//************ Rate Quota ************//

	// enable or disable rate quota
	IsEnableRateQuota bool

	// per second
	WriteRequestRate int

	// per second
	WriteByteRate int

	// means date read from hard disk, per second
	CatchupReadRequestRate int

	// means date read from hard disk, per second
	CatchupReadByteRate int

	// means date read from memory, per second
	EndReadRequestRate int

	// means date read from memory, per second
	EndReadByteRate int
}

func createDefaultRuntimeConfig() *RuntimeConfig {

	return &RuntimeConfig{
		RuntimeRateConfig{
			IsEnableRateQuota:      false,
			CatchupReadRequestRate: 100000,
			EndReadRequestRate:     100000,
			WriteRequestRate:       100000,
			WriteByteRate:          500 * 1024 * 1024,
			CatchupReadByteRate:    500 * 1024 * 1024,
			EndReadByteRate:        500 * 1024 * 1024,
		},
	}
}
