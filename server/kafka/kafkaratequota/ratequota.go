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

package kafkaratequota

import (
	"github.com/vipshop/vdl/server/runtimeconfig"
	"golang.org/x/time/rate"
	"sync"
)

type KafkaRate struct {
	IsEnableRateQuota         bool
	WriteRequestLimiter       *rate.Limiter
	WriteByteLimiter          *rate.Limiter
	CatchupReadRequestLimiter *rate.Limiter
	CatchupReadByteLimiter    *rate.Limiter
	EndReadRequestLimiter     *rate.Limiter
	EndReadByteRateLimiter    *rate.Limiter
}

var (
	defaultKafkaRate *KafkaRate = nil
	kafkaRWLock      sync.RWMutex
)

func GetKafkaRate() *KafkaRate {
	kafkaRWLock.RLock()
	defer kafkaRWLock.RUnlock()
	return defaultKafkaRate
}

func SetKafkaRateByConfig(config *runtimeconfig.RuntimeRateConfig) {
	kafkaRWLock.Lock()
	defer kafkaRWLock.Unlock()

	defaultKafkaRate = &KafkaRate{
		IsEnableRateQuota:         config.IsEnableRateQuota,
		WriteRequestLimiter:       rate.NewLimiter(rate.Limit(config.WriteRequestRate), config.WriteRequestRate),
		WriteByteLimiter:          rate.NewLimiter(rate.Limit(config.WriteByteRate), config.WriteByteRate),
		CatchupReadRequestLimiter: rate.NewLimiter(rate.Limit(config.CatchupReadRequestRate), config.CatchupReadRequestRate),
		CatchupReadByteLimiter:    rate.NewLimiter(rate.Limit(config.CatchupReadByteRate), config.CatchupReadByteRate),
		EndReadByteRateLimiter:    rate.NewLimiter(rate.Limit(config.EndReadByteRate), config.EndReadByteRate),
		EndReadRequestLimiter:     rate.NewLimiter(rate.Limit(config.EndReadRequestRate), config.EndReadRequestRate),
	}
}
