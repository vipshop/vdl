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

import (
	"encoding/json"
	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/stablestore"
	"sync"
)

var (
	runtimeConfigKey = []byte("runtime_config_Key")
)

type RuntimeConfigStore struct {
	stableStore stablestore.StableStore

	// guards the db
	sync.Mutex
}

func NewRuntimeConfigStore(stableStore stablestore.StableStore) *RuntimeConfigStore {

	return &RuntimeConfigStore{
		stableStore: stableStore,
	}
}

func (r *RuntimeConfigStore) GetOrCreateRuntimeConfig() *RuntimeConfig {
	r.Lock()
	defer r.Unlock()

	return r.doGetOrCreateRuntimeConfig(true)
}

func (r *RuntimeConfigStore) UpdateRuntimeConfig(update *RuntimeConfig) {
	r.Lock()
	defer r.Unlock()

	b, err := json.Marshal(update)
	if err != nil {
		glog.Fatalf("marshal RuntimeConfig should never fail: %v", err)
	}
	err = r.stableStore.Put(runtimeConfigKey, b)
	if err != nil {
		glog.Fatalf("store RuntimeConfig should never fail: %v", err)
	}

}

func (r *RuntimeConfigStore) doGetOrCreateRuntimeConfig(create bool) *RuntimeConfig {

	value, err := r.stableStore.Get(runtimeConfigKey)
	if err != nil {
		glog.Fatalf("get RuntimeConfig from store should never fail: %v", err)
	}
	var runtimeConfig *RuntimeConfig = nil

	if value == nil || len(value) == 0 {
		if create {
			runtimeConfig = createDefaultRuntimeConfig()
			b, err := json.Marshal(runtimeConfig)
			if err != nil {
				glog.Fatalf("marshal RuntimeConfig should never fail: %v", err)
			}
			err = r.stableStore.Put(runtimeConfigKey, b)
			if err != nil {
				glog.Fatalf("store RuntimeConfig should never fail: %v", err)
			}
		}
	} else {
		runtimeConfig = &RuntimeConfig{}
		if err = json.Unmarshal(value, runtimeConfig); err != nil {
			glog.Fatalf("Unmarshal RuntimeConfig from store should never fail: %v", err)
		}
	}
	return runtimeConfig
}
