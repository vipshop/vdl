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

package stablestore

import "sync"

/**
InmemStore use to mock the storage, using memory. DO NOT use to production
*/

// InmemStore implements the LogStore and StableStore interface.
// It should NOT EVER be used for production. It is used only for
// unit tests. Use the MDBStore implementation instead.
type InmemStableStore struct {
	l  sync.RWMutex
	kv map[string][]byte
}

// NewInmemStore returns a new in-memory backend. Do not ever
// use for production. Only for testing.
func NewInmemStableStore() *InmemStableStore {
	i := &InmemStableStore{
		kv: make(map[string][]byte),
	}
	return i
}

func (i *InmemStableStore) Put(key []byte, val []byte) error {

	i.l.Lock()
	defer i.l.Unlock()

	i.kv[string(key)] = val

	return nil
}

func (i *InmemStableStore) Get(key []byte) ([]byte, error) {

	i.l.Lock()
	defer i.l.Unlock()
	return i.kv[string(key)], nil
}

func (i *InmemStableStore) Close() error {
	i.kv = make(map[string][]byte)
	return nil
}

func (i *InmemStableStore) GetBatch(keys []string) (map[string][]byte, error) {
	i.l.Lock()
	defer i.l.Unlock()

	returnMap := make(map[string][]byte)

	for _, key := range keys {
		returnMap[key] = i.kv[string(key)]
	}

	return returnMap, nil

}
