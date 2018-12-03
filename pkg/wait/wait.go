// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wait

import (
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Wait is an interface that provides the ability to wait and trigger events that
// are associated with IDs.
type Wait interface {
	// Register waits returns a chan that waits on the given ID.
	// The chan will be triggered when Trigger is called with
	// the same ID.
	Register(id uint64) <-chan interface{}
	// Trigger triggers the waiting chans with the given ID.
	Trigger(id uint64, x interface{})
	IsRegistered(id uint64) bool
}

type list struct {
	l               sync.Mutex
	m               map[uint64]chan interface{}
	t               map[uint64]time.Time
	needStats       bool
	summaryVec      *prometheus.SummaryVec
	summaryVecLabel string
}

// New creates a Wait.
func New() Wait {
	return &list{
		m:         make(map[uint64]chan interface{}),
		needStats: false,
	}
}

// New creates a Wait.
func NewWithStats(summaryVec *prometheus.SummaryVec, summaryVecLabel string) Wait {
	return &list{
		m:               make(map[uint64]chan interface{}),
		t:               make(map[uint64]time.Time),
		needStats:       true,
		summaryVec:      summaryVec,
		summaryVecLabel: summaryVecLabel,
	}
}

func (w *list) Register(id uint64) <-chan interface{} {
	w.l.Lock()
	defer w.l.Unlock()
	ch := w.m[id]
	if ch == nil {
		ch = make(chan interface{}, 1)
		w.m[id] = ch
		if w.needStats {
			w.t[id] = time.Now()
		}
	} else {
		log.Panicf("dup id %x", id)
	}
	return ch
}

func (w *list) Trigger(id uint64, x interface{}) {
	var beginTime time.Time
	w.l.Lock()
	ch := w.m[id]
	delete(w.m, id)
	if w.needStats {
		beginTime = w.t[id]
		delete(w.t, id)
	}
	w.l.Unlock()

	if w.needStats {
		w.summaryVec.WithLabelValues(w.summaryVecLabel).Observe(float64(time.Now().Sub(beginTime) / time.Millisecond))
	}
	if ch != nil {
		ch <- x
		close(ch)
	}
}

func (w *list) IsRegistered(id uint64) bool {
	w.l.Lock()
	defer w.l.Unlock()
	_, ok := w.m[id]
	return ok
}

type waitWithResponse struct {
	ch <-chan interface{}
}

func NewWithResponse(ch <-chan interface{}) Wait {
	return &waitWithResponse{ch: ch}
}

func (w *waitWithResponse) Register(id uint64) <-chan interface{} {
	return w.ch
}
func (w *waitWithResponse) Trigger(id uint64, x interface{}) {}
func (w *waitWithResponse) IsRegistered(id uint64) bool {
	panic("waitWithResponse.IsRegistered() shouldn't be called")
}
