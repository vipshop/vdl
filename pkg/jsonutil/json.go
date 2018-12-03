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

package jsonutil

import (
	"encoding/json"

	"github.com/vipshop/vdl/pkg/glog"
)

//var plog = logutil.NewMergeLogger(capnslog.NewPackageLogger(conf.LOG_ROOT, "json"))

func MustMarshal(v interface{}) []byte {

	value, err := json.Marshal(v)
	if err != nil {
		glog.Fatalln("json Marshal should not be fail", err)
	}
	return value

}

func MustUnMarshal(b []byte, v interface{}) {
	err := json.Unmarshal(b, v)
	if err != nil {
		glog.Fatalln("json UnMarshal should not be fail", err)
	}
}
