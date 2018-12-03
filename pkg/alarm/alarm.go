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

package alarm

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"

	"time"

	"github.com/vipshop/vdl/pkg/glog"
)

const (
	RetryTimes = 3
)

type AlarmResponse struct {
	Success bool   `json:"success"`
	Code    int    `json:"code"`
	Object  string `json:"object"`
	Message string `json:"message"`
}

var scriptPath string

func SetScriptPath(p string) {
	scriptPath = p
}

func Alarm(message string) {
	var try int
	var resp AlarmResponse

	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		glog.Errorf("alarm script not exist,scriptPath=%s,message=%s", scriptPath, message)
		return
	}
	if len(message) == 0 {
		glog.Errorf("alarm message is null,ignore")
		return
	}
	for try < RetryTimes {
		cmd := exec.Command("bash", scriptPath, message)
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			glog.Errorf("fail to exec alarm script,scriptPath=%s,err=%s,message=%s", scriptPath, err.Error(), message)
			return
		}
		//if unmarshal  retry 3 times
		err = json.Unmarshal(out.Bytes(), &resp)
		if err != nil || resp.Success == false {
			try++
			time.Sleep(time.Second * time.Duration(try*10))
			glog.Infof("alarm response is %s,retry send alarm[%s],after %d second,scriptPath=%s",
				out.String(), message, try*10, scriptPath)
			continue
		} else {
			glog.Infof("send alarm[%s] success,scriptPath=%s,message=%s", scriptPath, message)
			return
		}
	}
	glog.Errorf("send alarm[%s] error, and try max times[%d],scriptPath=%s", message, try, scriptPath)
}
