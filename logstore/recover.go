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
	"io/ioutil"
	"sync"

	"path"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/vipshop/vdl/pkg/glog"
)

var (
	exitFlagFileName string = "exit_flag_file"
	NormalExitFlag   string = "normal_exit"
	UnnormalExitFlag string = "unnormal_exit"
)

type FlagFile struct {
	filePath string
	l        sync.Mutex
}

func NewFlagFile(dir string) *FlagFile {
	if len(dir) == 0 {
		glog.Fatalf("[recover.go-NewFlagFile]:dir is nil in NewFlagFile")
	}
	f := &FlagFile{
		filePath: path.Join(dir, exitFlagFileName),
	}
	return f
}

//判断是否需要恢复index 文件，以下情形需要恢复:
//1.exit_flag_file文件不存在
//2.exit_flag_file文件中的内容不是normal_exit
func (f *FlagFile) NeedRecover() bool {
	f.l.Lock()
	defer f.l.Unlock()

	if IsFileExist(f.filePath) {
		data, err := ioutil.ReadFile(f.filePath)
		if err != nil {
			glog.Fatalf("[recover.go-NeedRecover]:ReadFile error,err=%s,filePath=%s", err.Error(), f.filePath)
		}
		if string(data) == NormalExitFlag {
			return false
		}
	}
	return true
}

//如果文件不存在，创建文件并写入标识
func (f *FlagFile) WriteExitFlag(flag string) {
	f.l.Lock()
	defer f.l.Unlock()
	var err error
	if len(flag) == 0 {
		return
	}
	//文件不存在，WriteFile会创建
	err = ioutil.WriteFile(f.filePath, []byte(flag), fileutil.PrivateFileMode)
	if err != nil {
		glog.Fatalf("[recover.go-WriteExitFlag]:WriteFile error,err=%s,filePath=%s,flag=%s",
			err.Error(), f.filePath, flag)
	}
}
