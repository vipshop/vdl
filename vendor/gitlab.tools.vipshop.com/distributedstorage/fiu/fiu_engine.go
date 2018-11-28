/*
 * Copyright (c) 2017, Vipshop, All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of drsd nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package fiu

import (
	"encoding/binary"
	"os"
	"syscall"
)

const (
	defaultFIUShmName = "/fiu.shm.keyword"

	shmSize = 1 << 20
)

type Logger interface {
	Fatalf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	IsDebug() bool
}

type FIUEngine struct {

	// execute FIU only when isTestMode = true
	isTestMode bool

	logger Logger

	fiuShmName string

	fiuShmFile *os.File

	fiuShmData []byte
}

var defaultFIUEngine *FIUEngine = nil

// for production or non-test mode
func CreateNonFIUEngine(logger Logger) {

	if defaultFIUEngine != nil {
		logger.Fatalf("[fiu_engine.go-CreateNonFIUEngine]: FIU Engine already create, you cannot create again")
	}

	defaultFIUEngine = &FIUEngine{
		isTestMode: false,
		logger:     logger,
	}
}

func CreateFIUEngine(fiuShmName string, logger Logger) {

	if defaultFIUEngine != nil {
		logger.Fatalf("[fiu_engine.go-CreateFIUEngine]: FIU Engine already create, you cannot create again")
	}

	if fiuShmName == "" {
		fiuShmName = defaultFIUShmName
	}

	fiuShmFile, err := OpenShm(fiuShmName, os.O_RDWR, 0666)
	if err != nil {
		logger.Fatalf("[fiu_engine.go-CreateFIUEngine]: open %s file error: %v", fiuShmName, err)
	}

	data, err := syscall.Mmap(int(fiuShmFile.Fd()), 0, shmSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		logger.Fatalf("[fiu_engine.go-CreateFIUEngine]: Mmap %s file error: %v", fiuShmName, err)
	}

	defaultFIUEngine = &FIUEngine{
		fiuShmName: fiuShmName,
		isTestMode: true,
		logger:     logger,
		fiuShmFile: fiuShmFile,
		fiuShmData: data,
	}

}

func IsSyncPointExist(syncPointName string) bool {

	checkInit()

	if !defaultFIUEngine.isTestMode {
		return false
	}

	reqSize := int(binary.LittleEndian.Uint32(defaultFIUEngine.fiuShmData[0:4]))
	if defaultFIUEngine.logger.IsDebug() {
		defaultFIUEngine.logger.Debugf("[fiu_engine.go-IsSyncPointExist]: Get size from shm file :%d", reqSize)
	}

	for i := 0; i < reqSize; i++ {
		start := shmHeaderSize + i*reqMsgSize + keywordOffset
		end := start + keywordSize
		keyword := byteString(defaultFIUEngine.fiuShmData[start:end])
		if defaultFIUEngine.logger.IsDebug() {
			defaultFIUEngine.logger.Debugf("[fiu_engine.go-IsSyncPointExist]: Get keyword from shm file :%s", keyword)
		}
		if keyword == syncPointName {
			if defaultFIUEngine.logger.IsDebug() {
				defaultFIUEngine.logger.Debugf("[fiu_engine.go-IsSyncPointExist]: IsSyncPointExist %s true", syncPointName)
			}
			return true
		}
	}
	if defaultFIUEngine.logger.IsDebug() {
		defaultFIUEngine.logger.Debugf("[fiu_engine.go-IsSyncPointExist]: IsSyncPointExist %s false", syncPointName)
	}

	return false
}

func Close() {

	if defaultFIUEngine == nil {
		return
	}

	if defaultFIUEngine.isTestMode {
		err := syscall.Munmap(defaultFIUEngine.fiuShmData)
		if err != nil {
			defaultFIUEngine.logger.Errorf("[fiu_engine.go-Close]:Munmap error, fileName: %s, err: %v",
				defaultFIUEngine.fiuShmFile, err)
		}

		err = defaultFIUEngine.fiuShmFile.Close()
		if err != nil {
			defaultFIUEngine.logger.Errorf("[fiu_engine.go-Close]:Close file error, fileName: %s, err: %v",
				defaultFIUEngine.fiuShmFile, err)
		}
	}

	defaultFIUEngine = nil
}

func checkInit() {

	if defaultFIUEngine == nil {
		panic("[fiu_engine.go-checkInit]: should call Create FIU Engine first")
	}
}

func byteString(p []byte) string {
	for i := 0; i < len(p); i++ {
		if p[i] == 0 {
			return string(p[0:i])
		}
	}
	return string(p)
}
