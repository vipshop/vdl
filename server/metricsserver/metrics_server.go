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

package metricsserver

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_model/go"
	"github.com/vipshop/vdl/pkg/glog"
)

const (
	logMaxSize        int64 = 1024 * 1024 * 100
	logFileNamePrefix       = "vdl_metrics"
	logFileNameExtend       = "log"

	defaultLogReservedHour = 36
	defaultReportDuration  = 1
)

type MetricsServer struct {

	// current writing log file
	currentWritingLogFile *MetricsLogFile

	config *MetricsServerConfig

	shutdownWriteLoopCh  chan struct{}
	shutdownDeleteLoopCh chan struct{}

	shutdownFinishCh chan struct{}

	closeOnce sync.Once

	lastReport *MetricsLastReport

	wg sync.WaitGroup
}

type MetricsServerConfig struct {

	// second
	ReportDuration int

	// log reserve hour
	LogReservedHour int

	// the metrics file dir
	MetricsFileDir string

	// end point for this vdl server
	// current can use ip + vdl name
	EndPoint string

	// vdl name, use to metric file name
	VDLName string
}

type MetricsLogFile struct {
	file *os.File

	startTime *time.Time
}

type MetricsLastReport struct {

	// kafka
	ksWriteReqTps            float64
	KsWriteRespTps           float64
	KsWriteReqLogTps         float64
	KsWriteReqMsgErrMaxSize  float64
	KsWriteReqMsgErrOther    float64
	KsWriteReqSrvErrNoLeader float64
	KsReadLogTps             float64
	KsConnCount              float64
	KsConnOnlineConnection   float64
	//logstore
	LogstoreReadCount  float64
	LogstoreWriteCount float64

	SegmentReadCount     float64
	SegmentCutTotalCount float64
	SegmentCutCount      float64
	// raft, string is raft group name
	RaftLatencyPerEntryMap map[string]float64
}

func NewMetricsServer(conf *MetricsServerConfig) *MetricsServer {

	// os.cleanpath
	conf.MetricsFileDir = path.Clean(conf.MetricsFileDir)

	metricsServer := &MetricsServer{
		config:               conf,
		shutdownWriteLoopCh:  make(chan struct{}),
		shutdownFinishCh:     make(chan struct{}),
		shutdownDeleteLoopCh: make(chan struct{}),
		lastReport: &MetricsLastReport{
			RaftLatencyPerEntryMap: make(map[string]float64),
		},
	}

	if conf.ReportDuration == 0 {
		conf.ReportDuration = defaultReportDuration
	}

	if conf.LogReservedHour == 0 {
		conf.LogReservedHour = defaultLogReservedHour
	}

	glog.Infof("Metrcis Config , "+
		"ReportDuration :%v, LogReservedHour:%v, MetricsFileDir:%v , EndPoint:%v, VDLName:%v",
		conf.ReportDuration, conf.LogReservedHour, conf.MetricsFileDir, conf.EndPoint, conf.VDLName)

	return metricsServer
}

func (ms *MetricsServer) Start() {

	err := os.MkdirAll(ms.config.MetricsFileDir, fileutil.PrivateDirMode)
	if err != nil {
		glog.Fatalf("MkdirAll error in Start,err=%s", err.Error())
	}

	ms.wg.Add(1)
	go ms.startWriteLoop()

	ms.wg.Add(1)
	go ms.startDeleteLoop()

	ms.wg.Wait()

	ms.shutdownFinishCh <- struct{}{}
}

func (ms *MetricsServer) startWriteLoop() {

	defer func() {
		ms.wg.Done()
	}()

	ms.currentWritingLogFile = ms.createMetricsLogFile()

	for {
		timeoutCh := time.After(time.Duration(ms.config.ReportDuration) * time.Second)
		select {
		case <-ms.shutdownWriteLoopCh:
			glog.Infof("write loop received shutdown signal")
			ms.currentWritingLogFile.file.Close()
			return
		case <-timeoutCh:
			if ms.isNeedRollingFile() {
				err := ms.currentWritingLogFile.file.Close()
				if err != nil {
					glog.Errorf("close file[%s] error,err=%s", ms.currentWritingLogFile.file.Name(), err.Error())
				}
				ms.currentWritingLogFile = ms.createMetricsLogFile()
			}
			var timestamp int64 = time.Now().UnixNano() / int64(time.Millisecond)
			ms.writeKafkaMetrics(timestamp)
			ms.writeRaftMetrics(timestamp)
			ms.writeTransportMetrics(timestamp)
			ms.writeStoreMetrics(timestamp)
		}
	}

}

func (ms *MetricsServer) startDeleteLoop() {

	defer func() {
		ms.wg.Done()
	}()

	for {
		timeoutCh := time.After(time.Hour)
		select {
		case <-ms.shutdownDeleteLoopCh:
			glog.Infof("delete loop received shutdown signal")
			return
		case <-timeoutCh:
			// delete files
			fileNames, err := fileutil.ReadDir(ms.config.MetricsFileDir)
			if err != nil {
				glog.Errorf("read dir %v error, %v ", ms.config.MetricsFileDir, err)
				break
			}

			now := time.Now()

			for _, fileName := range fileNames {

				tags := strings.Split(fileName, "_")
				timeStr := tags[len(tags)-1]
				dayStr := tags[len(tags)-2]
				var err error
				var year, month, day, hour, min, sec int
				if year, err = strconv.Atoi(dayStr[0:4]); err != nil {
					glog.Errorf("log %v [year] error, %v ", fileName, err)
					continue
				}
				if month, err = strconv.Atoi(dayStr[4:6]); err != nil {
					glog.Errorf("log %v [month] error, %v ", fileName, err)
					continue
				}
				if day, err = strconv.Atoi(dayStr[6:8]); err != nil {
					glog.Errorf("log %v [day] error, %v ", fileName, err)
					continue
				}
				if hour, err = strconv.Atoi(timeStr[0:2]); err != nil {
					glog.Errorf("log %v [hour] error, %v ", fileName, err)
					continue
				}
				if min, err = strconv.Atoi(timeStr[2:4]); err != nil {
					glog.Errorf("log %v [min] error, %v ", fileName, err)
					continue
				}
				if sec, err = strconv.Atoi(timeStr[4:6]); err != nil {
					glog.Errorf("log %v [sec] error, %v ", fileName, err)
					continue
				}
				fileDate := time.Date(year, time.Month(month), day, hour, min, sec, 0, now.Location())

				// whether can delete
				if now.Sub(fileDate) > time.Duration(ms.config.LogReservedHour)*time.Hour {
					fileNameWithPath := filepath.Join(ms.config.MetricsFileDir, fileName)
					error := os.Remove(fileNameWithPath)
					if error != nil {
						glog.Errorf("remove log %v error, %v ", fileName, err)
					}
				}
			}
		}
	}

}

func (ms *MetricsServer) Stop() {

	ms.closeOnce.Do(func() {
		ms.shutdownWriteLoopCh <- struct{}{}
		ms.shutdownDeleteLoopCh <- struct{}{}
		<-ms.shutdownFinishCh
	})

}

// rolling if
// 1) reach max size
// 2) reach next hour
func (ms *MetricsServer) isNeedRollingFile() bool {

	//reach max size
	fileStat, err := ms.currentWritingLogFile.file.Stat()
	if err != nil {
		glog.Fatalf("Get file stat err,err=%s", err.Error())
	}
	if fileStat.Size() > logMaxSize {
		return true
	}

	// reach next hour
	now := time.Now()
	if now.Hour() != ms.currentWritingLogFile.startTime.Hour() {
		return true
	}

	return false
}

func (ms *MetricsServer) createMetricsLogFile() *MetricsLogFile {

	t := time.Now()
	fileName := ms.getNewLogName(t)
	fileNameWithPath := filepath.Join(ms.config.MetricsFileDir, fileName)

	// create log file
	f, err := os.Create(fileNameWithPath)
	if err != nil {
		glog.Fatalf("create MetricsLogFile error,err=%s", err.Error())
	}

	metricsLogFile := &MetricsLogFile{
		file:      f,
		startTime: &t,
	}

	return metricsLogFile
}

func (ms *MetricsServer) getNewLogName(t time.Time) string {
	name := fmt.Sprintf("%s_%s_%04d%02d%02d_%02d%02d%02d.%s",
		logFileNamePrefix,
		ms.config.VDLName,
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		logFileNameExtend)
	return name
}

func (ms *MetricsServer) generateMercury(metricName string, timestamp int64,
	value float64, counterType string, endpointType string) *Mercury {

	return &Mercury{
		Namespace:   "custom",
		MetricName:  metricName,
		Endpoint:    ms.config.EndPoint,
		Timestamp:   timestamp,
		DoubleValue: value,
		MercuryMeta: MercuryMeta{
			Duration:     ms.config.ReportDuration,
			CounterType:  counterType,
			EndpointType: endpointType,
		},
	}
}

func (ms *MetricsServer) mustWriteMercuryAndNewLine(m *Mercury) {
	b, err := m.MarshalJSON()
	if err != nil {
		glog.Fatalf("MarshalJSON error,err=%s",
			err.Error())
	}
	ms.mustWrite(b)
	ms.mustWrite([]byte("\n"))
}

func (ms *MetricsServer) mustWrite(b []byte) {
	wrote := 0
	for {
		n, err := ms.currentWritingLogFile.file.Write(b[wrote:])
		if err != nil {
			if err != io.ErrShortWrite {
				glog.Fatalf("write MetricsLogFile[%s] error,err=%s",
					ms.currentWritingLogFile.file.Name(), err.Error())
			}
		}
		if n+wrote == len(b) {
			break
		}
		wrote += n
	}

}

func (ms *MetricsServer) calculateTps(pre float64, current float64) float64 {

	if int64(current-pre) == 0 {
		return 0
	}

	return float64(int(current-pre) / ms.config.ReportDuration)
}

//MB/sec
func (ms *MetricsServer) calculateThroughputs(pre float64, current float64) float64 {
	if int64(current-pre) == 0 {
		return 0
	}
	return (current - pre) / (1024 * 1024) / float64(ms.config.ReportDuration)
}

type metricsValueCalculateFunc func(org *MetricsServer, metricDTO *io_prometheus_client.Metric) float64

func (ms *MetricsServer) doWriteMetrics(namespace string, timestamp int64, metric prometheus.Metric,
	callback metricsValueCalculateFunc, counterType string, endpoint string) {

	metricDTO := &io_prometheus_client.Metric{}
	metric.Write(metricDTO)
	ms.doWriteMetricsWithDTO(namespace, timestamp, metricDTO, callback, counterType, endpoint)

}

func (ms *MetricsServer) doWriteMetricsWithDTO(namespace string, timestamp int64, metricDTO *io_prometheus_client.Metric,
	callback metricsValueCalculateFunc, counterType string, endpoint string) {

	mercury := ms.generateMercury(
		namespace,
		timestamp,
		callback(ms, metricDTO),
		counterType,
		endpoint)
	ms.mustWriteMercuryAndNewLine(mercury)
}
