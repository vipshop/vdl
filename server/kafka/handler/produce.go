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

package handler

import (
	"hash/crc32"
	"net"
	"time"

	"context"

	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/server/kafka/kafkametrics"
	"github.com/vipshop/vdl/server/kafka/kafkaratequota"
	"github.com/vipshop/vdl/server/kafka/protocol"
)

type ProduceRequest struct {
	Header  *protocol.RequestHeader
	Request *protocol.ProduceRequest
}

type ProduceResponse struct {
	*protocol.Response
	beginTime time.Time
}

const LogPrintInterval = time.Second * 2

//version 2
func (h *KafkaAPIHandler) HandleProducePipeline(reqChan chan *ProduceRequest,
	responseChan chan *ProduceResponse, ShutdownCh chan struct{}) error {
	lastPrintErrorLogTime := time.Now()
	for {
		req, ok := <-reqChan
		if ok == false {
			ShutdownCh <- struct{}{}
			if glog.V(1) {
				glog.Infof("D:[produce.go-HandleProducePipeline]:HandleProducePipeline quit")
			}
			return nil
		}

		//metrics
		kafkametrics.MetricsKsWriteReqTps.Inc()
		beginTime := time.Now()

		// rate limit
		kafkaRate := kafkaratequota.GetKafkaRate()
		if kafkaRate.IsEnableRateQuota {
			kafkaRate.WriteByteLimiter.WaitN(context.Background(), int(req.Header.Size))
			kafkaRate.WriteRequestLimiter.Wait(context.Background())
		}

		resp := new(protocol.ProduceResponses)
		resp.Responses = make([]*protocol.ProduceResponse, len(req.Request.TopicData))
		resp.ThrottleTimeMs = 0 //not support
		for i, td := range req.Request.TopicData {
			//one topic has only one partition
			presps := make([]*protocol.ProducePartitionResponse, len(td.Data))
			for j, p := range td.Data {
				presp := &protocol.ProducePartitionResponse{}
				presp.Partition = p.Partition
				presp.ErrorCode = 0
				presp.Timestamp = time.Now().Unix()
				presp.OffsetChannels = make([]<-chan interface{}, 0, 10)
				presps[j] = presp

				lsw, err := h.LogStreamWrapperGetter.GetLogStreamWrapper(td.Topic)
				if err != nil {
					glog.Warningf("[produce.go-HandleProducePipeline]:GetLogStreamWrapper error,error=%s,topic=%s",
						err.Error(), td.Topic)
					presp.ErrorCode = INVALID_TOPIC_EXCEPTION
					//metrics
					kafkametrics.MetricsKsWriteReqMsgErrOther.Inc()
					break
				}

				// should not 0 messages
				if len(p.Messages) <= 0 {
					glog.Warningf("[produce.go-HandleProducePipeline]: should not 0 messages for topic=%s, partition=%v",
						td.Topic, p.Partition)
					presp.ErrorCode = UNKNOWN
					//metrics
					kafkametrics.MetricsKsWriteReqMsgErrOther.Inc()
					break
				}

				// check leadership, if leadership change between StoreMessage,
				// client will not receive resp because raft will not notify
				if lsw.IsLeader() == false {
					duration := time.Now().Sub(lastPrintErrorLogTime)
					if LogPrintInterval < duration {
						glog.Errorf("[produce.go-HandleProducePipeline]:print ErrorLog every %v,produce message to follower", duration)
						lastPrintErrorLogTime = time.Now()
					}
					presp.ErrorCode = NOT_LEADER_FOR_PARTITION
					//metrics
					kafkametrics.MetricsKsWriteReqSrvErrNoLeader.Inc()
					break
				}

				var checkSuccess bool = true
				datas := make([][]byte, len(p.Messages))

				// previous check, abort fast and avoid accept part of message but reject others
				for index, message := range p.Messages {
					if message == nil {
						presp.ErrorCode = UNKNOWN
						checkSuccess = false
						//metrics
						kafkametrics.MetricsKsWriteReqMsgErrOther.Inc()
						break
					}

					// check message size
					if len(message.Value) > lsw.GetMaxSizePerMsg() {
						presp.ErrorCode = MESSAGE_TOO_LARGE
						checkSuccess = false
						//metrics
						kafkametrics.MetricsKsWriteReqMsgErrMaxSize.Inc()
						break
					}

					err := h.CheckMessageAttributes(message.Attributes)
					if err != nil {
						glog.Errorf("[produce.go-HandleProducePipeline]:CheckMessageAttributes error,Attribute=%d",
							message.Attributes)
						presp.ErrorCode = UNSUPPORTED_FOR_MESSAGE_FORMAT
						checkSuccess = false
						//metrics
						kafkametrics.MetricsKsWriteReqMsgErrOther.Inc()
						break
					}

					data, err := protocol.Encode(message.Message)
					if err != nil {
						glog.Errorf("[produce.go-HandleProducePipeline]:Encode message error, error=%s,message=%v",
							err.Error(), *(message.Message))
						presp.ErrorCode = UNKNOWN
						checkSuccess = false
						//metrics
						kafkametrics.MetricsKsWriteReqMsgErrOther.Inc()
						break
					}

					err = h.CheckMessageCrc32(data)
					if err != nil {
						glog.Errorf("[produce.go-HandleProducePipeline]:CheckMessageCrc32 message error,message=%v",
							*(message.Message))
						presp.ErrorCode = CORRUPT_MESSAGE
						checkSuccess = false
						//metrics
						kafkametrics.MetricsKsWriteReqMsgErrOther.Inc()
						break
					}

					datas[index] = data
				}

				//store message into storage
				if checkSuccess {
					// return err when the following cases:
					// 1) raftGroup not start
					// 2) raft node close
					// 3) cannot put into raft process (ctx timeout)
					// 4) fast fail when :
					//    4.1) doing leader transfer
					//    4.2) no leader
					// all cases haven't add to raft
					chs, err := lsw.StoreMessageBatch(datas)
					if err != nil {
						glog.Errorf("[produce.go-HandleProducePipeline]:StoreMessage error,error=%s", err.Error())
						presp.ErrorCode = BROKER_NOT_AVAILABLE
						break
					}
					presp.OffsetChannels = append(presp.OffsetChannels, chs...)
				}

				//metrics
				kafkametrics.MetricsKsWriteReqLogTps.Add(float64(len(p.Messages)))

			}
			resp.Responses[i] = &protocol.ProduceResponse{
				Topic:              td.Topic,
				PartitionResponses: presps,
			}
		}
		resp.SetVersion(req.Header.APIVersion)
		r := &protocol.Response{
			CorrelationID: req.Header.CorrelationID,
			Body:          resp,
		}
		produceResponse := new(ProduceResponse)
		produceResponse.Response = r
		produceResponse.beginTime = beginTime
		responseChan <- produceResponse
	}
	return nil
}

func (h *KafkaAPIHandler) CheckMessageCrc32(data []byte) error {
	var dataCrc32 uint32
	dataCrc32 = protocol.Encoding.Uint32(data[:4])
	calculateCrc32 := crc32.ChecksumIEEE(data[4:])
	if glog.V(1) {
		glog.Infof("D:[produce.go-CheckMessageCrc32]:message dataCrc32=%d,calculateCrc32=%d,data=%v",
			dataCrc32, calculateCrc32, data)
	}
	if calculateCrc32 != dataCrc32 {
		glog.Errorf("[produce.go-CheckMessageCrc32]:message crc32 is not consistent,dataCrc32=%d,calculateCrc32=%d,data=%v",
			dataCrc32, calculateCrc32, data)
		return ErrCrcNotMatch
	}
	return nil
}

func (h *KafkaAPIHandler) CheckMessageAttributes(Attributes int8) error {
	//get the lowest 3 bits
	compressCodec := Attributes & 0x07
	if glog.V(1) {
		glog.Infof("D:[produce.go-CheckMessageAttributes]:message Attributes=%d,compressCodec=%d",
			Attributes, compressCodec)
	}
	switch int8(compressCodec) {
	case 0:
	case 1:
		glog.Errorf("[produce.go-CheckMessageAttributes]:message don't support GZIP compression")
		return ErrNotSupport
	case 2:
		glog.Errorf("[produce.go-CheckMessageAttributes]:message don't support Snappy compression")
		return ErrNotSupport
	default:
		glog.Errorf("[produce.go-CheckMessageAttributes]:message don't support this feature")
		return ErrNotSupport
	}
	return nil
}

func (h *KafkaAPIHandler) HandleResponsePipeline(conn net.Conn,
	responseChan chan *ProduceResponse, ShutdownCh chan struct{}, kafkaClosedChan chan struct{}) error {
	var err error
	lastPrintErrorLogTime := time.Now()
	for {
		resp, ok := <-responseChan
		if ok == false {
			ShutdownCh <- struct{}{}
			if glog.V(1) {
				glog.Infof("D:[produce.go-HandleResponsePipeline]:HandleResponsePipeline quit")
			}
			return nil
		}
		prs := resp.Response.Body.(*protocol.ProduceResponses)
		for i := 0; i < len(prs.Responses); i++ {
			topicResp := prs.Responses[i]
			for j := 0; j < len(topicResp.PartitionResponses); j++ {
				chs := topicResp.PartitionResponses[j].OffsetChannels
				for m, ch := range chs {
					if ch == nil {
						prs.Responses[i].PartitionResponses[j].ErrorCode = BROKER_NOT_AVAILABLE
						break
					} else {
						select {
						case x := <-ch:
							if x == nil {
								glog.Fatalf("[logstream_wrapper.go-HandleResponsePipeline]:Store Message has no offset.")
							}
							offset := x.(int64)
							if m == 0 {
								prs.Responses[i].PartitionResponses[j].BaseOffset = offset
							}
						case <-kafkaClosedChan:
							ShutdownCh <- struct{}{}
							if glog.V(1) {
								glog.Infof("D:[produce.go-HandleResponsePipeline]:HandleResponsePipeline quit as kafka server shutdown")
							}
							return nil
						}
					}
				}
			}
		}
		//TODO write with buffer

		resp.Body = prs
		err = h.write(conn, resp.Response)
		if err != nil {
			duration := time.Now().Sub(lastPrintErrorLogTime)
			if LogPrintInterval < duration {
				glog.Errorf(
					"[produce.go-HandleResponsePipeline]:print ErrorLog every %v,write response error,error=%s",
					duration, err.Error())
				lastPrintErrorLogTime = time.Now()
			}
		}

		//metrics
		kafkametrics.MetricsKsWriteRespTps.Inc()
		kafkametrics.MetricsKsWriteLatency.Observe(float64(time.Now().Sub(resp.beginTime) / time.Millisecond))
	}
	return nil
}
