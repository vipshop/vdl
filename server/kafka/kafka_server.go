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

package kafka

import (
	"io"
	"net"
	"runtime"

	"time"

	"github.com/vipshop/vdl/pkg/glog"
	"github.com/vipshop/vdl/server/kafka/handler"
	"github.com/vipshop/vdl/server/kafka/kafkametrics"
	"github.com/vipshop/vdl/server/kafka/kafkaratequota"
	"github.com/vipshop/vdl/server/kafka/protocol"
	"github.com/vipshop/vdl/server/runtimeconfig"
)

const (
	ProducePipelineSize = 100
)

type Server struct {
	Addr               string
	Ln                 *net.TCPListener
	ShutdownCh         chan struct{}
	APIHandler         *handler.KafkaAPIHandler
	ConnectionsMaxIdle time.Duration
}

func NewServer(serverAddr string, timeout time.Duration, getter handler.LogStreamWrapperGetter, runtimeConfig *runtimeconfig.RuntimeConfig) (*Server, error) {

	kafkaratequota.SetKafkaRateByConfig(&runtimeConfig.RuntimeRateConfig)

	return &Server{
		Addr:               serverAddr,
		ShutdownCh:         make(chan struct{}),
		APIHandler:         &handler.KafkaAPIHandler{LogStreamWrapperGetter: getter},
		ConnectionsMaxIdle: timeout,
	}, nil
}

func (s *Server) Start() error {
	//profiling
	//go func() {
	//	log.Println(http.ListenAndServe("127.0.0.1:6060", nil))
	//}()
	addr, err := net.ResolveTCPAddr("tcp", s.Addr)
	if err != nil {
		glog.Errorf("[kafka_server.go-Start]:ResolveTCPAddr error,s.Addr=%s,error=%s", s.Addr, err.Error())
		return err
	}

	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		glog.Errorf("[kafka_server.go-Start]:ListenTCP error,error=%s", err.Error())
		return err
	}
	s.Ln = ln
	glog.Infof("[kafka_server.go-Start]: kafka server start,ConnectionsMaxIdle=%.2fs", s.ConnectionsMaxIdle.Seconds())
	for {
		select {
		case <-s.ShutdownCh:
			glog.Infof("[kafka_server.go-Start]: server closed")
			return nil
		default:
			conn, err := s.Ln.Accept()
			if err != nil {
				glog.Infof("[kafka_server.go-Start]:listener accept failed: %v", err)
				continue
			}
			glog.Infof("accept a connection, from %v", conn.RemoteAddr())
			//s.Wg.Add(1)
			//metrics
			kafkametrics.MetricsKsConnectTimes.Inc()
			kafkametrics.MetricsKsOnlineConnections.Inc()
			go s.handleRequest(conn)
		}
	}
	return nil
}

func (s *Server) Close() error {
	close(s.ShutdownCh)
	s.Ln.Close()
	//s.Wg.Wait()
	return nil
}

func (s *Server) handleRequest(conn net.Conn) {
	produceRequestChan := make(chan *handler.ProduceRequest, ProducePipelineSize)
	produceResponseChan := make(chan *handler.ProduceResponse, ProducePipelineSize)
	reqQuitChan := make(chan struct{}, 1)
	responseQuitChan := make(chan struct{}, 1)
	errChan := make(chan error, 1)

	defer func() {
		close(produceRequestChan)
		<-reqQuitChan
		close(produceResponseChan)
		<-responseQuitChan
		conn.Close()
		//metrics
		kafkametrics.MetricsKsOnlineConnections.Dec()
	}()
	//通过两个goroutine实现生产消息pipeline
	go s.APIHandler.HandleProducePipeline(produceRequestChan, produceResponseChan, reqQuitChan)
	go s.APIHandler.HandleResponsePipeline(conn, produceResponseChan, responseQuitChan, s.ShutdownCh)

	for {
		//收到退出信号或发生错误，则退出
		select {
		case <-s.ShutdownCh:
			glog.Infof("[kafka_server.go-handleRequest]: server closed")
			return
		case err := <-errChan:
			glog.Infof("[kafka_server.go-handleRequest]:handle message error:err=%s", err.Error())
			return
		default:
		}
		//设置读请求超时
		if err := conn.SetReadDeadline(time.Now().Add(s.ConnectionsMaxIdle)); err != nil {
			glog.Errorf("[kafka_server.go-handleRequest]:conn SetReadDeadline error:%s", err.Error())
			return
		}

		p := make([]byte, 4)
		n, err := io.ReadFull(conn, p[:])
		if err != nil {
			//超时则检查一下程序是否需要退出
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				glog.Infof("[kafka_server.go-handleRequest]:read time out")
				return
			}
			//客户端关闭连接
			if err == io.EOF {
				if glog.V(1) {
					glog.Infof("D:[kafka_server.go-handleRequest]:client closed this connection,conn.RemoteAddr=%s",
						conn.RemoteAddr().String())
				}
				break
			}
			glog.Errorf("[kafka_server.go-handleRequest]:ReadFull error:%s", err.Error())
			break
		}

		if n == 0 {
			glog.Infof("[kafka_server.go-handleRequest]: read null")
			continue
		}
		//取消超时
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			glog.Errorf("[kafka_server.go-handleRequest]:SetReadDeadline error:%s", err.Error())
			return
		}
		//获取包的大小
		size := protocol.Encoding.Uint32(p)
		if size == 0 {
			glog.Infof("[kafka_server.go-handleRequest]: header size is 0")
			continue
		}

		b := make([]byte, size+4) //+4 since we're going to copy the size into b
		copy(b, p)

		_, err = io.ReadFull(conn, b[4:])
		if err != nil {
			glog.Errorf("[kafka_server.go-handleRequest]: ReadFull error, error=%s", err.Error())
			break
		}

		d := protocol.NewDecoder(b)
		//set header
		header := new(protocol.RequestHeader)
		//handle error
		err = header.Decode(d)
		if err != nil {
			glog.Errorf("Decode header error,err=%s", err.Error())
			return
		}

		//debug info
		if glog.V(1) {
			glog.Infof("D:[kafka_server.go-handleRequest]:correlation id [%d], "+
				"request size [%d], key [%s], API_Version [%d],ClientID=%s,data=%v",
				header.CorrelationID, size,
				protocol.KeyToString[header.APIKey],
				header.APIVersion, header.ClientID, b)
		}
		switch header.APIKey {
		case protocol.ProduceKey:
			produceReq := &protocol.ProduceRequest{}
			//set produceReq
			err = s.decode(produceReq, d)
			if err != nil {
				glog.Errorf("decode produceReq error,err=%s,buf=%v", err.Error(), b)
				return
			}
			req := &handler.ProduceRequest{
				Header:  header,
				Request: produceReq,
			}
			produceRequestChan <- req
		case protocol.APIVersionsKey:
			// API Version haven't body
			if err = s.APIHandler.HandleApiVersions(conn, header); err != nil {
				glog.Errorf("[kafka_server.go-handleRequest]:handleApiVersions error,conn.RemoteAddr=%s,error=%s",
					conn.RemoteAddr().String(), err.Error())
			}
		case protocol.MetadataKey:
			req := &protocol.MetadataRequest{}
			err = s.decode(req, d)
			if err != nil && err != protocol.ErrArrayIsNull {
				glog.Errorf("decode metadata error,err=%s,buf=%v", err.Error(), b)
				return
			}
			switch header.APIVersion {
			case 0:
				if err = s.APIHandler.HandleMetadataV0(conn, header, req); err != nil {
					glog.Errorf("[kafka_server.go-handleRequest]:HandleMetadataV0 Metadata request failed: %s", err)
				}
			case 1:
				if err = s.APIHandler.HandleMetadataV1(conn, header, req); err != nil {
					glog.Errorf("[kafka_server.go-handleRequest]:HandleMetadataV1 Metadata request failed: %s", err)
				}
			case 2:
				if err = s.APIHandler.HandleMetadataV2(conn, header, req); err != nil {
					glog.Errorf("[kafka_server.go-handleRequest]:HandleMetadataV2 Metadata request failed: %s", err)
				}
			default:
				glog.Errorf("[kafka_server.go-handleRequest]:MetadataKey api version[%d],not support",
					header.APIVersion)
				//TODO return not support message to client
			}
		case protocol.FetchKey:
			req := &protocol.FetchRequest{}
			err = s.decode(req, d)
			if err != nil {
				glog.Errorf("decode FetchRequest error,err=%s,buf=%v", err.Error(), b)
				return
			}
			if err = s.APIHandler.HandleFetch(conn, header, req); err != nil {
				glog.Errorf("Find Fetch request failed: %s", err)
			}
		case protocol.OffsetsKey:
			req := &protocol.OffsetsRequest{}
			err = s.decode(req, d)
			if err != nil {
				glog.Errorf("decode OffsetsRequest error,err=%s,buf=%v", err.Error(), b)
				return
			}
			if err = s.APIHandler.HandleOffsets(conn, header, req); err != nil {
				glog.Errorf("HandleOffsets request failed: %s", err)
			}
		default:
			glog.Errorf("VDL unsupport API KEY : %d, disconnect this connection [%v]", header.APIKey, conn.RemoteAddr())
			return
		}
	}
	return
}

func (s *Server) decode(req protocol.Decoder, d protocol.PacketDecoder) (err error) {
	//defer recover kafka protocol decode
	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
			buf := make([]byte, 16384)
			buf = buf[:runtime.Stack(buf, false)]
			glog.Errorf("=== BEGIN goroutine stack dump ===\n%s\n=== END goroutine stack dump ===", string(buf))
		}
	}()
	err = req.Decode(d)
	if err != nil {
		return err
	}
	return nil
}
