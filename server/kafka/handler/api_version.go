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
	"net"

	"github.com/vipshop/vdl/server/kafka/protocol"
)

var (
	supportVersions = make([]*protocol.ApiVersion, 6)
)

func init() {

	// common
	supportVersions[0] = &protocol.ApiVersion{ApiKey: protocol.APIVersionsKey, MinVersion: 0, MaxVersion: 0}
	supportVersions[1] = &protocol.ApiVersion{ApiKey: protocol.MetadataKey, MinVersion: 0, MaxVersion: 2}
	supportVersions[2] = &protocol.ApiVersion{ApiKey: protocol.LeaderAndISRKey, MinVersion: 0, MaxVersion: 0}

	//produce
	supportVersions[3] = &protocol.ApiVersion{ApiKey: protocol.ProduceKey, MinVersion: 0, MaxVersion: 2}

	//consumer
	supportVersions[4] = &protocol.ApiVersion{ApiKey: protocol.FetchKey, MinVersion: 0, MaxVersion: 2}
	supportVersions[5] = &protocol.ApiVersion{ApiKey: protocol.OffsetsKey, MinVersion: 0, MaxVersion: 0}

}

func (h *KafkaAPIHandler) HandleApiVersions(conn net.Conn, header *protocol.RequestHeader) error {
	response := new(protocol.APIVersionsResponse)
	response.RequestVersion = header.APIVersion
	response.ApiVersions = supportVersions
	response.ErrorCode = 0
	response.ThrottleTimeMs = 0
	r := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          response,
	}
	return h.write(conn, r)
}
