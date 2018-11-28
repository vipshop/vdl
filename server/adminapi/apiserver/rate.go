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

package apiserver

import (
	"encoding/json"
	"errors"
	"github.com/vipshop/vdl/server/adminapi/apierror"
	"github.com/vipshop/vdl/server/adminapi/apipb"
	"github.com/vipshop/vdl/server/kafka/kafkaratequota"
	"golang.org/x/net/context"
	"strconv"
	"strings"
)

type RateAdminServer struct {
	rateInfoGetter RateInfoGetter
}

func NewRateAdminServer(getter RateInfoGetter) *RateAdminServer {
	return &RateAdminServer{
		rateInfoGetter: getter,
	}
}

// this function use for rpc server handle list rate info
func (ras *RateAdminServer) ListRateInfo(ctx context.Context, req *apipb.RateListRequest) (*apipb.RateListResponse, error) {

	b, err := json.Marshal(&ras.rateInfoGetter.GetRuntimeConfig().RuntimeRateConfig)
	if err != nil {
		return nil, apierror.ToRPCError(err)
	}
	response := &apipb.RateListResponse{
		Msg: b,
	}
	return response, nil
}

// this function use for rpc server handle update rate info
func (ras *RateAdminServer) UpdateRateInfo(ctx context.Context, req *apipb.RateUpdateRequest) (*apipb.RateUpdateResponse, error) {

	requestString := string(req.Msg)

	// check string
	keyValues := strings.Split(requestString, ",")

	// pre check
	for _, keyValue := range keyValues {
		temp := strings.Split(keyValue, "=")
		if len(temp) != 2 {
			return nil, apierror.ToRPCError(errors.New(keyValue + " is invalid configuration"))
		}
		err := preCheckRateConfig(temp[0], temp[1])
		if err != nil {
			return nil, apierror.ToRPCError(err)
		}
	}

	// do update
	// update memory and take effect
	// for RuntimeConfig update, we currently not use atomic update/load,
	// because this a low frequency just use for cli
	for _, keyValue := range keyValues {
		temp := strings.Split(keyValue, "=")
		key := temp[0]
		value := temp[1]
		switch key {
		case "IsEnableRateQuota":
			updateValue, _ := strconv.ParseBool(value)
			ras.rateInfoGetter.GetRuntimeConfig().IsEnableRateQuota = updateValue
		case "WriteRequestRate":
			updateValue, _ := strconv.Atoi(value)
			ras.rateInfoGetter.GetRuntimeConfig().WriteRequestRate = updateValue
		case "WriteByteRate":
			updateValue, _ := strconv.Atoi(value)
			ras.rateInfoGetter.GetRuntimeConfig().WriteByteRate = updateValue
		case "CatchupReadRequestRate":
			updateValue, _ := strconv.Atoi(value)
			ras.rateInfoGetter.GetRuntimeConfig().CatchupReadRequestRate = updateValue
		case "CatchupReadByteRate":
			updateValue, _ := strconv.Atoi(value)
			ras.rateInfoGetter.GetRuntimeConfig().CatchupReadByteRate = updateValue
		case "EndReadRequestRate":
			updateValue, _ := strconv.Atoi(value)
			ras.rateInfoGetter.GetRuntimeConfig().EndReadRequestRate = updateValue
		case "EndReadByteRate":
			updateValue, _ := strconv.Atoi(value)
			ras.rateInfoGetter.GetRuntimeConfig().EndReadByteRate = updateValue
		}
	}

	// update rate had protect by lock
	kafkaratequota.SetKafkaRateByConfig(&ras.rateInfoGetter.GetRuntimeConfig().RuntimeRateConfig)

	// update store
	ras.rateInfoGetter.GetRuntimeConfigStore().UpdateRuntimeConfig(ras.rateInfoGetter.GetRuntimeConfig())

	b, err := json.Marshal(&ras.rateInfoGetter.GetRuntimeConfig().RuntimeRateConfig)
	if err != nil {
		return nil, apierror.ToRPCError(err)
	}
	response := &apipb.RateUpdateResponse{
		Msg: b,
	}
	return response, nil
}

func preCheckRateConfig(key string, value string) error {

	switch key {

	case "IsEnableRateQuota":
		_, err := strconv.ParseBool(value)
		if err != nil {
			return errors.New("IsEnableRateQuota should be true|false")
		}
	case "WriteRequestRate", "WriteByteRate",
		"CatchupReadRequestRate", "CatchupReadByteRate",
		"EndReadRequestRate", "EndReadByteRate":

		_, err := strconv.Atoi(value)
		if err != nil {
			return errors.New(key + " should be a valid integer")
		}

	default:
		return errors.New("Cannot found any config for " + key)
	}

	return nil

}
