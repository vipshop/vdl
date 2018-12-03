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

package apiclient

import (
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/vipshop/vdl/server/adminapi/apipb"
	"google.golang.org/grpc"
)

var (
	ErrNoAvailableEndpoints = errors.New("VDL Client: no available endpoints")
)

type Client struct {
	apipb.MembershipClient
	apipb.LogStreamAdminClient
	apipb.DevToolClient
	apipb.SnapshotAdminClient
	apipb.RateAdminClient

	conn *grpc.ClientConn
	cfg  *ClientConfig
}

type ClientConfig struct {
	endpoints   []string
	dialTimeout time.Duration
}

func NewClient(endpoints []string, dialTimeout time.Duration) (*Client, error) {

	if len(endpoints) == 0 {
		return nil, ErrNoAvailableEndpoints
	}

	conf := &ClientConfig{
		endpoints:   endpoints,
		dialTimeout: dialTimeout,
	}

	client := &Client{
		conn: nil,
		cfg:  conf,
	}

	var err error
	client.conn, err = grpc.Dial(getHost(endpoints[0]), grpc.WithTimeout(conf.dialTimeout), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client.MembershipClient = apipb.NewMembershipClient(client.conn)
	client.LogStreamAdminClient = apipb.NewLogStreamAdminClient(client.conn)
	client.DevToolClient = apipb.NewDevToolClient(client.conn)
	client.SnapshotAdminClient = apipb.NewSnapshotAdminClient(client.conn)
	client.RateAdminClient = apipb.NewRateAdminClient(client.conn)

	return client, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func getHost(ep string) string {
	url, uerr := url.Parse(ep)
	if uerr != nil || !strings.Contains(ep, "://") {
		return ep
	}
	return url.Host
}
