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

package command

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/vipshop/vdl/server/adminapi/apipb"
	"github.com/vipshop/vdl/server/runtimeconfig"
	"os"
	"text/tabwriter"
)

type simplePrinter struct {
}

func (s *simplePrinter) MemberAdd(r *apipb.MemberAddResponse) {
	fmt.Printf("success:Member %16x added to logstream %s in cluster [%16x]\n", r.Member.ID, r.Header.LogstreamName, r.Header.ClusterId)
}

func (s *simplePrinter) MemberRemove(id uint64, r *apipb.MemberRemoveResponse) {
	fmt.Printf("success:Member %16x removed from logstream %s in cluster [%16x]\n", id, r.Header.LogstreamName, r.Header.ClusterId)
}

func (s *simplePrinter) MemberUpdate(id uint64, r *apipb.MemberUpdateResponse) {
	fmt.Printf("success:Member %16x updated in logstream %s in cluster [%16x]\n", id, r.Header.LogstreamName, r.Header.ClusterId)
}

func (s *simplePrinter) MemberList(r *apipb.MemberListResponse) {
	_, rows := makeMemberListTable(r)
	for _, row := range rows {
		fmt.Println(strings.Join(row, ", "))
	}
}

func (s *simplePrinter) LogStreamStart(r *apipb.LogStreamStartResponse) {
	fmt.Printf("logstream %s started\n", r.Header.LogstreamName)
}

func (s *simplePrinter) LogStreamStop(r *apipb.LogStreamStopResponse) {
	fmt.Printf("logstream %s stoped\n", r.Header.LogstreamName)
}

func (s *simplePrinter) LogStreamLeaderTransfer(r *apipb.LogStreamLeaderTransferResponse) {
	fmt.Printf("%s leader transfer success! \n", r.Header.LogstreamName)
}

func (s *simplePrinter) DeleteLogStreamFile(r *apipb.DeleteFileResponse) {
	if r.ResultStatus == "OK" {
		fmt.Printf("delete logstream files success\n")
	} else {
		fmt.Printf("delete logstream files failed,err=%s", r.ErrorMsg)
	}
}

func (s *simplePrinter) GetDeletePermission(r *apipb.GetDeletePermissionResp) {
	if r.ResultStatus == "disable" {
		fmt.Printf("not allow to delete segment in this raft group\n")
	} else {
		fmt.Printf("allow to delete segment in this raft group\n")
	}
}

func (s *simplePrinter) SetDeletePermission(r *apipb.SetDeletePermissionResp) {
	if r.ResultStatus == "OK" {
		fmt.Printf("OK")
	} else {
		fmt.Printf("ERROR:set delete permission error,err=%s\n", r.ErrorMsg)
	}
}

func (s *simplePrinter) DebugSwitch(r *apipb.DebugSwitchResponse) {
	if r.ResultStatus == "OK" {
		fmt.Printf("switch debug log success\n")
	} else {
		fmt.Printf("switch debug log,err=%s", r.ErrorMsg)
	}
}

func (s *simplePrinter) CreateSnapshotMeta(r *apipb.SnapshotResponse) {
	if r.ResultStatus == "OK" {
		fmt.Printf("OK:create snapshot meta file success\n")
		fmt.Printf("%s\n", string(r.Msg))
	} else {
		fmt.Printf("ERROR:create snapshot meta file failed,err=%s", string(r.Msg))
	}
}

func (s *simplePrinter) UpdateRate(rateInfo *runtimeconfig.RuntimeRateConfig) {
	fmt.Printf("========Update Rate Success! Updated Config========\n")
	s.ListRate(rateInfo)
}

func (s *simplePrinter) ListRate(rateInfo *runtimeconfig.RuntimeRateConfig) {

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	fmt.Fprintf(w, "IsEnableRateQuota\t: %v\n", rateInfo.IsEnableRateQuota)
	fmt.Fprintf(w, "WriteRequestRate\t: %d\n", rateInfo.WriteRequestRate)
	fmt.Fprintf(w, "WriteByteRate\t: %d\n", rateInfo.WriteByteRate)
	fmt.Fprintf(w, "CatchupReadRequestRate\t: %d\n", rateInfo.CatchupReadRequestRate)
	fmt.Fprintf(w, "CatchupReadByteRate\t: %d\n", rateInfo.CatchupReadByteRate)
	fmt.Fprintf(w, "EndReadRequestRate\t: %d\n", rateInfo.EndReadRequestRate)
	fmt.Fprintf(w, "EndReadByteRate\t: %d\n", rateInfo.EndReadByteRate)
	w.Flush()
}

func makeMemberListTable(r *apipb.MemberListResponse) (hdr []string, rows [][]string) {
	hdr = []string{"ID", "Name", "Peer Addrs", "IsLeader"}
	for _, m := range r.Members {
		isLeader := m.ID == r.Leader
		rows = append(rows, []string{
			"ID: " + fmt.Sprintf("%x", m.ID),
			"Name: " + m.Name,
			"PeerAddrs: " + strings.Join(m.PeerURLs, ","),
			"IsLeader: " + strconv.FormatBool(isLeader),
		})
	}
	return
}
