package apiserver

import (
	"encoding/json"
	"net/http"

	"github.com/vipshop/vdl/consensus/raftgroup/api/apicommon"
)

const (
	VDLServerInfoPrefix = "/api/vdlserverinfo"
)

type VDLServerInfoHandler struct {
	apicommon.VDLServerInfo
}

func NewVDLServerInfoHandler(nodeID uint64, vdlserverID int32, host string, port int32) *VDLServerInfoHandler {
	return &VDLServerInfoHandler{
		VDLServerInfo: apicommon.VDLServerInfo{
			RaftNodeID:  nodeID,
			VDLServerID: vdlserverID,
			Host:        host,
			Port:        port,
		},
	}
}

func (h *VDLServerInfoHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !apicommon.AllowMethod(w, r.Method, "GET") {
		return
	}

	b, err := json.Marshal(h.VDLServerInfo)
	if err != nil {
		panic(err)
	}
	w.Write(b)
}
