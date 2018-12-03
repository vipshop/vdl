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

package httputil

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/vipshop/vdl/pkg/glog"
)

type HTTPError struct {
	Message string `json:"message"`
	// Code is the HTTP status code
	Code int `json:"-"`
}

func (e HTTPError) Error() string {
	return e.Message
}

func (e HTTPError) WriteTo(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Code)
	b, err := json.Marshal(e)
	if err != nil {
		glog.Fatalf("marshal HTTPError should never fail (%v)", err)
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return nil
}

func NewHTTPError(code int, m string) *HTTPError {
	return &HTTPError{
		Message: m,
		Code:    code,
	}
}

// allowMethod verifies that the given method is one of the allowed methods,
// and if not, it writes an error to w.  A boolean is returned indicating
// whether or not the method is allowed.
func AllowMethod(w http.ResponseWriter, m string, ms ...string) bool {
	for _, meth := range ms {
		if m == meth {
			return true
		}
	}
	w.Header().Set("Allow", strings.Join(ms, ","))
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	return false
}

// writeError logs and writes the given Error to the ResponseWriter
// If Error is an VDL error, it is rendered to the ResponseWriter
// Otherwise, it is assumed to be a StatusInternalServerError
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		return
	}
	switch e := err.(type) {
	case *HTTPError:
		if et := e.WriteTo(w); et != nil {
			//debug info
			if glog.V(1) {
				glog.Infof("D:error writing HTTPError (%v) to %s", et, r.RemoteAddr)
			}
		}
	default:
		herr := NewHTTPError(http.StatusInternalServerError, "Internal Server Error")
		if et := herr.WriteTo(w); et != nil {
			//debug info
			if glog.V(1) {
				glog.Infof("D:error writing HTTPError (%v) to %s", et, r.RemoteAddr)
			}
		}
	}
}
