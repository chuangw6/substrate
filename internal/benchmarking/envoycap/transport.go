// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package envoycap

import (
	"net"
	"net/http"
	"time"
)

// NewHTTPClient builds the client used for every measured ping.
//
// Deliberately not http.DefaultTransport: its MaxIdleConnsPerHost of 2
// serializes the client behind two connections and the resulting queueing
// reads back as server latency. Every request here goes to the same host (the
// router Service), so the per-host pool is the whole pool.
//
// HTTP/2 is left off so the client->Envoy hop stays HTTP/1.1 keep-alive. That
// keeps the client off the ephemeral-port treadmill; the fresh-connection-per-
// request behavior we are measuring is Envoy's upstream hop to the worker
// pods (max_requests_per_connection: 1), not ours.
func NewHTTPClient(idleConns int, timeout time.Duration) *http.Client {
	if idleConns < 1 {
		idleConns = 1
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        idleConns,
			MaxIdleConnsPerHost: idleConns,
			MaxConnsPerHost:     0,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
			ForceAttemptHTTP2:   false,
		},
	}
}
