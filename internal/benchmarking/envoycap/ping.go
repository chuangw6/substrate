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
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"google.golang.org/protobuf/proto"
)

// pingPath is the glutton workload's echo endpoint. The workload is
// deliberately trivial — protobuf in, the same message back — so that what the
// ladder measures is the entry path, not the workload.
const pingPath = "/ping"

// Pinger issues one glutton ping through the router.
type Pinger struct {
	// Client is the shared, tuned HTTP client.
	Client *http.Client
	// RouterURL is the router base URL, no trailing slash.
	RouterURL string
}

// Ping POSTs a protobuf echo request addressed to hostHeader and verifies the
// reply. token is echoed back by the workload and must be unique per request;
// the caller supplies it so this path does no crypto/rand work at 2000 QPS.
//
// No trace context is injected. Envoy's shipped OpenTelemetry config samples at
// 100% only for requests that arrive without a parent, so injecting one would
// silently change the tracing behavior we are supposed to be measuring
// as-shipped.
func (p *Pinger) Ping(ctx context.Context, hostHeader, token string) (Outcome, string) {
	body, err := proto.Marshal(&gluttonpb.PingRequest{Message: token})
	if err != nil {
		return OutcomeTransport, "marshal"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.RouterURL+pingPath, bytes.NewReader(body))
	if err != nil {
		return OutcomeTransport, "build_request"
	}
	req.Host = hostHeader
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := p.Client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || isTimeout(err) {
			return OutcomeTimeout, "timeout"
		}
		return OutcomeTransport, "transport"
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		if isTimeout(err) {
			return OutcomeTimeout, "timeout"
		}
		return OutcomeTransport, "body_read"
	}
	if resp.StatusCode >= 400 {
		return OutcomeHTTPError, "http_" + strconv.Itoa(resp.StatusCode)
	}

	pong := &gluttonpb.PingResponse{}
	if err := proto.Unmarshal(respBody, pong); err != nil {
		return OutcomeMismatch, "unmarshal"
	}
	if pong.GetMessage() != token {
		return OutcomeMismatch, "echo_mismatch"
	}
	return OutcomeOK, ""
}

func isTimeout(err error) bool {
	var t interface{ Timeout() bool }
	return errors.As(err, &t) && t.Timeout()
}
