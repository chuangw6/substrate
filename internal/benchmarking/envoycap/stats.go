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
	"math"
	"slices"
	"time"
)

// Outcome classifies how a request ended. Every outcome, success or not,
// contributes its latency to the percentiles: dropping failures would flatter
// the numbers exactly when the system is in trouble.
type Outcome string

const (
	// OutcomeOK is a 2xx whose echoed payload matched what was sent.
	OutcomeOK Outcome = "ok"
	// OutcomeHTTPError is a response with status >= 400.
	OutcomeHTTPError Outcome = "http_error"
	// OutcomeTimeout is a request the client gave up on.
	OutcomeTimeout Outcome = "timeout"
	// OutcomeTransport is a connection-level failure (refused, reset, no port).
	OutcomeTransport Outcome = "transport"
	// OutcomeMismatch is a 2xx whose body did not echo the request.
	OutcomeMismatch Outcome = "mismatch"
)

// Sample is one measured request.
type Sample struct {
	// ScheduledAt is when the request was due, per the pacer.
	ScheduledAt time.Time
	// CompletedAt is when the response (or failure) landed. Used to derive
	// achieved throughput, which is a completion rate.
	CompletedAt time.Time
	// LatencyMS is CompletedAt - ScheduledAt, in fractional milliseconds.
	// Measured from the scheduled time and not from the actual send, so that
	// any client fall-behind lands in the reported latency rather than being
	// hidden by it.
	LatencyMS float64
	// DispatchLagMS is actualSendAt - ScheduledAt: how late the client itself
	// was. A rig guard, and already included in LatencyMS.
	DispatchLagMS float64
	// Outcome is how the request ended.
	Outcome Outcome
	// Class is the finer-grained reason, e.g. "http_503" or "timeout". Empty
	// for OutcomeOK.
	Class string
	// ActorIdx is the actor the request was addressed to.
	ActorIdx int
}

// MSFloat converts a duration to fractional milliseconds. The existing boomer
// metrics path truncates to whole milliseconds, which is unusable at a 3 ms
// latency floor.
func MSFloat(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}

// Quantiles are the latency percentiles reported for one step.
type Quantiles struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

// Percentile returns the q-quantile (0..1) of an already-sorted slice using
// the nearest-rank method. Exact, because we keep every raw sample rather than
// bucketing.
func Percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Summarize sorts values in place and returns its percentiles.
func Summarize(values []float64) Quantiles {
	slices.Sort(values)
	return Quantiles{
		P50: Percentile(values, 0.50),
		P95: Percentile(values, 0.95),
		P99: Percentile(values, 0.99),
		Max: Percentile(values, 1.0),
	}
}

// AfterWarmup returns the samples scheduled at or after stepStart+warmup.
//
// The filter is on the scheduled time, not the completion time: a request
// offered inside the measurement window belongs to it however long it takes to
// come back, and a request offered during warmup does not join the window just
// because it landed late.
func AfterWarmup(samples []Sample, stepStart time.Time, warmup time.Duration) []Sample {
	cutoff := stepStart.Add(warmup)
	out := make([]Sample, 0, len(samples))
	for _, s := range samples {
		if !s.ScheduledAt.Before(cutoff) {
			out = append(out, s)
		}
	}
	return out
}
