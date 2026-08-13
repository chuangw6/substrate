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

// The output contract: the flat per-window record, where it goes — a JSONL file
// for a local run, tagged stdout for the in-cluster Job — and the ladder spec
// that expands into the rungs a run walks.

package routercap

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// StatsFile is the run's only data file. Fixed rather than configurable so the
// orchestrator can find it without being told.
const StatsFile = "stats.jsonl"

// StatsRecord is one line of stats.jsonl: one measurement window, flat, with
// the envelope keys first and the measurements after them. Flat because the
// tools that plot this are not this repo's, and a nested record makes every one
// of them write a walker.
//
// Optional pointers are the difference between "the container used no CPU" and
// "cAdvisor did not report it this window": an absent key is the second.
type StatsRecord struct {
	// Timestamp is the window's closing instant, T1. A window describes the
	// interval that ends here, so a series stamped this way never claims a
	// measurement before it was taken.
	Timestamp time.Time `json:"timestamp"`
	Tag       string    `json:"tag,omitempty"`
	TestName  string    `json:"test_name"`

	// CPULimitCores is the Envoy container's CPU limit under test. One run
	// measures one limit, so it is constant across a file and is what
	// distinguishes two files.
	CPULimitCores int     `json:"cpu_limit_cores"`
	Rung          int     `json:"rung"`
	RungQPS       float64 `json:"rung_qps"`
	// Warmup marks a window inside a rung's discarded head. Kept in the file
	// rather than dropped, so a reader can show the settling or exclude it.
	Warmup        bool    `json:"warmup"`
	WindowSeconds float64 `json:"window_seconds"`

	// OfferedQPS is the pacer's fixed schedule, not a count of what the
	// generator emitted, so a struggling generator cannot redefine the x-axis.
	OfferedQPS  float64 `json:"offered_qps"`
	AchievedQPS float64 `json:"achieved_qps"`
	SuccessQPS  float64 `json:"success_qps"`
	// Latency is timed from each request's scheduled send, so client-side
	// queueing is inside it and coordinated omission is not possible. Failures
	// and timeouts contribute their full latency rather than being dropped.
	LatencyP50Ms  float64 `json:"latency_p50_ms"`
	LatencyP95Ms  float64 `json:"latency_p95_ms"`
	LatencyMeanMs float64 `json:"latency_mean_ms"`
	InFlightMax   int64   `json:"in_flight_max"`
	// DispatchLagP95Ms is scheduled-to-wire delay: the generator measuring
	// itself. Large means that part of the curve describes the rig.
	DispatchLagP95Ms float64 `json:"dispatch_lag_p95_ms"`

	ClientConnections    int64   `json:"client_connections"`
	ClientNewConnections float64 `json:"client_new_connections"`
	// Requests per connection *opened in this window*, so it is absent — not
	// zero — when the pool opened nothing and every request rode a connection
	// that already existed. That is the healthy case, and a literal 0 here
	// would read as its opposite: keep-alive broken, a connection per request.
	ClientRequestsPerConnection *float64 `json:"client_requests_per_connection,omitempty"`

	// CPU is the mean over the window and memory the working set at its close.
	// The window is cAdvisor's, ~10s wide, so a burst shorter than that is
	// invisible here even when the latency series shows it plainly.
	EnvoyCPUCores      *float64 `json:"envoy_cpu_cores,omitempty"`
	EnvoyMemoryBytes   *float64 `json:"envoy_memory_bytes,omitempty"`
	SidecarCPUCores    *float64 `json:"sidecar_cpu_cores,omitempty"`
	SidecarMemoryBytes *float64 `json:"sidecar_memory_bytes,omitempty"`

	// The mean request split across the hops it passed through. The first four
	// sum to SpanTotalMs; SpanResumeMs is the control-plane round trip inside
	// the sidecar's handler and is not one of the four. Means throughout,
	// because percentiles do not decompose across hops. Absent when Envoy
	// reported no request times for the window.
	SpanBeforeEnvoyMs *float64 `json:"span_before_envoy_ms,omitempty"`
	SpanEnvoyMs       *float64 `json:"span_envoy_ms,omitempty"`
	SpanSidecarMs     *float64 `json:"span_sidecar_ms,omitempty"`
	SpanWorkerMs      *float64 `json:"span_worker_ms,omitempty"`
	SpanResumeMs      *float64 `json:"span_resume_ms,omitempty"`
	SpanTotalMs       *float64 `json:"span_total_ms,omitempty"`
	// SpanCountSpread is how far apart the four instruments' request counts
	// were, and SpanResolutionShare is Envoy's whole-millisecond rounding as a
	// fraction of the total. Above ~0.1 and ~0.05 respectively the split
	// should not be read closely.
	SpanCountSpread     *float64 `json:"span_count_spread,omitempty"`
	SpanResolutionShare *float64 `json:"span_resolution_share,omitempty"`

	// Guards names the rig guards that tripped on this window: the harness
	// saying it, not the router, ran out. GuardFatal marks the window that
	// ended the run.
	Guards     []string `json:"guards,omitempty"`
	GuardFatal bool     `json:"guard_fatal,omitempty"`
}

// Record flattens the window into the output row. The envelope fields the
// sample does not carry — tag and test name — come from the caller.
func (s Sample) Record(tag, testName string) StatsRecord {
	r := StatsRecord{
		Timestamp:     s.T1,
		Tag:           tag,
		TestName:      testName,
		CPULimitCores: s.CPULimitCores,
		Rung:          s.Rung,
		RungQPS:       s.RungQPS,
		Warmup:        s.Warmup,
		WindowSeconds: s.WindowSeconds,

		OfferedQPS:       s.Load.OfferedQPS,
		AchievedQPS:      s.Load.AchievedQPS,
		SuccessQPS:       s.Load.SuccessQPS,
		LatencyP50Ms:     s.Load.Latency.P50Ms,
		LatencyP95Ms:     s.Load.Latency.P95Ms,
		LatencyMeanMs:    s.Load.Latency.MeanMs,
		InFlightMax:      s.Load.InFlightMax,
		DispatchLagP95Ms: s.Load.DispatchLag.P95Ms,

		ClientConnections:    s.Client.ConnectionsInUse,
		ClientNewConnections: s.Client.NewConnections,
	}
	if s.Client.NewConnections > 0 {
		r.ClientRequestsPerConnection = ptr(s.Client.RequestsPerConnection)
	}
	if u, ok := s.Containers[RoleEnvoy]; ok {
		r.EnvoyCPUCores, r.EnvoyMemoryBytes = ptr(u.CPUCores), ptr(u.MemoryWorkingSetBytes)
	}
	if u, ok := s.Containers[RoleSidecar]; ok {
		r.SidecarCPUCores, r.SidecarMemoryBytes = ptr(u.CPUCores), ptr(u.MemoryWorkingSetBytes)
	}
	if sp := s.Spans; sp != nil && sp.Measured {
		r.SpanBeforeEnvoyMs = ptr(sp.BeforeEnvoyMs)
		r.SpanEnvoyMs = ptr(sp.EnvoyInternalMs)
		r.SpanWorkerMs = ptr(sp.WorkerMs)
		r.SpanTotalMs = ptr(sp.TotalClientMs)
		r.SpanCountSpread = ptr(sp.CountSpread)
		r.SpanResolutionShare = ptr(sp.ResolutionMsShare)
		if sp.SidecarMeasured {
			r.SpanSidecarMs, r.SpanResumeMs = ptr(sp.SidecarMs), ptr(sp.ResumeMs)
		}
	}
	for _, t := range s.Guards {
		r.Guards = append(r.Guards, string(t.Guard))
		r.GuardFatal = r.GuardFatal || t.Fatal
	}
	return r
}

func ptr(v float64) *float64 { return &v }

// Sink receives the run's records.
type Sink interface {
	Write(StatsRecord) error
}

// JSONLSink writes newline-delimited JSON. Unbuffered on purpose: a killed run
// still leaves every completed line intact and readable.
type JSONLSink struct {
	mu    sync.Mutex
	enc   *json.Encoder
	close func() error
}

// NewJSONLSink writes to w.
func NewJSONLSink(w io.Writer) *JSONLSink {
	return &JSONLSink{enc: json.NewEncoder(w)}
}

// OpenJSONLSink creates dir and stats.jsonl inside it.
func OpenJSONLSink(dir string) (*JSONLSink, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create output dir %s: %w", dir, err)
	}
	f, err := os.Create(filepath.Join(dir, StatsFile))
	if err != nil {
		return nil, err
	}
	s := NewJSONLSink(f)
	s.close = f.Close
	return s, nil
}

// Write appends one record.
func (s *JSONLSink) Write(v StatsRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(v)
}

// Close releases the underlying file.
func (s *JSONLSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.close == nil {
		return nil
	}
	err := s.close()
	s.close = nil
	return err
}

// StreamStats tags a record line as data rather than as a log line.
const StreamStats = "stats"

// streamLine wraps one record so run.sh can tell records from the binary's own
// logs. kubectl logs merges stdout and stderr into one stream, so the tag is
// what keeps them separable at the other end.
type streamLine struct {
	Stream string `json:"stream"`
	Record any    `json:"record"`
}

// StreamSink writes tagged records to a single writer. The generator is a Job
// in a distroless container, so stdout is the only channel that survives;
// run.sh unwraps the tagged lines back into the file a laptop run writes
// directly.
type StreamSink struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewStreamSink writes tagged JSONL to w.
func NewStreamSink(w io.Writer) *StreamSink {
	return &StreamSink{enc: json.NewEncoder(w)}
}

// Write emits one tagged record.
func (s *StreamSink) Write(v StatsRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(streamLine{Stream: StreamStats, Record: v})
}

// MultiSink fans each record out to every sink, and reports every failure
// rather than the first. A local file failing to write is not a reason to stop
// streaming the same record to stdout, which may be the copy that survives.
type MultiSink []Sink

// Write sends one record to every sink.
func (m MultiSink) Write(v StatsRecord) error {
	errs := make([]error, 0, len(m))
	for _, s := range m {
		errs = append(errs, s.Write(v))
	}
	return errors.Join(errs...)
}

// LadderSpec describes one sweep of offered load.
type LadderSpec struct {
	StartQPS float64 `json:"start_qps"`
	StepQPS  float64 `json:"step_qps"`
	Rungs    int     `json:"rungs"`
	// Hold is how long each rung runs, and Warmup the leading part of it
	// excluded from the summary. Warmup windows are still written.
	Hold   time.Duration `json:"hold"`
	Warmup time.Duration `json:"warmup"`
}

// Build materializes the rungs. StartAt is left zero: the pacer stamps it when
// the rung actually begins, since that depends on how long the previous rung
// took to finish dispatching.
func (l LadderSpec) Build() []Rung {
	out := make([]Rung, 0, l.Rungs)
	for i := 0; i < l.Rungs; i++ {
		out = append(out, Rung{
			Index:   i,
			RateQPS: l.StartQPS + float64(i)*l.StepQPS,
			Hold:    l.Hold,
			Warmup:  l.Warmup,
		})
	}
	return out
}

// PeakQPS is the top rung's rate, which is what the in-flight cap and the
// connection pool have to be sized against.
func (l LadderSpec) PeakQPS() float64 {
	if l.Rungs <= 0 {
		return 0
	}
	return l.StartQPS + float64(l.Rungs-1)*l.StepQPS
}
