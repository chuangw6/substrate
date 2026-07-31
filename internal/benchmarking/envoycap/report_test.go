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
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func step(qps, p95 float64) StepReport {
	return StepReport{OfferedQPS: qps, Quantiles: Quantiles{P95: p95}}
}

func TestP95CrossingQPS(t *testing.T) {
	tests := []struct {
		name  string
		steps []StepReport
		want  float64
	}{
		{
			name: "never crosses",
			steps: []StepReport{
				step(250, 4), step(500, 6), step(750, 9), step(1000, 14),
			},
			want: 0,
		},
		{
			name: "crosses exactly on a rung",
			steps: []StepReport{
				step(250, 4), step(500, 500),
			},
			want: 500,
		},
		{
			// Log-space interpolation: log(500) is halfway between log(100)
			// and log(2500), so the crossing is halfway between the rungs.
			name: "interpolates in log-latency space",
			steps: []StepReport{
				step(1000, 100), step(1250, 2500),
			},
			want: 1125,
		},
		{
			name:  "first rung is already over budget",
			steps: []StepReport{step(250, 900), step(500, 1800)},
			want:  250,
		},
		{name: "no steps", steps: nil, want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := P95CrossingQPS(tc.steps, BudgetMS)
			if math.Abs(got-tc.want) > 1e-6 {
				t.Errorf("P95CrossingQPS = %g, want %g", got, tc.want)
			}
		})
	}
}

// TestReportJSONIsFramedAndParsable is the contract between the Job's stdout
// and the driver script that slices the report out of it.
func TestReportJSONIsFramedAndParsable(t *testing.T) {
	r := &Report{
		Run: RunInfo{Cluster: "substrate-envoycap", Caveats: Caveats},
		Steps: []StepReport{{
			Repeat: 1, Step: 1, OfferedQPS: 250, AchievedQPS: 249.7,
			Count: 5493, OK: 5490, Fail: 3,
			FailByClass: map[string]int{"http_503": 2, "timeout": 1},
			Quantiles:   Quantiles{P50: 3.9, P95: 8.2, P99: 14.1, Max: 91},
			Envoy:       EnvoyStep{Counters: map[string]float64{"upstream_cx_connect_fail": 0}},
		}},
	}

	var buf bytes.Buffer
	if err := r.WriteJSON(&buf); err != nil {
		t.Fatalf("WriteJSON returned unexpected error: %v", err)
	}
	out := buf.String()

	begin := strings.Index(out, JSONBeginSentinel)
	end := strings.Index(out, JSONEndSentinel)
	if begin < 0 || end < 0 || end < begin {
		t.Fatalf("report is not framed by the sentinels:\n%s", out)
	}
	body := out[begin+len(JSONBeginSentinel) : end]

	var back Report
	if err := json.Unmarshal([]byte(body), &back); err != nil {
		t.Fatalf("framed body is not valid JSON: %v", err)
	}
	if back.Run.Cluster != "substrate-envoycap" {
		t.Errorf("round-tripped cluster = %q, want substrate-envoycap", back.Run.Cluster)
	}
	if len(back.Steps) != 1 || back.Steps[0].P95 != 8.2 {
		t.Errorf("round-tripped steps = %+v, want one step with p95 8.2", back.Steps)
	}

	// The embedded Quantiles must flatten, because that is the shape the chart
	// script reads.
	var raw map[string]any
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		t.Fatalf("framed body is not a JSON object: %v", err)
	}
	first := raw["steps"].([]any)[0].(map[string]any)
	for _, key := range []string{"p50_ms", "p95_ms", "p99_ms", "max_ms", "offered_qps", "achieved_qps"} {
		if _, ok := first[key]; !ok {
			t.Errorf("step object has no %q key; the chart script reads these directly", key)
		}
	}
}

// TestPtrEncodesUnmeasuredAsNull keeps "we could not measure it" distinct from
// "it was zero" in the output.
func TestPtrEncodesUnmeasuredAsNull(t *testing.T) {
	tests := []struct {
		name string
		in   float64
		want string
	}{
		{name: "a real value survives", in: 2.5, want: "2.5"},
		{name: "zero is a real value", in: 0, want: "0"},
		{name: "NaN becomes null", in: math.NaN(), want: "null"},
		{name: "infinity becomes null", in: math.Inf(1), want: "null"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(ptr(tc.in))
			if err != nil {
				t.Fatalf("marshal returned unexpected error: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("ptr(%v) marshalled to %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestWriteTableCoversEveryStep(t *testing.T) {
	r := &Report{Steps: []StepReport{
		{Repeat: 1, Step: 1, OfferedQPS: 250, Quantiles: Quantiles{P95: 8.2}},
		{Repeat: 1, Step: 2, OfferedQPS: 500, Quantiles: Quantiles{P95: 640.1}, RigLimited: true,
			RigNotes: []string{"per-worker-pod-IP offered rate too high"}},
	}}
	var buf bytes.Buffer
	if err := r.WriteTable(&buf); err != nil {
		t.Fatalf("WriteTable returned unexpected error: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"offered", "250", "500", "8.2", "640.1", "LIMITED"} {
		if !strings.Contains(out, want) {
			t.Errorf("table is missing %q:\n%s", want, out)
		}
	}
	// Unavailable server-side numbers render as "-", not as 0.0.
	if !strings.Contains(out, "-") {
		t.Errorf("table does not mark unmeasured columns:\n%s", out)
	}
}

func TestRigBannerNamesTheRig(t *testing.T) {
	banner := RigBanner([]string{"dispatch lag p99 120.0 ms exceeds 50.0 ms"})
	for _, want := range []string{"RIG LIMIT REACHED", "not the system under test", "dispatch lag p99"} {
		if !strings.Contains(banner, want) {
			t.Errorf("banner is missing %q:\n%s", want, banner)
		}
	}
}
