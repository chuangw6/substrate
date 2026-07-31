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
	"strings"
	"testing"
	"time"
)

func TestGuardInputsEvaluate(t *testing.T) {
	// The intended operating point: 2000 QPS spread over 40 worker pod IPs is
	// 50 connections/s per IP, 8x under the ephemeral-port ceiling.
	clean := GuardInputs{
		OfferedQPS:        2000,
		DistinctWorkerIPs: 40,
		DispatchLagP99MS:  0.4,
		LoadgenCores:      2.1,
		GOMAXPROCS:        8,
		EnvoyDeltas:       map[string]float64{"upstream_cx_connect_fail": 0, "upstream_cx_overflow": 0},
	}

	tests := []struct {
		name    string
		mutate  func(*GuardInputs)
		wantSub string
	}{
		{name: "clean rig trips nothing", mutate: func(*GuardInputs) {}},
		{
			name:    "too few worker IPs for the rate",
			mutate:  func(g *GuardInputs) { g.DistinctWorkerIPs = 2 },
			wantSub: "ephemeral-port exhaustion",
		},
		{
			name:    "dispatcher falling behind",
			mutate:  func(g *GuardInputs) { g.DispatchLagP99MS = 120 },
			wantSub: "dispatch lag p99",
		},
		{
			name:    "load generator out of CPU",
			mutate:  func(g *GuardInputs) { g.LoadgenCores = 7.5 },
			wantSub: "load generator CPU",
		},
		{
			name:    "envoy could not get an upstream connection",
			mutate:  func(g *GuardInputs) { g.EnvoyDeltas["upstream_cx_connect_fail"] = 12 },
			wantSub: "upstream_cx_connect_fail",
		},
		{
			name:    "envoy connection pool overflowed",
			mutate:  func(g *GuardInputs) { g.EnvoyDeltas["upstream_cx_overflow"] = 3 },
			wantSub: "upstream_cx_overflow",
		},
		{
			name:   "unknown loadgen CPU does not trip the CPU guard",
			mutate: func(g *GuardInputs) { g.LoadgenCores = math.NaN() },
		},
		{
			name:   "exactly at the per-IP ceiling does not trip",
			mutate: func(g *GuardInputs) { g.OfferedQPS, g.DistinctWorkerIPs = MaxPerWorkerIPRPS, 1 },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := clean
			g.EnvoyDeltas = map[string]float64{}
			for k, v := range clean.EnvoyDeltas {
				g.EnvoyDeltas[k] = v
			}
			tc.mutate(&g)

			notes := g.Evaluate()
			if tc.wantSub == "" {
				if len(notes) != 0 {
					t.Fatalf("Evaluate tripped unexpectedly: %v", notes)
				}
				return
			}
			if len(notes) == 0 {
				t.Fatalf("Evaluate returned no notes, want one mentioning %q", tc.wantSub)
			}
			joined := strings.Join(notes, "\n")
			if !strings.Contains(joined, tc.wantSub) {
				t.Errorf("Evaluate notes = %q, want one containing %q", joined, tc.wantSub)
			}
		})
	}
}

func TestGuardInputsRigExhausted(t *testing.T) {
	// The distinction the whole abort decision rests on: a dispatcher that fell
	// behind a *constrained* server is reporting that server's latency, and
	// aborting there would stop the ladder at exactly the saturated steps the
	// curve exists to show. Anything else that trips is the rig, constrained
	// server or not.
	tests := []struct {
		name        string
		g           GuardInputs
		constrained bool
		want        bool
	}{
		{
			name: "clean rig is not exhausted",
			g:    GuardInputs{OfferedQPS: 2000, DistinctWorkerIPs: 40, LoadgenCores: 2, GOMAXPROCS: 8},
		},
		{
			name: "dispatch lag against a healthy system is the rig",
			g: GuardInputs{OfferedQPS: 2000, DistinctWorkerIPs: 40, LoadgenCores: 2, GOMAXPROCS: 8,
				DispatchLagP99MS: 13294},
			want: true,
		},
		{
			name: "dispatch lag against a constrained system is not",
			g: GuardInputs{OfferedQPS: 2000, DistinctWorkerIPs: 40, LoadgenCores: 2, GOMAXPROCS: 8,
				DispatchLagP99MS: 13294},
			constrained: true,
		},
		{
			name: "out of CPU is the rig even on a constrained step",
			g: GuardInputs{OfferedQPS: 2000, DistinctWorkerIPs: 40, LoadgenCores: 7.5, GOMAXPROCS: 8,
				DispatchLagP99MS: 13294},
			constrained: true,
			want:        true,
		},
		{
			name: "envoy unable to connect upstream is the rig even on a constrained step",
			g: GuardInputs{OfferedQPS: 2000, DistinctWorkerIPs: 40, LoadgenCores: 2, GOMAXPROCS: 8,
				EnvoyDeltas: map[string]float64{"upstream_cx_connect_fail": 12}},
			constrained: true,
			want:        true,
		},
		{
			name:        "per-worker-IP ceiling is the rig even on a constrained step",
			g:           GuardInputs{OfferedQPS: 2000, DistinctWorkerIPs: 2, LoadgenCores: 2, GOMAXPROCS: 8},
			constrained: true,
			want:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.g.RigExhausted(tc.constrained); got != tc.want {
				t.Errorf("RigExhausted(%v) = %v, want %v (notes: %v)",
					tc.constrained, got, tc.want, tc.g.Evaluate())
			}
			// A step that is not aborting still has to say what happened.
			if tc.g.DispatchLagP99MS > MaxDispatchLagP99MS && len(tc.g.Evaluate()) == 0 {
				t.Error("Evaluate dropped the dispatch-lag note; a non-fatal note must still be recorded")
			}
		})
	}
}

func TestInFlightBudgetCoversTheTimeout(t *testing.T) {
	// The pool has to hold a full timeout's worth of requests, or the open loop
	// silently becomes a closed one as soon as latency exceeds pool/rate.
	tests := []struct {
		name   string
		maxQPS float64
		want   int
	}{
		{name: "sized at rate times the timeout", maxQPS: 1000, want: 10000},
		{name: "clamped at the socket ceiling", maxQPS: 4000, want: maxInFlight},
		{name: "never zero", maxQPS: 0, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := inFlightBudget(tc.maxQPS); got != tc.want {
				t.Errorf("inFlightBudget(%v) = %d, want %d", tc.maxQPS, got, tc.want)
			}
		})
	}
}

func TestGuardInputsConstraints(t *testing.T) {
	// Constraints judge the system under test, so unlike Evaluate a trip here
	// must never be treated as a reason to stop measuring.
	tests := []struct {
		name     string
		deltas   map[string]float64
		gauges   map[string]float64
		watchdog map[string]float64
		wantSub  string
	}{
		{
			name:   "ext_proc well inside the callout cap",
			deltas: map[string]float64{"upstream_rq_pending_overflow": 0},
			gauges: map[string]float64{"upstream_rq_active": 120},
		},
		{
			name:    "ext_proc callouts rejected by max_requests",
			deltas:  map[string]float64{"upstream_rq_pending_overflow": 57},
			gauges:  map[string]float64{"upstream_rq_active": ExtProcMaxRequests},
			wantSub: "upstream_rq_pending_overflow",
		},
		{
			name:    "ext_proc circuit breaker open",
			gauges:  map[string]float64{"circuit_breakers_default_rq_open": 1},
			wantSub: "circuit breaker open",
		},
		{
			name:    "ext_proc concurrency approaching the cap",
			gauges:  map[string]float64{"upstream_rq_active": ExtProcMaxRequests * 0.9},
			wantSub: "approaching the callout cap",
		},
		{
			name:   "just under the near-limit threshold stays quiet",
			gauges: map[string]float64{"upstream_rq_active": ExtProcMaxRequests * ExtProcNearLimit},
		},
		{
			name: "no ext_proc scrape reports nothing rather than zero",
		},
		{
			name:     "a watchdog miss on a dedicated node is worth saying out loud",
			watchdog: map[string]float64{"worker_watchdog_miss": 3},
			wantSub:  "Envoy watchdog recorded 3 miss",
		},
		{
			name:     "a mega miss counts even with no plain misses",
			watchdog: map[string]float64{"main_thread_watchdog_mega_miss": 1},
			wantSub:  "1 mega_miss",
		},
		{
			name:     "watchdog scraped and clean stays quiet",
			watchdog: map[string]float64{"worker_watchdog_miss": 0, "worker_watchdog_mega_miss": 0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := GuardInputs{ExtProcDeltas: tc.deltas, ExtProcGauges: tc.gauges, WatchdogDeltas: tc.watchdog}
			notes := g.Constraints()
			if tc.wantSub == "" {
				if len(notes) != 0 {
					t.Fatalf("Constraints tripped unexpectedly: %v", notes)
				}
				return
			}
			joined := strings.Join(notes, "\n")
			if !strings.Contains(joined, tc.wantSub) {
				t.Errorf("Constraints notes = %q, want one containing %q", joined, tc.wantSub)
			}
		})
	}
}

func TestExtProcHeadroomIsNaNWithoutAScrape(t *testing.T) {
	// NaN, not 0: "we did not measure it" and "it was idle" must not look the
	// same, or a missing scrape reads as proof of headroom.
	g := GuardInputs{ExtProcGauges: map[string]float64{}}
	if got := g.ExtProcHeadroom(); !math.IsNaN(got) {
		t.Errorf("ExtProcHeadroom() = %v, want NaN", got)
	}

	g = GuardInputs{ExtProcGauges: map[string]float64{"upstream_rq_active": 512}}
	if got, want := g.ExtProcHeadroom(), 0.5; got != want {
		t.Errorf("ExtProcHeadroom() = %v, want %v", got, want)
	}
}

func TestPerWorkerIPRPS(t *testing.T) {
	tests := []struct {
		name string
		qps  float64
		ips  int
		want float64
	}{
		{name: "default operating point", qps: 2000, ips: 40, want: 50},
		{name: "all load on one pod", qps: 500, ips: 1, want: 500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GuardInputs{OfferedQPS: tc.qps, DistinctWorkerIPs: tc.ips}.PerWorkerIPRPS()
			if got != tc.want {
				t.Errorf("PerWorkerIPRPS = %g, want %g", got, tc.want)
			}
		})
	}
	if got := (GuardInputs{OfferedQPS: 100, DistinctWorkerIPs: 0}).PerWorkerIPRPS(); !math.IsNaN(got) {
		t.Errorf("PerWorkerIPRPS with no known worker IPs = %g, want NaN", got)
	}
}

func TestSingleflightCollapse(t *testing.T) {
	tests := []struct {
		name   string
		qps    float64
		actors int
		resume time.Duration
		want   float64
	}{
		{
			// 1000 QPS over 40 actors is 25/s per actor; a 3 ms resume gives
			// load 0.075, so ~7% of RPCs ride along with another.
			name: "1000 QPS, 40 actors, 3 ms resume",
			qps:  1000, actors: 40, resume: 3 * time.Millisecond,
			want: 0.075 / 1.075,
		},
		{
			// Halving the pool doubles the per-actor rate and the collapse.
			name: "same rate over 20 actors collapses more",
			qps:  1000, actors: 20, resume: 3 * time.Millisecond,
			want: 0.15 / 1.15,
		},
		{
			name: "an instant resume never collapses",
			qps:  2000, actors: 40, resume: 0,
			want: math.NaN(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SingleflightCollapse(tc.qps, tc.actors, tc.resume)
			if math.IsNaN(tc.want) {
				if !math.IsNaN(got) {
					t.Fatalf("SingleflightCollapse = %g, want NaN", got)
				}
				return
			}
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("SingleflightCollapse = %g, want %g", got, tc.want)
			}
		})
	}
}

func TestCPUMeterReportsPositiveUsage(t *testing.T) {
	m := NewCPUMeter()
	// Burn a little CPU so the interval has something in it.
	deadline := time.Now().Add(20 * time.Millisecond)
	x := 0
	for time.Now().Before(deadline) {
		x++
	}
	_ = x

	cores, gomaxprocs := m.Sample()
	if gomaxprocs < 1 {
		t.Fatalf("GOMAXPROCS = %d, want >= 1", gomaxprocs)
	}
	if math.IsNaN(cores) {
		t.Fatal("CPUMeter.Sample returned NaN; process CPU accounting is unavailable")
	}
	if cores < 0 {
		t.Errorf("CPUMeter.Sample = %g cores, want >= 0", cores)
	}
}
