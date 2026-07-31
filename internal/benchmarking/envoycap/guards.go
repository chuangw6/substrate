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
	"fmt"
	"math"
	"runtime"
	"syscall"
	"time"
)

// Rig guard thresholds. These bound the *test rig*, not the system under test.
// "The rig ran out" and "the system ran out" are different answers and must
// not be reported the same way, so tripping one of these aborts the run rather
// than producing a number.
const (
	// MaxPerWorkerIPRPS caps the request rate to any single worker pod IP.
	// Envoy opens a fresh TCP connection per request to the worker
	// (max_requests_per_connection: 1), and the ephemeral port space is per
	// destination IP: roughly 28k ports over a 60 s TIME_WAIT is ~470
	// connections/s. 400 is the safe ceiling under that.
	MaxPerWorkerIPRPS = 400.0

	// MaxDispatchLagP99MS caps how late the client's own dispatcher may be.
	// Beyond this the load generator, not the router, is shaping arrivals.
	MaxDispatchLagP99MS = 50.0

	// MaxLoadgenCPUFraction caps load generator CPU as a fraction of
	// GOMAXPROCS. Set well above the pacer's own spin cost (~0.3 of a core at
	// the top of the ladder) so it fires on real saturation, not on the pacer.
	MaxLoadgenCPUFraction = 0.80
)

// CPUMeter reports the process's own CPU use between calls.
//
// Reads the kernel's accounting for this process rather than the node's, which
// is the question the guard actually asks: is the load generator itself the
// thing that ran out?
type CPUMeter struct {
	lastCPU  time.Duration
	lastWall time.Time
}

// NewCPUMeter starts a measurement interval at the current instant.
func NewCPUMeter() *CPUMeter {
	m := &CPUMeter{}
	m.lastCPU, _ = processCPU()
	m.lastWall = time.Now()
	return m
}

// Sample closes the interval opened by the previous call and returns the mean
// number of cores used over it, along with GOMAXPROCS.
func (m *CPUMeter) Sample() (cores float64, gomaxprocs int) {
	now := time.Now()
	cpu, err := processCPU()
	wall := now.Sub(m.lastWall)
	prevCPU, prevWall := m.lastCPU, m.lastWall
	m.lastCPU, m.lastWall = cpu, now
	_ = prevWall

	gomaxprocs = runtime.GOMAXPROCS(0)
	if err != nil || wall <= 0 {
		return math.NaN(), gomaxprocs
	}
	return (cpu - prevCPU).Seconds() / wall.Seconds(), gomaxprocs
}

func processCPU() (time.Duration, error) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, err
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), nil
}

// GuardInputs are the per-step measurements the rig guards judge.
type GuardInputs struct {
	OfferedQPS        float64
	DistinctWorkerIPs int
	DispatchLagP99MS  float64
	LoadgenCores      float64
	GOMAXPROCS        int
	// EnvoyDeltas are the forward-proxy cluster counter deltas for this step,
	// keyed as in Snapshot.EnvoyCounters.
	EnvoyDeltas map[string]float64
	// ExtProcDeltas are the ext_proc callout cluster's counter deltas over the
	// step, and ExtProcGauges the peak of its gauges while the load was
	// applied. These judge the system under test, not the rig, so they feed
	// Constraints rather than Evaluate.
	ExtProcDeltas map[string]float64
	ExtProcGauges map[string]float64
	// WatchdogDeltas are Envoy's watchdog miss counters over the step, summed
	// across worker threads and keyed as in Snapshot.Watchdog.
	WatchdogDeltas map[string]float64
}

// watchdogMisses totals stalled-event-loop events over the step: misses are
// loops that went >200ms without a tick, mega misses >1s.
func (g GuardInputs) watchdogMisses() (miss, mega float64) {
	return g.WatchdogDeltas["worker_watchdog_miss"] + g.WatchdogDeltas["main_thread_watchdog_miss"],
		g.WatchdogDeltas["worker_watchdog_mega_miss"] + g.WatchdogDeltas["main_thread_watchdog_mega_miss"]
}

// ExtProcHeadroom is peak ext_proc concurrency during the step as a fraction of
// Envoy's default max_requests. Returns NaN when the gauge was never sampled,
// which is not the same as sampling it and finding it idle.
func (g GuardInputs) ExtProcHeadroom() float64 {
	active, ok := g.ExtProcGauges["upstream_rq_active"]
	if !ok {
		return math.NaN()
	}
	return active / ExtProcMaxRequests
}

// ExtProcNearLimit is the fraction of max_requests past which the ext_proc
// circuit breaker is close enough to matter that the step should say so.
const ExtProcNearLimit = 0.80

// Constraints returns notes about the *system under test* reaching a limit.
//
// These are deliberately not rig guards: a rig note aborts the run, because a
// number produced by an exhausted load generator is worthless. An ext_proc
// circuit breaker opening is the opposite — it is a real, as-shipped property
// of the thing being measured, and quite possibly the answer to "which layer
// bound first". The ladder must run on so the shape above it is visible.
func (g GuardInputs) Constraints() []string {
	var notes []string

	if v, ok := g.ExtProcDeltas["upstream_rq_pending_overflow"]; ok && v > 0 {
		notes = append(notes, fmt.Sprintf(
			"ext_proc cluster %q rejected %.0f callouts this step (upstream_rq_pending_overflow): Envoy's default max_requests: %.0f is binding, and it is a configuration limit rather than exhausted CPU",
			extProcCluster, v, ExtProcMaxRequests))
	}
	if v, ok := g.ExtProcGauges["circuit_breakers_default_rq_open"]; ok && v > 0 {
		notes = append(notes, fmt.Sprintf(
			"ext_proc cluster %q had its max_requests circuit breaker open during this step", extProcCluster))
	}
	if frac := g.ExtProcHeadroom(); !math.IsNaN(frac) && frac > ExtProcNearLimit {
		notes = append(notes, fmt.Sprintf(
			"peak ext_proc concurrency %.0f of max_requests %.0f (%.0f%%) during this step: approaching the callout cap",
			g.ExtProcGauges["upstream_rq_active"], ExtProcMaxRequests, frac*100))
	}
	// A watchdog miss says an Envoy event loop went un-ticked: either the thread
	// could not get on a CPU, or something on it blocked. On a node provisioned
	// for this measurement the first is supposed to be impossible, so a miss
	// here is either a broken assumption about the node or a genuine stall
	// inside Envoy -- and both change how the step's latency should be read.
	// Note it and keep going; the ladder's shape is still the deliverable.
	if miss, mega := g.watchdogMisses(); miss > 0 || mega > 0 {
		notes = append(notes, fmt.Sprintf(
			"Envoy watchdog recorded %.0f miss and %.0f mega_miss this step: an event loop went >200ms (>1s for mega) without a tick, so the thread was starved of CPU or blocked on something",
			miss, mega))
	}
	return notes
}

// PerWorkerIPRPS is the offered rate per distinct worker pod IP. Round-robin
// over the pool makes this exact rather than an estimate.
func (g GuardInputs) PerWorkerIPRPS() float64 {
	if g.DistinctWorkerIPs < 1 {
		return math.NaN()
	}
	return g.OfferedQPS / float64(g.DistinctWorkerIPs)
}

// fatalEnvoyCounters are counters whose value should be exactly zero on a
// healthy rig. A non-zero connect-fail or overflow means Envoy could not get a
// connection to a worker, which is the shape port exhaustion takes from the
// proxy's side.
var fatalEnvoyCounters = []string{
	"upstream_cx_connect_fail",
	"upstream_cx_overflow",
	"upstream_cx_connect_timeout",
	"upstream_rq_pending_overflow",
}

// Evaluate returns one note per tripped guard, empty if the rig is clean.
func (g GuardInputs) Evaluate() []string {
	notes := g.exhaustionNotes()
	if n := g.dispatchLagNote(); n != "" {
		notes = append(notes, n)
	}
	return notes
}

// RigExhausted reports whether the tripped guards mean the *rig* ran out, which
// is the only thing that justifies cutting the ladder short.
//
// Dispatch lag is the one guard whose trip can be an effect rather than a
// cause. In an open loop with a bounded worker pool, a server slow enough to
// tie up every worker forces the dispatcher to block, and the lag that follows
// says nothing about the load generator's own health -- it is the server's
// latency, arriving by a different route. Aborting there would stop the ladder
// at exactly the saturated steps the curve is for, and would report "the rig
// ran out" about a step where the system under test had already opened a
// circuit breaker.
//
// So: dispatch lag alone, on a step where the system under test is
// demonstrably constrained, is recorded and the ladder runs on. Every other
// guard -- per-worker-IP connection rate, load generator CPU, Envoy unable to
// get an upstream connection -- is unambiguous rig exhaustion and still aborts,
// including on a constrained step.
func (g GuardInputs) RigExhausted(systemConstrained bool) bool {
	if len(g.exhaustionNotes()) > 0 {
		return true
	}
	return !systemConstrained && g.dispatchLagNote() != ""
}

// dispatchLagNote fires when the client's dispatcher is late. Whether that is
// the rig's fault is RigExhausted's question, not this one's.
func (g GuardInputs) dispatchLagNote() string {
	if g.DispatchLagP99MS <= MaxDispatchLagP99MS {
		return ""
	}
	return fmt.Sprintf(
		"dispatch lag p99 %.1f ms exceeds %.1f ms: arrivals were shaped by the load generator, not offered on schedule",
		g.DispatchLagP99MS, MaxDispatchLagP99MS)
}

// exhaustionNotes are the guards that mean the rig itself ran out of something,
// with no reading under which the system under test is responsible.
func (g GuardInputs) exhaustionNotes() []string {
	var notes []string

	if rps := g.PerWorkerIPRPS(); rps > MaxPerWorkerIPRPS {
		notes = append(notes, fmt.Sprintf(
			"per-worker-pod-IP offered rate %.0f/s exceeds %.0f/s: with one fresh TCP connection per request, the rig is near ephemeral-port exhaustion",
			rps, MaxPerWorkerIPRPS))
	}
	if !math.IsNaN(g.LoadgenCores) && g.GOMAXPROCS > 0 {
		if frac := g.LoadgenCores / float64(g.GOMAXPROCS); frac > MaxLoadgenCPUFraction {
			notes = append(notes, fmt.Sprintf(
				"load generator CPU %.0f%% of GOMAXPROCS=%d exceeds %.0f%%",
				frac*100, g.GOMAXPROCS, MaxLoadgenCPUFraction*100))
		}
	}
	for _, name := range fatalEnvoyCounters {
		if v, ok := g.EnvoyDeltas[name]; ok && v > 0 {
			notes = append(notes, fmt.Sprintf(
				"envoy %s went up by %.0f during this step: the proxy could not get an upstream connection",
				name, v))
		}
	}
	return notes
}

// SingleflightCollapse estimates the fraction of pings whose ResumeActor RPC
// was collapsed into another request's.
//
// atenet's resumer wraps the call in singleflight keyed on the actor, so
// concurrent requests to the *same* actor share one RPC. That flatters the
// control-plane path, and it grows as the system slows, so it is reported
// per step rather than assumed away. With per-actor arrival rate lambda and
// resume duration L, the expected share is lambda*L/(1+lambda*L).
//
// Returns NaN when the resume duration is unknown (no router scrape).
func SingleflightCollapse(offeredQPS float64, actors int, resume time.Duration) float64 {
	if actors < 1 || resume <= 0 || math.IsNaN(offeredQPS) {
		return math.NaN()
	}
	load := (offeredQPS / float64(actors)) * resume.Seconds()
	return load / (1 + load)
}
