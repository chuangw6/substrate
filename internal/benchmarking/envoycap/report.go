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
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"text/tabwriter"
)

// Sentinels framing the machine-readable report on stdout. The Job's log is
// the only channel out of the cluster that needs no Workload Identity binding,
// no object-storage dependency, and no shell in the image; the driver script
// slices the report out from between these.
const (
	JSONBeginSentinel = "===ENVOYCAP-JSON-BEGIN==="
	JSONEndSentinel   = "===ENVOYCAP-JSON-END==="
)

// Caveats are the as-shipped conditions that were in force for the whole
// measurement. They are carried in the report rather than left to the writeup
// because a number without them is not reproducible.
var Caveats = []string{
	"envoy --component-log-level upstream:debug,router:debug,ext_proc:debug (as shipped)",
	"envoy StdoutAccessLog enabled: one access-log line per request (as shipped)",
	"OTLP tracing at 100% RandomSampling (as shipped)",
	"dynamic_forward_proxy_cluster max_requests_per_connection: 1, so Envoy opens a fresh TCP connection to the worker pod per request (as shipped)",
	"ate-cluster (the ext_proc callout) has no circuit_breakers block, so it runs Envoy's default max_requests: 1024 — at most 1024 concurrent callouts (as shipped)",
	"no CPU requests on either atenet-router container (as shipped)",
}

// Flags records the six experiment knobs, so a run directory is
// self-describing.
type Flags struct {
	Actors       int     `json:"actors"`
	StartQPS     float64 `json:"start_qps"`
	MaxQPS       float64 `json:"max_qps"`
	Steps        int     `json:"steps"`
	StepDuration string  `json:"step_duration"`
	Repeat       int     `json:"repeat"`
}

// RunInfo is the run-level header of the report.
type RunInfo struct {
	Cluster           string   `json:"cluster"`
	GitSHA            string   `json:"git_sha"`
	Image             string   `json:"image"`
	StartedAt         string   `json:"started_at"`
	FinishedAt        string   `json:"finished_at"`
	Flags             Flags    `json:"flags"`
	Atespace          string   `json:"atespace"`
	RouterURL         string   `json:"router_url"`
	RouterPodIP       string   `json:"router_pod_ip"`
	RouterNode        string   `json:"router_node"`
	LoadgenNode       string   `json:"loadgen_node"`
	GOMAXPROCS        int      `json:"gomaxprocs"`
	WarmupSeconds     float64  `json:"warmup_s"`
	WorkerGoroutines  int      `json:"worker_goroutines"`
	Actors            []Actor  `json:"actors"`
	DistinctWorkerIPs int      `json:"distinct_worker_ips"`
	Caveats           []string `json:"caveats"`
	// EnvoyConcurrency is Envoy's worker-thread count, which with --concurrency
	// unset is the node's CPU count. It belongs in the run header because it is
	// how the node's size reaches Envoy at all, and because a capacity number
	// means something different at 8 threads than at 88. Zero means the admin
	// endpoint was not readable, not that Envoy had no workers.
	EnvoyConcurrency int `json:"envoy_concurrency"`
	// Aborted is set when a rig guard tripped. A run that aborted still emits
	// every step it completed, including the one that tripped — the point is
	// to say plainly that the rig, not the system, is what ran out.
	Aborted     bool   `json:"aborted"`
	AbortReason string `json:"abort_reason,omitempty"`
}

// EnvoyStep is Envoy's own view of one step, as counter deltas and histogram
// quantiles over that step alone.
type EnvoyStep struct {
	Counters              map[string]float64 `json:"counters"`
	DownstreamRqTimeP50MS *float64           `json:"downstream_rq_time_p50_ms"`
	DownstreamRqTimeP95MS *float64           `json:"downstream_rq_time_p95_ms"`
	DownstreamRqTimeP99MS *float64           `json:"downstream_rq_time_p99_ms"`
	// UpstreamRqPerCx is upstream_rq_total/upstream_cx_total for the step. A
	// value near 1.0 confirms max_requests_per_connection: 1 is in force.
	UpstreamRqPerCx *float64 `json:"upstream_rq_per_cx"`

	// The forward-proxy hop, split. UpstreamRqTime is Envoy's time from
	// dispatching to the worker pod to the worker's last byte;
	// UpstreamCxConnect is the TCP handshake inside it, which
	// max_requests_per_connection: 1 makes every request pay.
	//
	// Without these, "client minus route.duration" is a single term covering
	// Envoy, the network, the handshake and the worker, and a tail anywhere in
	// it looks the same. With them a slow step says which.
	UpstreamRqTimeP50MS    *float64 `json:"upstream_rq_time_p50_ms"`
	UpstreamRqTimeP95MS    *float64 `json:"upstream_rq_time_p95_ms"`
	UpstreamRqTimeP99MS    *float64 `json:"upstream_rq_time_p99_ms"`
	UpstreamCxConnectP50MS *float64 `json:"upstream_cx_connect_p50_ms"`
	UpstreamCxConnectP95MS *float64 `json:"upstream_cx_connect_p95_ms"`
	UpstreamCxConnectP99MS *float64 `json:"upstream_cx_connect_p99_ms"`
}

// ExtProcStep is the ext_proc callout cluster's view of one step. Counters are
// deltas over the step; Gauges are the *peak* of repeated samples taken while
// the step's load was applied, since a level read after the load stops is
// always zero.
type ExtProcStep struct {
	Counters map[string]float64 `json:"counters"`
	Gauges   map[string]float64 `json:"peak_gauges"`
	// GaugeSamples is how many readings the peaks came from. Zero means the
	// gauges were never read, and Gauges is null rather than a set of zeroes:
	// "not measured" must not read as "measured, and idle".
	GaugeSamples int `json:"gauge_samples"`
	// MaxRequests is Envoy's default, restated per step so a reader of one
	// record does not have to know it. ActiveFraction is peak
	// upstream_rq_active against it: the headroom on the callout path.
	MaxRequests    float64  `json:"max_requests"`
	ActiveFraction *float64 `json:"active_fraction"`
}

// StepReport is one rung of one pass.
type StepReport struct {
	Repeat      int     `json:"repeat"`
	Step        int     `json:"step"`
	OfferedQPS  float64 `json:"offered_qps"`
	AchievedQPS float64 `json:"achieved_qps"`
	WarmupS     float64 `json:"warmup_s"`
	MeasuredS   float64 `json:"measured_s"`

	Count       int            `json:"count"`
	OK          int            `json:"ok"`
	Fail        int            `json:"fail"`
	FailByClass map[string]int `json:"fail_by_class"`

	// Quantiles is embedded, so it flattens to p50_ms/p95_ms/p99_ms/max_ms.
	// Computed from every request in the measurement window, failures
	// included: dropping them would flatter the numbers exactly when the
	// system is in trouble.
	Quantiles

	DispatchLagP99MS float64  `json:"dispatch_lag_p99_ms"`
	LoadgenCores     *float64 `json:"loadgen_cores_used"`
	GOMAXPROCS       int      `json:"gomaxprocs"`
	PerWorkerIPRPS   float64  `json:"per_worker_ip_rps"`

	SingleflightCollapse *float64 `json:"singleflight_collapse_estimate"`

	Envoy              EnvoyStep   `json:"envoy"`
	ExtProc            ExtProcStep `json:"envoy_extproc"`
	RouteDurationP50MS *float64    `json:"route_duration_p50_ms"`
	RouteDurationP95MS *float64    `json:"route_duration_p95_ms"`
	// SidecarCPUCores is the atenet-router *container's* CPU over the step, not
	// Envoy's. The field used to be called router_cpu_cores and was read as
	// Envoy's; it never was. Envoy publishes no CPU counter on its admin
	// endpoint, so Envoy's CPU comes from Cloud Monitoring after the fact.
	SidecarCPUCores *float64 `json:"router_sidecar_cpu_cores"`

	// Watchdog holds per-step deltas of Envoy's watchdog miss counters, summed
	// over all worker threads. Non-zero means an event loop was starved or
	// blocked; zero rules starvation out but says nothing about work queued
	// behind a healthy loop.
	Watchdog map[string]float64 `json:"envoy_watchdog"`

	RigLimited bool     `json:"rig_limited"`
	RigNotes   []string `json:"rig_notes"`
	// ConstraintNotes record the system under test hitting a limit, as opposed
	// to the rig hitting one. They never abort the run.
	ConstraintNotes []string `json:"constraint_notes,omitempty"`
	ScrapeErrs      []string `json:"scrape_errors,omitempty"`
}

// Report is the whole run: what the charts read. The chart script never parses
// logs.
type Report struct {
	Run   RunInfo      `json:"run"`
	Steps []StepReport `json:"steps"`
}

// WriteJSON emits the report between the framing sentinels.
func (r *Report) WriteJSON(w io.Writer) error {
	body, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "%s\n%s\n%s\n", JSONBeginSentinel, body, JSONEndSentinel)
	return err
}

// WriteTable prints the human-readable step table.
func (r *Report) WriteTable(w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "pass\tstep\toffered\tachieved\tn\tfail\tp50 ms\tp95 ms\tp99 ms\tmax ms\tenvoy p95\troute p95\thop p95\tconn p99\tlag p99\tlg cores\txproc pk\twatchdog\trig")
	for _, s := range r.Steps {
		// "LIMITED" is the rig having run out, which aborts the run. "lagging"
		// is a rig note that did not: the dispatcher fell behind a server that
		// was already constrained, so the step's offered rate was not actually
		// offered on schedule and its latencies are a floor.
		rig := ""
		switch {
		case s.RigLimited:
			rig = "LIMITED"
		case len(s.RigNotes) > 0:
			rig = "lagging"
		}
		// The ext_proc column is peak concurrency during the step against
		// Envoy's default max_requests: 1024, where a callout-path ceiling
		// would show up. "-" means it was never sampled, not that it was idle.
		extproc := "-"
		if s.ExtProc.ActiveFraction != nil {
			extproc = fmt.Sprintf("%.0f%%", *s.ExtProc.ActiveFraction*100)
		}
		if len(s.ConstraintNotes) > 0 {
			extproc += "*"
		}
		// miss/mega over the step, summed across Envoy's worker threads. Both
		// zero is the reading that rules out CPU starvation of the proxy, so it
		// belongs on the table and not only in the JSON.
		watchdog := "-"
		if len(s.Watchdog) > 0 {
			watchdog = fmt.Sprintf("%.0f/%.0f",
				s.Watchdog["worker_watchdog_miss"]+s.Watchdog["main_thread_watchdog_miss"],
				s.Watchdog["worker_watchdog_mega_miss"]+s.Watchdog["main_thread_watchdog_mega_miss"])
		}
		fmt.Fprintf(tw, "%d\t%d\t%.0f\t%.0f\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%s\t%s\t%s\t%s\t%.1f\t%s\t%s\t%s\t%s\n",
			s.Repeat, s.Step, s.OfferedQPS, s.AchievedQPS, s.Count, s.Fail,
			s.P50, s.P95, s.P99, s.Max,
			fmtPtr(s.Envoy.DownstreamRqTimeP95MS), fmtPtr(s.RouteDurationP95MS),
			fmtPtr(s.Envoy.UpstreamRqTimeP95MS), fmtPtr(s.Envoy.UpstreamCxConnectP99MS),
			s.DispatchLagP99MS, fmtPtr(s.LoadgenCores), extproc, watchdog, rig)
	}
	return tw.Flush()
}

func fmtPtr(v *float64) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf("%.1f", *v)
}

// ptr converts a float to a JSON-safe pointer: NaN and Inf, which
// encoding/json refuses, become null. "We could not measure it" and "it was
// zero" must not look the same in the output.
func ptr(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

// P95CrossingQPS interpolates the offered rate at which p95 latency crosses
// budgetMS, in log-latency space, for one pass. It returns 0 when the pass
// never crosses.
//
// This is the answer the whole measurement exists to produce, so it is
// computed here from the same data the charts draw rather than read off a
// chart by eye.
func P95CrossingQPS(steps []StepReport, budgetMS float64) float64 {
	var prev *StepReport
	for i := range steps {
		s := &steps[i]
		if s.P95 >= budgetMS {
			if prev == nil || prev.P95 <= 0 || s.P95 <= 0 {
				return s.OfferedQPS
			}
			lo, hi := math.Log(prev.P95), math.Log(s.P95)
			if hi == lo {
				return s.OfferedQPS
			}
			t := (math.Log(budgetMS) - lo) / (hi - lo)
			return prev.OfferedQPS + t*(s.OfferedQPS-prev.OfferedQPS)
		}
		prev = s
	}
	return 0
}

// RigBanner is the loud, unmissable notice printed when a rig guard trips.
func RigBanner(notes []string) string {
	var b strings.Builder
	line := strings.Repeat("!", 78)
	b.WriteString(line + "\n")
	b.WriteString("!! RIG LIMIT REACHED - ABORTING\n")
	b.WriteString("!! The test rig ran out, not the system under test.\n")
	b.WriteString("!! The numbers up to this step stand; this step and anything beyond it\n")
	b.WriteString("!! measure the load generator.\n")
	for _, n := range notes {
		b.WriteString("!!   - " + n + "\n")
	}
	b.WriteString(line + "\n")
	return b.String()
}
