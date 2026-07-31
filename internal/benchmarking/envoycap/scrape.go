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
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Metric names read off the router pod. Both endpoints live on the pod but
// neither is in the atenet-router Service, so they are reached by pod IP,
// injected as an env var by the driver rather than added to the manifest —
// the deployment is measured as shipped.
const (
	// forwardProxyCluster is the Envoy cluster that carries every request to a
	// worker pod. Its connection counters are the rig's canary: with
	// max_requests_per_connection: 1 each request opens a fresh TCP connection,
	// so port exhaustion shows up here first.
	forwardProxyCluster = "dynamic_forward_proxy_cluster"

	// extProcCluster is the Envoy cluster carrying the ext_proc callout to the
	// router sidecar — one gRPC stream per request. The router's xDS never
	// configures circuit breakers on it (cmd/atenet/internal/router/xds.go), so
	// it runs Envoy's defaults, and the one that binds first is
	// max_requests: 1024 on the default priority: at most 1024 callouts may be
	// in flight at once. Concurrency is offered-rate times callout latency, so
	// the cap is reached at 1024/L — around 2000 QPS once a callout takes half a
	// second. Requests refused by it are counted in upstream_rq_pending_overflow.
	extProcCluster = "ate-cluster"

	// ExtProcMaxRequests is Envoy's default max_requests for a cluster with no
	// circuit_breakers block, which is what ate-cluster runs as shipped. Carried
	// here so the report can state headroom against it rather than leave the
	// reader to know the default.
	ExtProcMaxRequests = 1024.0

	// ingressHCM is the HTTP connection manager prefix for the listener under
	// test; the admin endpoint also reports one for "admin", which must not be
	// mixed in.
	ingressHCM = "ingress_http"

	envoyRqTimeMetric   = "envoy_http_downstream_rq_time"
	routeDurationMetric = "atenet_router_route_duration_seconds"

	// upstreamRqTimeMetric and upstreamCxConnectMetric split the worker hop.
	//
	// The layer decomposition stops one level short without them: subtracting
	// route.duration from downstream_rq_time leaves "Envoy proxying plus the
	// worker hop" as a single opaque term, and on this path that term is where
	// the tail lives. upstream_rq_time is the hop; upstream_cx_connect_ms is the
	// TCP handshake inside it, which max_requests_per_connection: 1 makes every
	// request pay. Carrying both says whether a slow hop was a slow worker or a
	// slow connect, and those have different fixes.
	//
	// Both are per-cluster histograms in milliseconds.
	upstreamRqTimeMetric    = "envoy_cluster_upstream_rq_time"
	upstreamCxConnectMetric = "envoy_cluster_upstream_cx_connect_ms"

	// sidecarCPUMetric is scraped from the router's own :9090 endpoint, so it is
	// the Go process's CPU — the atenet-router container, not the envoy
	// container beside it in the same pod. Envoy publishes no equivalent on its
	// admin endpoint, so Envoy's CPU comes from Cloud Monitoring instead. The
	// name says sidecar because an earlier writeup read this number as Envoy's
	// and drew a conclusion from it that the number does not support.
	sidecarCPUMetric = "process_cpu_seconds_total"

	// concurrencyMetric is Envoy's worker-thread count. Unset --concurrency
	// means Envoy sizes it from the node's CPU count, so this reads back what
	// the node shape actually bought and belongs in the run record next to the
	// machine type.
	concurrencyMetric = "envoy_server_concurrency"
)

// watchdogCounters distinguish "Envoy could not get on the CPU" from "Envoy was
// on the CPU and the work took that long", which is the difference between a
// contended cluster and a real capacity number.
//
// Envoy's watchdog ticks each worker's event loop. A miss is a loop that went
// >200ms without a tick, a mega_miss >1s. Both mean the thread was starved or
// blocked. Crucially, neither fires for work that is merely *queued* behind a
// healthy loop — zero misses rules out starvation, and rules out nothing else.
//
// Counted with no label filter so the per-worker series sum into a whole-proxy
// total; the per-worker breakdown is not what a capacity report needs.
var watchdogCounters = []string{
	"envoy_server_worker_watchdog_miss",
	"envoy_server_worker_watchdog_mega_miss",
	"envoy_server_main_thread_watchdog_miss",
	"envoy_server_main_thread_watchdog_mega_miss",
}

// envoyCounters are the Envoy counters carried per step. Anything absent from
// a scrape is simply left out rather than reported as zero — "we did not see
// it" and "it was zero" are different claims.
var envoyCounters = []string{
	"envoy_cluster_upstream_cx_connect_fail",
	"envoy_cluster_upstream_cx_overflow",
	"envoy_cluster_upstream_cx_connect_timeout",
	"envoy_cluster_upstream_rq_pending_overflow",
	"envoy_cluster_upstream_cx_total",
	"envoy_cluster_upstream_rq_total",
	"envoy_cluster_upstream_rq_timeout",
}

// extProcCounters are cumulative counters on the ext_proc callout cluster,
// carried as per-step deltas. upstream_rq_pending_overflow is the one that
// records a max_requests rejection.
var extProcCounters = []string{
	"envoy_cluster_upstream_rq_pending_overflow",
	"envoy_cluster_upstream_rq_total",
	"envoy_cluster_upstream_rq_timeout",
	"envoy_cluster_upstream_cx_connect_fail",
}

// extProcGauges are instantaneous readings on the ext_proc callout cluster.
// Unlike the counters these are not differenced -- a gauge delta is
// meaningless. They are sampled repeatedly *while the step's load is applied*
// and reduced to their peak: a gauge read after the load stops is necessarily
// zero, which would read back as proof of headroom against max_requests when
// in fact nothing was measured.
var extProcGauges = []string{
	"envoy_cluster_upstream_rq_active",
	"envoy_cluster_upstream_cx_active",
	"envoy_cluster_circuit_breakers_default_rq_open",
	"envoy_cluster_circuit_breakers_default_cx_open",
}

// Snapshot is one read of the router pod's two metrics endpoints.
type Snapshot struct {
	At time.Time
	// EnvoyCounters holds the forward-proxy cluster counters present in the
	// scrape, keyed by metric name with the envoy_cluster_ prefix stripped.
	EnvoyCounters map[string]float64
	// ExtProcCounters and ExtProcGauges hold the same for the ext_proc callout
	// cluster, whose default max_requests: 1024 is a candidate binding
	// constraint on the path being measured.
	ExtProcCounters map[string]float64
	ExtProcGauges   map[string]float64
	// EnvoyRqTime is Envoy's own end-to-end latency histogram for the ingress
	// listener, in milliseconds.
	EnvoyRqTime *Histogram
	// RouteDuration is the ext_proc handler's histogram (ResumeActor plus
	// bookkeeping), in seconds.
	RouteDuration *Histogram
	// UpstreamRqTime and UpstreamCxConnect are the forward-proxy cluster's own
	// histograms — the worker hop, and the TCP handshake inside it — in
	// milliseconds.
	UpstreamRqTime    *Histogram
	UpstreamCxConnect *Histogram
	// SidecarCPUSeconds is the atenet-router *container's* cumulative CPU time.
	// It is not Envoy's; see sidecarCPUMetric.
	SidecarCPUSeconds float64
	// Watchdog holds Envoy's cumulative watchdog miss counters, keyed with the
	// envoy_server_ prefix stripped, and differenced per step by the caller.
	Watchdog map[string]float64
	// Concurrency is Envoy's worker-thread count, or 0 if it was not scraped.
	Concurrency float64
	// Errs records endpoints that could not be read. A failed scrape must not
	// fail the run, but it must not be invisible either.
	Errs []string
}

// Scraper reads the router pod's Envoy admin and Go metrics endpoints.
// Either URL may be empty, which disables that half.
type Scraper struct {
	Client           *http.Client
	EnvoyAdminURL    string
	RouterMetricsURL string
}

// NewScraper builds a Scraper for a router pod IP. An empty podIP yields a
// scraper that reads nothing, so the ladder still runs (without the layer
// decomposition) when the driver could not resolve the pod.
func NewScraper(podIP string) *Scraper {
	s := &Scraper{Client: &http.Client{Timeout: 5 * time.Second}}
	if podIP == "" {
		return s
	}
	s.EnvoyAdminURL = fmt.Sprintf("http://%s:9901/stats/prometheus", podIP)
	s.RouterMetricsURL = fmt.Sprintf("http://%s:9090/metrics", podIP)
	return s
}

// Enabled reports whether this scraper will read anything.
func (s *Scraper) Enabled() bool {
	return s.EnvoyAdminURL != "" || s.RouterMetricsURL != ""
}

// Scrape reads both endpoints. It never returns an error: a step whose scrape
// failed still reports its client-side latency, with the failure recorded in
// Errs.
func (s *Scraper) Scrape(ctx context.Context) Snapshot {
	snap := Snapshot{
		At:              time.Now(),
		EnvoyCounters:   map[string]float64{},
		ExtProcCounters: map[string]float64{},
		ExtProcGauges:   map[string]float64{},
		Watchdog:        map[string]float64{},
	}

	if s.EnvoyAdminURL != "" {
		families, err := s.fetch(ctx, s.EnvoyAdminURL)
		if err != nil {
			snap.Errs = append(snap.Errs, "envoy admin: "+err.Error())
		} else {
			collectCluster(families, envoyCounters, forwardProxyCluster, snap.EnvoyCounters)
			collectCluster(families, extProcCounters, extProcCluster, snap.ExtProcCounters)
			collectCluster(families, extProcGauges, extProcCluster, snap.ExtProcGauges)
			snap.EnvoyRqTime = histogramValue(families[envoyRqTimeMetric],
				map[string]string{"envoy_http_conn_manager_prefix": ingressHCM})
			fwd := map[string]string{"envoy_cluster_name": forwardProxyCluster}
			snap.UpstreamRqTime = histogramValue(families[upstreamRqTimeMetric], fwd)
			snap.UpstreamCxConnect = histogramValue(families[upstreamCxConnectMetric], fwd)
			for _, name := range watchdogCounters {
				if v, ok := counterValue(families[name], nil); ok {
					snap.Watchdog[trimEnvoyServerPrefix(name)] = v
				}
			}
			if v, ok := counterValue(families[concurrencyMetric], nil); ok {
				snap.Concurrency = v
			}
		}
	}

	if s.RouterMetricsURL != "" {
		families, err := s.fetch(ctx, s.RouterMetricsURL)
		if err != nil {
			snap.Errs = append(snap.Errs, "router metrics: "+err.Error())
		} else {
			snap.RouteDuration = histogramValue(families[routeDurationMetric], nil)
			if v, ok := counterValue(families[sidecarCPUMetric], nil); ok {
				snap.SidecarCPUSeconds = v
			}
		}
	}
	return snap
}

// ScrapeExtProcGauges reads only the ext_proc cluster's gauges. Separate from
// Scrape because it runs on a tight poll while the step is under load, where
// the histograms and the router endpoint are neither wanted nor cheap.
//
// Returns nil if the endpoint could not be read; the caller keeps a peak across
// samples, and a failed sample must not enter it as a zero.
func (s *Scraper) ScrapeExtProcGauges(ctx context.Context) map[string]float64 {
	if s.EnvoyAdminURL == "" {
		return nil
	}
	families, err := s.fetch(ctx, s.EnvoyAdminURL)
	if err != nil {
		return nil
	}
	out := map[string]float64{}
	collectCluster(families, extProcGauges, extProcCluster, out)
	return out
}

func (s *Scraper) fetch(ctx context.Context, url string) (map[string]*dto.MetricFamily, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	// The zero TextParser is invalid; it must be told how to validate names.
	// UTF8Validation is the library's own default and accepts everything Envoy
	// and the Go client emit.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	return parser.TextToMetricFamilies(resp.Body)
}

// collectCluster reads the named metrics for one Envoy cluster into dst, keyed
// with the envoy_cluster_ prefix stripped. A metric absent from the scrape is
// left out rather than stored as zero: "we did not see it" and "it was zero"
// are different claims.
func collectCluster(families map[string]*dto.MetricFamily, names []string, cluster string, dst map[string]float64) {
	for _, name := range names {
		if v, ok := counterValue(families[name], map[string]string{"envoy_cluster_name": cluster}); ok {
			dst[trimEnvoyClusterPrefix(name)] = v
		}
	}
}

func trimEnvoyClusterPrefix(name string) string {
	return trimPrefix(name, "envoy_cluster_")
}

func trimEnvoyServerPrefix(name string) string {
	return trimPrefix(name, "envoy_server_")
}

func trimPrefix(name, prefix string) string {
	if len(name) > len(prefix) && name[:len(prefix)] == prefix {
		return name[len(prefix):]
	}
	return name
}

// matches reports whether m carries every label in want. A nil or empty want
// matches anything, which is how the single-series router metrics are read.
func matches(m *dto.Metric, want map[string]string) bool {
	for k, v := range want {
		found := false
		for _, lp := range m.GetLabel() {
			if lp.GetName() == k {
				if lp.GetValue() != v {
					return false
				}
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// counterValue sums the matching series of a counter or gauge family.
func counterValue(fam *dto.MetricFamily, want map[string]string) (float64, bool) {
	if fam == nil {
		return 0, false
	}
	total, found := 0.0, false
	for _, m := range fam.GetMetric() {
		if !matches(m, want) {
			continue
		}
		switch {
		case m.GetCounter() != nil:
			total += m.GetCounter().GetValue()
		case m.GetGauge() != nil:
			total += m.GetGauge().GetValue()
		case m.GetUntyped() != nil:
			total += m.GetUntyped().GetValue()
		default:
			continue
		}
		found = true
	}
	return total, found
}

// histogramValue sums the matching series of a histogram family into one
// histogram. Summing is correct here because the series being merged are label
// partitions of the same latency population (the router's route.duration
// splits by outcome, for instance) and we want the aggregate.
func histogramValue(fam *dto.MetricFamily, want map[string]string) *Histogram {
	if fam == nil {
		return nil
	}
	cumulative := map[float64]float64{}
	out := &Histogram{}
	any := false
	for _, m := range fam.GetMetric() {
		h := m.GetHistogram()
		if h == nil || !matches(m, want) {
			continue
		}
		any = true
		out.Count += float64(h.GetSampleCount())
		out.Sum += h.GetSampleSum()
		for _, b := range h.GetBucket() {
			ub := b.GetUpperBound()
			if math.IsInf(ub, 1) {
				continue
			}
			cumulative[ub] += float64(b.GetCumulativeCount())
		}
	}
	if !any || len(cumulative) == 0 {
		return nil
	}
	bounds := make([]float64, 0, len(cumulative))
	for ub := range cumulative {
		bounds = append(bounds, ub)
	}
	sort.Float64s(bounds)
	out.Bounds = bounds
	out.Cumulative = make([]float64, len(bounds))
	for i, ub := range bounds {
		out.Cumulative[i] = cumulative[ub]
	}
	return out
}
