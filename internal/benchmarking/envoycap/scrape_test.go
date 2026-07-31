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
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// envoyStats is a trimmed excerpt of a real
// http://<router-pod-ip>:9901/stats/prometheus response. It deliberately
// includes the "admin" connection manager alongside "ingress_http", and a
// second cluster alongside dynamic_forward_proxy_cluster, because mixing
// either in would produce a plausible wrong answer.
const envoyStats = `# TYPE envoy_cluster_upstream_cx_connect_fail counter
envoy_cluster_upstream_cx_connect_fail{envoy_cluster_name="dynamic_forward_proxy_cluster"} 4
envoy_cluster_upstream_cx_connect_fail{envoy_cluster_name="xds_cluster"} 99
# TYPE envoy_cluster_upstream_cx_overflow counter
envoy_cluster_upstream_cx_overflow{envoy_cluster_name="dynamic_forward_proxy_cluster"} 0
# TYPE envoy_cluster_upstream_cx_total counter
envoy_cluster_upstream_cx_total{envoy_cluster_name="dynamic_forward_proxy_cluster"} 1000
# TYPE envoy_cluster_upstream_rq_total counter
envoy_cluster_upstream_rq_total{envoy_cluster_name="dynamic_forward_proxy_cluster"} 1000
# TYPE envoy_http_downstream_rq_time histogram
envoy_http_downstream_rq_time_bucket{envoy_http_conn_manager_prefix="ingress_http",le="1"} 0
envoy_http_downstream_rq_time_bucket{envoy_http_conn_manager_prefix="ingress_http",le="5"} 50
envoy_http_downstream_rq_time_bucket{envoy_http_conn_manager_prefix="ingress_http",le="10"} 90
envoy_http_downstream_rq_time_bucket{envoy_http_conn_manager_prefix="ingress_http",le="25"} 100
envoy_http_downstream_rq_time_bucket{envoy_http_conn_manager_prefix="ingress_http",le="+Inf"} 100
envoy_http_downstream_rq_time_sum{envoy_http_conn_manager_prefix="ingress_http"} 600
envoy_http_downstream_rq_time_count{envoy_http_conn_manager_prefix="ingress_http"} 100
envoy_http_downstream_rq_time_bucket{envoy_http_conn_manager_prefix="admin",le="1"} 7
envoy_http_downstream_rq_time_bucket{envoy_http_conn_manager_prefix="admin",le="+Inf"} 7
envoy_http_downstream_rq_time_sum{envoy_http_conn_manager_prefix="admin"} 3
envoy_http_downstream_rq_time_count{envoy_http_conn_manager_prefix="admin"} 7
# TYPE envoy_cluster_upstream_rq_time histogram
envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="dynamic_forward_proxy_cluster",le="1"} 20
envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="dynamic_forward_proxy_cluster",le="5"} 180
envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="dynamic_forward_proxy_cluster",le="+Inf"} 200
envoy_cluster_upstream_rq_time_sum{envoy_cluster_name="dynamic_forward_proxy_cluster"} 700
envoy_cluster_upstream_rq_time_count{envoy_cluster_name="dynamic_forward_proxy_cluster"} 200
envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="xds_cluster",le="1"} 3
envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="xds_cluster",le="+Inf"} 3
envoy_cluster_upstream_rq_time_sum{envoy_cluster_name="xds_cluster"} 1
envoy_cluster_upstream_rq_time_count{envoy_cluster_name="xds_cluster"} 3
# TYPE envoy_cluster_upstream_cx_connect_ms histogram
envoy_cluster_upstream_cx_connect_ms_bucket{envoy_cluster_name="dynamic_forward_proxy_cluster",le="1"} 150
envoy_cluster_upstream_cx_connect_ms_bucket{envoy_cluster_name="dynamic_forward_proxy_cluster",le="+Inf"} 200
envoy_cluster_upstream_cx_connect_ms_sum{envoy_cluster_name="dynamic_forward_proxy_cluster"} 300
envoy_cluster_upstream_cx_connect_ms_count{envoy_cluster_name="dynamic_forward_proxy_cluster"} 200
# TYPE envoy_server_worker_watchdog_miss counter
envoy_server_worker_watchdog_miss{envoy_worker_id="0"} 4
envoy_server_worker_watchdog_miss{envoy_worker_id="1"} 5
# TYPE envoy_server_worker_watchdog_mega_miss counter
envoy_server_worker_watchdog_mega_miss{envoy_worker_id="0"} 0
envoy_server_worker_watchdog_mega_miss{envoy_worker_id="1"} 0
# TYPE envoy_server_main_thread_watchdog_miss counter
envoy_server_main_thread_watchdog_miss 2
# TYPE envoy_server_concurrency gauge
envoy_server_concurrency 88
`

// routerStats is a trimmed excerpt of http://<router-pod-ip>:9090/metrics.
// The route-duration histogram is in seconds and split by outcome.
const routerStats = `# TYPE atenet_router_route_duration_seconds histogram
atenet_router_route_duration_seconds_bucket{outcome="ok",le="0.001"} 10
atenet_router_route_duration_seconds_bucket{outcome="ok",le="0.005"} 80
atenet_router_route_duration_seconds_bucket{outcome="ok",le="0.01"} 100
atenet_router_route_duration_seconds_bucket{outcome="ok",le="+Inf"} 100
atenet_router_route_duration_seconds_sum{outcome="ok"} 0.4
atenet_router_route_duration_seconds_count{outcome="ok"} 100
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 123.5
`

func TestScraperReadsBothEndpoints(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/stats/prometheus":
			_, _ = w.Write([]byte(envoyStats))
		case "/metrics":
			_, _ = w.Write([]byte(routerStats))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	s := &Scraper{
		Client:           srv.Client(),
		EnvoyAdminURL:    srv.URL + "/stats/prometheus",
		RouterMetricsURL: srv.URL + "/metrics",
	}
	snap := s.Scrape(context.Background())
	if len(snap.Errs) != 0 {
		t.Fatalf("Scrape reported errors: %v", snap.Errs)
	}

	// Only the forward-proxy cluster's series, not xds_cluster's.
	if got := snap.EnvoyCounters["upstream_cx_connect_fail"]; got != 4 {
		t.Errorf("upstream_cx_connect_fail = %g, want 4 (xds_cluster must not be summed in)", got)
	}
	if got := snap.EnvoyCounters["upstream_cx_total"]; got != 1000 {
		t.Errorf("upstream_cx_total = %g, want 1000", got)
	}
	if _, ok := snap.EnvoyCounters["upstream_rq_timeout"]; ok {
		t.Error("upstream_rq_timeout is absent from the scrape but was reported; absent must not read as zero")
	}

	// Only the ingress listener's histogram, not the admin one.
	if snap.EnvoyRqTime == nil {
		t.Fatal("EnvoyRqTime is nil, want the ingress_http histogram")
	}
	if snap.EnvoyRqTime.Count != 100 {
		t.Errorf("EnvoyRqTime.Count = %g, want 100 (the admin listener must not be mixed in)", snap.EnvoyRqTime.Count)
	}

	// The forward-proxy hop, and only its cluster: xds_cluster carries the same
	// metric name and summing it in would quietly shift every quantile.
	if snap.UpstreamRqTime == nil {
		t.Fatal("UpstreamRqTime is nil, want the forward-proxy histogram")
	}
	if snap.UpstreamRqTime.Count != 200 {
		t.Errorf("UpstreamRqTime.Count = %g, want 200 (xds_cluster must not be summed in)", snap.UpstreamRqTime.Count)
	}
	if snap.UpstreamCxConnect == nil {
		t.Fatal("UpstreamCxConnect is nil, want the forward-proxy handshake histogram")
	}
	if snap.UpstreamCxConnect.Count != 200 {
		t.Errorf("UpstreamCxConnect.Count = %g, want 200", snap.UpstreamCxConnect.Count)
	}

	if snap.RouteDuration == nil {
		t.Fatal("RouteDuration is nil, want the route-duration histogram")
	}
	if snap.RouteDuration.Count != 100 {
		t.Errorf("RouteDuration.Count = %g, want 100", snap.RouteDuration.Count)
	}
	if snap.SidecarCPUSeconds != 123.5 {
		t.Errorf("SidecarCPUSeconds = %g, want 123.5", snap.SidecarCPUSeconds)
	}

	// Watchdog misses are summed across worker threads: the report wants "did
	// any event loop stall", not which one.
	if got := snap.Watchdog["worker_watchdog_miss"]; got != 9 {
		t.Errorf("worker_watchdog_miss = %g, want 9 (4+5 across both workers)", got)
	}
	if got := snap.Watchdog["worker_watchdog_mega_miss"]; got != 0 {
		t.Errorf("worker_watchdog_mega_miss = %g, want 0", got)
	}
	if got := snap.Watchdog["main_thread_watchdog_miss"]; got != 2 {
		t.Errorf("main_thread_watchdog_miss = %g, want 2", got)
	}
	if snap.Concurrency != 88 {
		t.Errorf("Concurrency = %g, want 88", snap.Concurrency)
	}
}

func TestScrapeFailureIsRecordedNotFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	s := &Scraper{
		Client:           srv.Client(),
		EnvoyAdminURL:    srv.URL + "/stats/prometheus",
		RouterMetricsURL: srv.URL + "/metrics",
	}
	snap := s.Scrape(context.Background())
	if len(snap.Errs) != 2 {
		t.Fatalf("Scrape recorded %d errors, want 2: %v", len(snap.Errs), snap.Errs)
	}
	for _, e := range snap.Errs {
		if !strings.Contains(e, "503") {
			t.Errorf("scrape error %q does not name the status code", e)
		}
	}
}

func TestNewScraperWithoutPodIPIsDisabled(t *testing.T) {
	s := NewScraper("")
	if s.Enabled() {
		t.Error("NewScraper(\"\") is enabled, want a scraper that reads nothing")
	}
	snap := s.Scrape(context.Background())
	if len(snap.Errs) != 0 || len(snap.EnvoyCounters) != 0 {
		t.Errorf("disabled scraper produced %+v, want an empty snapshot", snap)
	}
}

func TestHistogramSubAndQuantile(t *testing.T) {
	before := &Histogram{
		Bounds:     []float64{1, 5, 10},
		Cumulative: []float64{10, 10, 10},
		Count:      10, Sum: 5,
	}
	after := &Histogram{
		Bounds:     []float64{1, 5, 10},
		Cumulative: []float64{10, 60, 110},
		Count:      110, Sum: 405,
	}

	delta := after.Sub(before)
	if delta == nil {
		t.Fatal("Sub returned nil for matching bucket bounds")
	}
	if delta.Count != 100 {
		t.Errorf("delta Count = %g, want 100", delta.Count)
	}
	// 100 observations in the step: 0 at or below 1 ms, 50 at or below 5 ms,
	// 100 at or below 10 ms. p50 lands exactly on the 5 ms bound.
	if got := delta.Quantile(0.50); math.Abs(got-5) > 1e-9 {
		t.Errorf("delta p50 = %g, want 5", got)
	}
	// p95 is 95/100, four fifths of the way through the 5..10 bucket.
	if got := delta.Quantile(0.95); math.Abs(got-9.5) > 1e-9 {
		t.Errorf("delta p95 = %g, want 9.5", got)
	}
	if got := delta.Mean(); math.Abs(got-4) > 1e-9 {
		t.Errorf("delta mean = %g, want 4", got)
	}
}

func TestHistogramSubRejectsMismatchedBounds(t *testing.T) {
	a := &Histogram{Bounds: []float64{1, 5}, Cumulative: []float64{1, 2}, Count: 2}
	b := &Histogram{Bounds: []float64{1, 10}, Cumulative: []float64{1, 2}, Count: 2}
	if got := b.Sub(a); got != nil {
		t.Errorf("Sub across different bucket bounds = %+v, want nil rather than a wrong answer", got)
	}
	if got := a.Sub(nil); got != a {
		t.Error("Sub against no earlier snapshot should return the histogram unchanged")
	}
}

func TestHistogramScaleConvertsSecondsToMilliseconds(t *testing.T) {
	h := &Histogram{
		Bounds:     []float64{0.001, 0.005, 0.01},
		Cumulative: []float64{10, 80, 100},
		Count:      100, Sum: 0.4,
	}
	ms := h.Scale(1000)
	if ms.Bounds[1] != 5 {
		t.Errorf("scaled bound = %g, want 5", ms.Bounds[1])
	}
	if got := ms.Mean(); math.Abs(got-4) > 1e-9 {
		t.Errorf("scaled mean = %g ms, want 4", got)
	}
	// The original must be untouched; snapshots get reused across steps.
	if h.Bounds[1] != 0.005 {
		t.Errorf("Scale mutated its receiver: bound = %g", h.Bounds[1])
	}
}

func TestHistogramQuantileOnEmpty(t *testing.T) {
	var h *Histogram
	if got := h.Quantile(0.95); !math.IsNaN(got) {
		t.Errorf("nil histogram p95 = %g, want NaN", got)
	}
	if got := (&Histogram{}).Mean(); !math.IsNaN(got) {
		t.Errorf("empty histogram mean = %g, want NaN", got)
	}
}

func TestScraperRespectsContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second)
		_, _ = w.Write([]byte(envoyStats))
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &Scraper{Client: srv.Client(), EnvoyAdminURL: srv.URL + "/stats/prometheus"}
	snap := s.Scrape(ctx)
	if len(snap.Errs) == 0 {
		t.Error("Scrape with a cancelled context recorded no error")
	}
}

func TestScrapeExtProcGaugesReadsTheCalloutCluster(t *testing.T) {
	// The gauge poll runs on its own, separate from the full scrape, so it has
	// to pick the right cluster by itself. ate-cluster is the ext_proc callout;
	// the others in the same family are not.
	const stats = `# TYPE envoy_cluster_upstream_rq_active gauge
envoy_cluster_upstream_rq_active{envoy_cluster_name="ate-cluster"} 640
envoy_cluster_upstream_rq_active{envoy_cluster_name="dynamic_forward_proxy_cluster"} 12
envoy_cluster_upstream_rq_active{envoy_cluster_name="xds_cluster"} 1
# TYPE envoy_cluster_circuit_breakers_default_rq_open gauge
envoy_cluster_circuit_breakers_default_rq_open{envoy_cluster_name="ate-cluster"} 0
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(stats))
	}))
	defer srv.Close()

	s := &Scraper{Client: srv.Client(), EnvoyAdminURL: srv.URL}
	got := s.ScrapeExtProcGauges(context.Background())
	if got["upstream_rq_active"] != 640 {
		t.Errorf("upstream_rq_active = %v, want 640 (ate-cluster only)", got["upstream_rq_active"])
	}
	// Present and zero must survive as zero, not vanish: the circuit breaker
	// being closed is a real reading.
	if v, ok := got["circuit_breakers_default_rq_open"]; !ok || v != 0 {
		t.Errorf("circuit_breakers_default_rq_open = %v (present=%v), want 0 present", v, ok)
	}
}

func TestScrapeExtProcGaugesReturnsNilOnAFailedRead(t *testing.T) {
	// nil, not an empty map: a failed sample must not enter a peak as a zero.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	s := &Scraper{Client: srv.Client(), EnvoyAdminURL: srv.URL}
	if got := s.ScrapeExtProcGauges(context.Background()); got != nil {
		t.Errorf("ScrapeExtProcGauges on HTTP 503 = %v, want nil", got)
	}
	if got := (&Scraper{}).ScrapeExtProcGauges(context.Background()); got != nil {
		t.Errorf("ScrapeExtProcGauges with no admin URL = %v, want nil", got)
	}
}

func TestMergePeakKeepsTheHighWaterMark(t *testing.T) {
	// Concurrency against a cap is a level, so what matters across a step is
	// the highest sample, not the last one -- the last one is taken as the load
	// is easing off.
	var peaks map[string]float64
	for _, sample := range []map[string]float64{
		{"upstream_rq_active": 100, "circuit_breakers_default_rq_open": 0},
		{"upstream_rq_active": 900, "circuit_breakers_default_rq_open": 1},
		{"upstream_rq_active": 5, "circuit_breakers_default_rq_open": 0},
	} {
		peaks = mergePeak(peaks, sample)
	}
	if peaks["upstream_rq_active"] != 900 {
		t.Errorf("peak upstream_rq_active = %v, want 900", peaks["upstream_rq_active"])
	}
	if peaks["circuit_breakers_default_rq_open"] != 1 {
		t.Errorf("peak circuit_breakers_default_rq_open = %v, want 1 (it opened at some point)",
			peaks["circuit_breakers_default_rq_open"])
	}
}

func TestMergePeakRecordsASampledZero(t *testing.T) {
	// "Sampled, and idle" is a real result and must be distinguishable from
	// "never sampled", which stays nil.
	peaks := mergePeak(nil, map[string]float64{"upstream_rq_active": 0})
	if v, ok := peaks["upstream_rq_active"]; !ok || v != 0 {
		t.Errorf("peaks = %v, want a present zero", peaks)
	}
	if got := mergePeak(nil, nil); len(got) != 0 {
		t.Errorf("mergePeak(nil, nil) = %v, want empty", got)
	}
}
