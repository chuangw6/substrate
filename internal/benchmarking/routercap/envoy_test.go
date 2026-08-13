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

// Tests for Envoy and sidecar stats parsing and for the counter deltas.

package routercap

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// envoyFixture renders an admin /stats/prometheus payload in Envoy's own
// shape: HELP/TYPE headers, no per-sample timestamps, the cluster name carried
// in a label, and an unrelated cluster present so label routing is exercised.
func envoyFixture(cxTotal, rqTotal, cxActive float64) string {
	var b strings.Builder
	fmt.Fprintf(&b, `# TYPE envoy_server_concurrency gauge
envoy_server_concurrency{} 40
# TYPE envoy_server_memory_allocated gauge
envoy_server_memory_allocated{} 104857600
envoy_server_memory_heap_size{} 209715200
envoy_server_total_connections{} 512
# TYPE envoy_http_downstream_cx_active gauge
envoy_http_downstream_cx_active{envoy_http_conn_manager_prefix="ingress_http"} 128
envoy_http_downstream_rq_total{envoy_http_conn_manager_prefix="ingress_http"} 900000
# TYPE envoy_cluster_upstream_cx_total counter
envoy_cluster_upstream_cx_total{envoy_cluster_name="actor_original_dst"} %.0f
envoy_cluster_upstream_rq_total{envoy_cluster_name="actor_original_dst"} %.0f
envoy_cluster_upstream_cx_active{envoy_cluster_name="actor_original_dst"} %.0f
envoy_cluster_upstream_rq_active{envoy_cluster_name="actor_original_dst"} 310
envoy_cluster_upstream_rq_pending_active{envoy_cluster_name="actor_original_dst"} 4
envoy_cluster_upstream_cx_overflow{envoy_cluster_name="actor_original_dst"} 7
envoy_cluster_upstream_cx_connect_fail{envoy_cluster_name="actor_original_dst"} 2
envoy_cluster_upstream_cx_connect_timeout{envoy_cluster_name="actor_original_dst"} 1
envoy_cluster_upstream_rq_timeout{envoy_cluster_name="actor_original_dst"} 3
envoy_cluster_upstream_rq_retry{envoy_cluster_name="actor_original_dst"} 5
envoy_cluster_upstream_rq_pending_overflow{envoy_cluster_name="actor_original_dst"} 9
envoy_cluster_circuit_breakers_default_cx_open{envoy_cluster_name="actor_original_dst"} 0
envoy_cluster_circuit_breakers_default_rq_open{envoy_cluster_name="actor_original_dst"} 0
envoy_cluster_circuit_breakers_default_rq_pending_open{envoy_cluster_name="actor_original_dst"} 0
envoy_cluster_upstream_cx_total{envoy_cluster_name="ate-cluster"} 16
envoy_cluster_upstream_rq_total{envoy_cluster_name="ate-cluster"} %.0f
envoy_cluster_upstream_cx_active{envoy_cluster_name="ate-cluster"} 16
envoy_cluster_upstream_rq_active{envoy_cluster_name="ate-cluster"} 300
envoy_cluster_circuit_breakers_default_rq_open{envoy_cluster_name="ate-cluster"} 0
# a metric we do not ask for, which must not appear anywhere
envoy_cluster_upstream_cx_rx_bytes_total{envoy_cluster_name="actor_original_dst"} 123456789
`, cxTotal, rqTotal, cxActive, rqTotal)

	// Request-time histograms in Envoy's live shape: the admin listener
	// reports its own series, the ext_proc cluster emits no rq_time, and the
	// buckets must be ignored. The sums are rigged so the actor hop means
	// exactly 4ms and the in-Envoy total exactly 20ms, at any rqTotal.
	fmt.Fprintf(&b, `# TYPE envoy_http_downstream_rq_time histogram
envoy_http_downstream_rq_time_bucket{envoy_http_conn_manager_prefix="ingress_http",le="5"} 1234
envoy_http_downstream_rq_time_sum{envoy_http_conn_manager_prefix="ingress_http"} %.0f
envoy_http_downstream_rq_time_count{envoy_http_conn_manager_prefix="ingress_http"} %.0f
envoy_http_downstream_rq_time_sum{envoy_http_conn_manager_prefix="admin"} 100
envoy_http_downstream_rq_time_count{envoy_http_conn_manager_prefix="admin"} 100000
# TYPE envoy_cluster_upstream_rq_time histogram
envoy_cluster_upstream_rq_time_bucket{envoy_cluster_name="actor_original_dst",le="5"} 4321
envoy_cluster_upstream_rq_time_sum{envoy_cluster_name="actor_original_dst"} %.0f
envoy_cluster_upstream_rq_time_count{envoy_cluster_name="actor_original_dst"} %.0f
`, rqTotal*20, rqTotal, rqTotal*4, rqTotal)

	// Per-worker and http2 series, which the harness no longer reads. They stay
	// in the fixture because Envoy still publishes them: the parser has to walk
	// past a few thousand of these lines on every scrape without picking any of
	// them up.
	b.WriteString(`# TYPE envoy_cluster_http2_streams_active gauge
envoy_cluster_http2_streams_active{envoy_cluster_name="ate-cluster"} 24
envoy_cluster_http2_streams_active{envoy_cluster_name="xds_cluster"} 1
# TYPE envoy_server_worker_watchdog_miss counter
envoy_server_worker_watchdog_miss{envoy_worker_id="0"} 2
envoy_server_worker_watchdog_miss{envoy_worker_id="1"} 0
# TYPE envoy_listener_worker_downstream_cx_total counter
envoy_listener_worker_downstream_cx_total{envoy_worker_id="0",envoy_listener_address="0.0.0.0_8080"} 150
envoy_listener_worker_downstream_cx_total{envoy_worker_id="1",envoy_listener_address="0.0.0.0_8080"} 300
`)
	return b.String()
}

func TestParseEnvoyStats(t *testing.T) {
	at := time.Unix(1700000000, 0)
	got, err := parseEnvoyStats(strings.NewReader(envoyFixture(300, 900000, 295)), at)
	if err != nil {
		t.Fatalf("parseEnvoyStats: %v", err)
	}

	if got.Concurrency != 40 {
		t.Errorf("Concurrency = %v, want 40", got.Concurrency)
	}
	if got.MemoryAllocated != 104857600 || got.MemoryHeapSize != 209715200 {
		t.Errorf("memory = %v/%v, want 104857600/209715200", got.MemoryAllocated, got.MemoryHeapSize)
	}
	if got.DownstreamCxActive != 128 {
		t.Errorf("DownstreamCxActive = %v, want 128", got.DownstreamCxActive)
	}

	// Two: actor and ext_proc, the pair every request traverses. Server-level
	// metrics must not create a cluster entry, and neither must xds_cluster,
	// which appears in the fixture only in series the parser does not read.
	if len(got.Clusters) != 2 {
		t.Fatalf("got %d clusters, want 2: %v", len(got.Clusters), got.Clusters)
	}
	actor, ok := got.Clusters[ActorClusterName]
	if !ok {
		t.Fatalf("no %q cluster in %v", ActorClusterName, got.Clusters)
	}
	if actor.CxTotal != 300 || actor.RqTotal != 900000 || actor.CxActive != 295 {
		t.Errorf("actor cluster cx/rq/active = %v/%v/%v, want 300/900000/295",
			actor.CxTotal, actor.RqTotal, actor.CxActive)
	}
	if actor.CxOverflow != 7 || actor.RqPendingOverflow != 9 || actor.RqTimeout != 3 {
		t.Errorf("actor overflow/pending_overflow/timeout = %v/%v/%v, want 7/9/3",
			actor.CxOverflow, actor.RqPendingOverflow, actor.RqTimeout)
	}
	if got.Clusters[ExtProcClusterName].CxActive != 16 {
		t.Errorf("ext_proc cx_active = %v, want 16", got.Clusters[ExtProcClusterName].CxActive)
	}
}

// TestParseEnvoyStatsExcludesAdminRequestTime pins the exclusion that keeps
// the harness from measuring itself: the admin listener's request times are
// this harness scraping /stats. The fixture's admin listener is deliberately
// large and fast so a regression shows as a wrong number.
func TestParseEnvoyStatsExcludesAdminRequestTime(t *testing.T) {
	got, err := parseEnvoyStats(strings.NewReader(envoyFixture(300, 900000, 295)), time.Unix(1700000000, 0))
	if err != nil {
		t.Fatalf("parseEnvoyStats: %v", err)
	}
	if got.DownstreamRqTimeCount != 900000 {
		t.Errorf("DownstreamRqTimeCount = %v, want 900000 (the admin listener's 100000 must not be counted)",
			got.DownstreamRqTimeCount)
	}
	if mean := got.DownstreamRqTimeMsTotal / got.DownstreamRqTimeCount; mean != 20 {
		t.Errorf("in-Envoy mean = %v ms, want 20", mean)
	}
	if n := got.Clusters[ActorClusterName].RqTimeCount; n != 900000 {
		t.Errorf("actor RqTimeCount = %v, want 900000", n)
	}
	// The ext_proc cluster genuinely publishes no rq_time: its streams end in
	// a reset. The span code must tell that apart from a zero-millisecond mean.
	if n := got.Clusters[ExtProcClusterName].RqTimeCount; n != 0 {
		t.Errorf("ext_proc RqTimeCount = %v, want 0", n)
	}
}

// TestEnvoyDeltaSteadyStatePooling: with no new connections in the window the
// per-window requests-per-connection is undefined and must be absent, not
// zero. The cumulative ratio must still show pooling is in force.
func TestEnvoyDeltaSteadyStatePooling(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	prev, err := parseEnvoyStats(strings.NewReader(envoyFixture(6, 900000, 6)), t0)
	if err != nil {
		t.Fatalf("parse prev: %v", err)
	}
	// Ten seconds on: 100000 more requests, not one new connection.
	cur, err := parseEnvoyStats(strings.NewReader(envoyFixture(6, 1000000, 6)), t0.Add(10*time.Second))
	if err != nil {
		t.Fatalf("parse cur: %v", err)
	}

	d, err := envoyDelta(prev, cur, 10)
	if err != nil {
		t.Fatalf("envoyDelta: %v", err)
	}
	actor := d.Clusters[ActorClusterName]
	if actor.NewConnections != 0 {
		t.Fatalf("NewConnections = %v, want 0", actor.NewConnections)
	}
	if actor.WindowRqPerCx != nil {
		t.Errorf("WindowRqPerCx = %v, want nil: no connections were opened, so the ratio is undefined", *actor.WindowRqPerCx)
	}
	if want := 1000000.0 / 6.0; actor.RqPerCx != want {
		t.Errorf("RqPerCx = %v, want %v: pooling must still be visible when the window opens nothing", actor.RqPerCx, want)
	}
}

func TestEnvoyDelta(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	prev, err := parseEnvoyStats(strings.NewReader(envoyFixture(300, 900000, 295)), t0)
	if err != nil {
		t.Fatalf("parse prev: %v", err)
	}
	// Ten seconds later: 100 new connections carried 100000 more requests.
	cur, err := parseEnvoyStats(strings.NewReader(envoyFixture(400, 1000000, 295)), t0.Add(10*time.Second))
	if err != nil {
		t.Fatalf("parse cur: %v", err)
	}

	d, err := envoyDelta(prev, cur, 10)
	if err != nil {
		t.Fatalf("envoyDelta: %v", err)
	}
	actor := d.Clusters[ActorClusterName]
	if actor.NewConnections != 100 {
		t.Errorf("NewConnections = %v, want 100", actor.NewConnections)
	}
	if actor.Requests != 100000 {
		t.Errorf("Requests = %v, want 100000", actor.Requests)
	}
	if actor.NewConnectionsPerSec != 10 {
		t.Errorf("NewConnectionsPerSec = %v, want 10", actor.NewConnectionsPerSec)
	}
	// The pooling check the run depends on: far above 1 means connections are
	// reused and port use tracks concurrency, not request rate. Cumulative, so
	// 1000000 requests over 400 connections ever opened.
	if actor.RqPerCx != 2500 {
		t.Errorf("RqPerCx = %v, want 2500", actor.RqPerCx)
	}
	// The window's own ratio is present here because the window did open
	// connections: 100000 requests over 100 of them.
	if actor.WindowRqPerCx == nil || *actor.WindowRqPerCx != 1000 {
		t.Errorf("WindowRqPerCx = %v, want 1000", actor.WindowRqPerCx)
	}
	// Counters that did not move must delta to zero, not carry their absolute
	// value through.
	if actor.CxOverflow != 0 || actor.RqTimeout != 0 {
		t.Errorf("unchanged counters delta'd to %v/%v, want 0/0", actor.CxOverflow, actor.RqTimeout)
	}
	// Gauges are levels, read at the close.
	if actor.CxActive != 295 {
		t.Errorf("CxActive = %v, want 295", actor.CxActive)
	}
	if actor.CircuitBreakerOpen {
		t.Error("CircuitBreakerOpen = true, want false: no breaker gauge was set in the fixture")
	}
	if d.DownstreamRq != 0 {
		t.Errorf("DownstreamRq = %v, want 0", d.DownstreamRq)
	}
	if d.Concurrency != 40 {
		t.Errorf("Concurrency = %v, want 40", d.Concurrency)
	}
}

// TestParseEnvoyStatsReadsOnlyWhatTheSeriesNeeds guards the cost of the scrape.
// Envoy's /stats/prometheus is thousands of lines and it is read on every
// window boundary; anything the parser retains is something the run pays for
// and never uses.
func TestParseEnvoyStatsReadsOnlyWhatTheSeriesNeeds(t *testing.T) {
	got, err := parseEnvoyStats(strings.NewReader(envoyFixture(300, 900000, 295)), time.Unix(1700000000, 0))
	if err != nil {
		t.Fatalf("parseEnvoyStats: %v", err)
	}
	// xds_cluster appears in the fixture only in a per-worker http2 gauge. If
	// it turns up here, the parser is keeping series nothing reads.
	if _, ok := got.Clusters["xds_cluster"]; ok {
		t.Errorf("xds_cluster was parsed from a series the run does not read: %v", got.Clusters)
	}
}

func TestEnvoyDeltaRejectsARestart(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	prev, _ := parseEnvoyStats(strings.NewReader(envoyFixture(300, 900000, 295)), t0)
	// Envoy restarted: counters reset to near zero. Differencing would produce
	// a large negative rate, which must not reach the output.
	cur, _ := parseEnvoyStats(strings.NewReader(envoyFixture(4, 120, 4)), t0.Add(10*time.Second))

	if _, err := envoyDelta(prev, cur, 10); err == nil {
		t.Fatal("envoyDelta accepted counters that went backwards; want an error naming the restart")
	} else if !strings.Contains(err.Error(), "restarted") {
		t.Errorf("error = %q, want it to name the restart", err)
	}
}

func TestEnvoyDeltaFlagsAnOpenBreaker(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	prev, _ := parseEnvoyStats(strings.NewReader(envoyFixture(300, 900000, 295)), t0)
	body := strings.Replace(envoyFixture(400, 1000000, 20000),
		`envoy_cluster_circuit_breakers_default_rq_open{envoy_cluster_name="actor_original_dst"} 0`,
		`envoy_cluster_circuit_breakers_default_rq_open{envoy_cluster_name="actor_original_dst"} 1`, 1)
	cur, _ := parseEnvoyStats(strings.NewReader(body), t0.Add(10*time.Second))

	d, err := envoyDelta(prev, cur, 10)
	if err != nil {
		t.Fatalf("envoyDelta: %v", err)
	}
	if !d.Clusters[ActorClusterName].CircuitBreakerOpen {
		t.Error("CircuitBreakerOpen = false, want true: rq_open was 1")
	}
}

func TestEnvoyDeltaRejectsANonPositiveInterval(t *testing.T) {
	if _, err := envoyDelta(EnvoyStats{}, EnvoyStats{}, 0); err == nil {
		t.Fatal("envoyDelta accepted a zero-length interval")
	}
}

// routerFixture uses the names the OpenTelemetry Prometheus exporter produces
// today: dots become underscores, the histogram gains a unit suffix, and the
// counter gains _total. Route duration is split across outcome labels (summed:
// 5s over 2000 requests), and the parking 2.4s over 1000 resumes nests inside
// the 4s the handler spent on those requests so a sign error cannot pass.
const routerFixture = `# HELP atenet_router_parking_active Requests currently parked.
# TYPE atenet_router_parking_active gauge
atenet_router_parking_active{otel_scope_name="atenet-router"} 12
# TYPE atenet_router_parking_rejected_total counter
atenet_router_parking_rejected_total{otel_scope_name="atenet-router"} 40
# TYPE atenet_router_parking_wait_duration_seconds histogram
atenet_router_parking_wait_duration_seconds_bucket{le="0.1"} 970
atenet_router_parking_wait_duration_seconds_bucket{le="+Inf"} 1000
atenet_router_parking_wait_duration_seconds_sum{otel_scope_name="atenet-router"} 2.4
atenet_router_parking_wait_duration_seconds_count{otel_scope_name="atenet-router"} 1000
# TYPE atenet_router_route_duration_seconds histogram
atenet_router_route_duration_seconds_bucket{ate_router_outcome="ok",le="0.01"} 700
atenet_router_route_duration_seconds_sum{ate_router_outcome="ok",otel_scope_name="atenet-router"} 4
atenet_router_route_duration_seconds_count{ate_router_outcome="ok",otel_scope_name="atenet-router"} 1000
atenet_router_route_duration_seconds_sum{ate_router_outcome="cancelled",otel_scope_name="atenet-router"} 1
atenet_router_route_duration_seconds_count{ate_router_outcome="cancelled",otel_scope_name="atenet-router"} 100
atenet_router_route_duration_seconds_sum{ate_router_outcome="no_capacity",otel_scope_name="atenet-router"} 0
atenet_router_route_duration_seconds_count{ate_router_outcome="no_capacity",otel_scope_name="atenet-router"} 900
# unrelated series that must not be picked up
go_goroutines 250
`

func TestParseRouterStats(t *testing.T) {
	got, err := parseRouterStats(strings.NewReader(routerFixture), time.Unix(1700000000, 0))
	if err != nil {
		t.Fatalf("parseRouterStats: %v", err)
	}
	if !got.Found {
		t.Fatal("Found = false, want true")
	}
	if got.ParkingActive != 12 {
		t.Errorf("ParkingActive = %v, want 12", got.ParkingActive)
	}
	if got.ParkingRejectedTotal != 40 {
		t.Errorf("ParkingRejectedTotal = %v, want 40", got.ParkingRejectedTotal)
	}
	// Buckets are cumulative and would inflate the count if summed in.
	if got.ParkingWaitCount != 1000 {
		t.Errorf("ParkingWaitCount = %v, want 1000 (buckets must be skipped)", got.ParkingWaitCount)
	}
	if got.ParkingWaitSecondsTotal != 2.4 {
		t.Errorf("ParkingWaitSecondsTotal = %v, want 2.4", got.ParkingWaitSecondsTotal)
	}
	// Route duration must sum across every outcome label. Reading only the "ok"
	// series would drop the requests the sidecar cancelled or shed, which are
	// exactly the requests whose time is most worth attributing.
	if !got.RouteFound {
		t.Fatal("RouteFound = false, want true")
	}
	if got.RouteCount != 2000 {
		t.Errorf("RouteCount = %v, want 2000 (1000 ok + 100 cancelled + 900 no_capacity)", got.RouteCount)
	}
	if got.RouteSecondsTotal != 5 {
		t.Errorf("RouteSecondsTotal = %v, want 5", got.RouteSecondsTotal)
	}
}

// TestParseRouterStatsSeparatesTheTwoInstruments: parking wait nests inside
// route duration, so adding them would double-count the resume.
func TestParseRouterStatsSeparatesTheTwoInstruments(t *testing.T) {
	got, err := parseRouterStats(strings.NewReader(routerFixture), time.Unix(1700000000, 0))
	if err != nil {
		t.Fatalf("parseRouterStats: %v", err)
	}
	if got.ParkingWaitCount == got.RouteCount {
		t.Error("parking and route counts are equal: the two instruments were merged")
	}
	if got.ParkingWaitSecondsTotal >= got.RouteSecondsTotal {
		t.Errorf("parking wait %vs is not inside route duration %vs",
			got.ParkingWaitSecondsTotal, got.RouteSecondsTotal)
	}
}

// TestParseRouterStatsTracksTheTwoPrefixesIndependently covers a sidecar that
// exposes parking but not route duration — an older router image against a
// newer harness. The breakdown degrades to "sidecar and Envoy fused" rather
// than reporting a confident zero for the sidecar hop.
func TestParseRouterStatsTracksTheTwoPrefixesIndependently(t *testing.T) {
	parkingOnly := `atenet_router_parking_active{otel_scope_name="atenet-router"} 3
atenet_router_parking_wait_duration_seconds_count{otel_scope_name="atenet-router"} 50
`
	got, err := parseRouterStats(strings.NewReader(parkingOnly), time.Unix(1700000000, 0))
	if err != nil {
		t.Fatalf("parseRouterStats: %v", err)
	}
	if !got.Found {
		t.Error("Found = false, want true: parking series were present")
	}
	if got.RouteFound {
		t.Error("RouteFound = true on a payload with no route series")
	}
	if d := routerDelta(RouterStats{}, got); d.RouteMeasured {
		t.Error("RouterDelta.RouteMeasured = true, want false")
	}
}

func TestParseRouterStatsReportsARenamedMetricAsUnmeasured(t *testing.T) {
	// If the exporter's naming ever moves out from under parkingPrefix, the
	// output must say "not measured" rather than a confident zero.
	got, err := parseRouterStats(strings.NewReader("go_goroutines 250\n"), time.Unix(1700000000, 0))
	if err != nil {
		t.Fatalf("parseRouterStats: %v", err)
	}
	if got.Found {
		t.Fatal("Found = true on a payload with no parking series")
	}
	if d := routerDelta(RouterStats{}, got); d.Measured {
		t.Error("RouterDelta.Measured = true, want false")
	}
}

func TestRouterDelta(t *testing.T) {
	prev := RouterStats{Found: true, ParkingRejectedTotal: 40, ParkingWaitSecondsTotal: 25, ParkingWaitCount: 100}
	cur := RouterStats{
		Found:                   true,
		ParkingActive:           7,
		ParkingRejectedTotal:    46,
		ParkingWaitSecondsTotal: 32,
		ParkingWaitCount:        120,
	}

	d := routerDelta(prev, cur)
	if !d.Measured {
		t.Error("Measured = false, want true")
	}
	if d.ParkingActive != 7 {
		t.Errorf("ParkingActive = %v, want 7", d.ParkingActive)
	}
	if d.ParkingRejected != 6 {
		t.Errorf("ParkingRejected = %v, want 6", d.ParkingRejected)
	}
	if d.ResumeCalls != 20 {
		t.Errorf("ResumeCalls = %v, want 20", d.ResumeCalls)
	}
	// 7 seconds of slot occupancy spread over 20 resumes.
	if want := 350.0; d.MeanResumeMs != want {
		t.Errorf("MeanResumeMs = %v, want %v", d.MeanResumeMs, want)
	}
}

func TestRouterDeltaWithNoResumesLeavesTheMeanAtZero(t *testing.T) {
	// An idle window: two scrapes with no resume between them, which is what
	// the setup and drain phases look like. A mean over zero samples must not
	// divide by zero and surface as NaN in the JSON.
	d := routerDelta(RouterStats{Found: true}, RouterStats{Found: true})
	if d.MeanResumeMs != 0 {
		t.Errorf("MeanResumeMs = %v, want 0", d.MeanResumeMs)
	}
}
