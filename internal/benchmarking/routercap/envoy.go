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

// Scraping Envoy's admin /stats and the sidecar's /metrics, and deltaing the
// counters, which are cumulative since process start rather than per-interval.

package routercap

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"
)

// Envoy's Prometheus counters are cumulative since process start, so every
// counter in the output series is a difference between two scrapes taken at
// the window boundaries. Gauges are read at the closing scrape.
const (
	// ActorClusterName and ExtProcClusterName must match the xDS cluster names
	// in cmd/atenet/internal/router/xds.go. Every request traverses both, so
	// either can be the thing that saturates.
	ActorClusterName   = "actor_original_dst"
	ExtProcClusterName = "ate-cluster"
)

const (
	mClusterCxActive         = "envoy_cluster_upstream_cx_active"
	mClusterCxTotal          = "envoy_cluster_upstream_cx_total"
	mClusterCxOverflow       = "envoy_cluster_upstream_cx_overflow"
	mClusterCxConnectFail    = "envoy_cluster_upstream_cx_connect_fail"
	mClusterCxConnectTimeout = "envoy_cluster_upstream_cx_connect_timeout"
	mClusterRqActive         = "envoy_cluster_upstream_rq_active"
	mClusterRqTotal          = "envoy_cluster_upstream_rq_total"
	mClusterRqTimeout        = "envoy_cluster_upstream_rq_timeout"
	mClusterRqRetry          = "envoy_cluster_upstream_rq_retry"
	mClusterRqPendingActive  = "envoy_cluster_upstream_rq_pending_active"
	mClusterRqPendingOverflw = "envoy_cluster_upstream_rq_pending_overflow"
	mClusterCbCxOpen         = "envoy_cluster_circuit_breakers_default_cx_open"
	mClusterCbRqOpen         = "envoy_cluster_circuit_breakers_default_rq_open"
	mClusterCbPendingOpen    = "envoy_cluster_circuit_breakers_default_rq_pending_open"

	// Deliberately no circuit_breakers.default.remaining_* here: without
	// track_remaining they read a constant zero, the inverse of the truth.
	// Headroom is recoverable as circuit_breaker_limit minus rq_active.

	mServerConcurrency     = "envoy_server_concurrency"
	mServerMemoryAllocated = "envoy_server_memory_allocated"
	mServerMemoryHeapSize  = "envoy_server_memory_heap_size"
	mServerTotalConns      = "envoy_server_total_connections"

	mDownstreamCxActive = "envoy_http_downstream_cx_active"
	mDownstreamRqTotal  = "envoy_http_downstream_rq_total"

	// Both histograms are read only through _sum and _count for an exact mean;
	// a mean is the only statistic that stacks across hops, and Envoy's bucket
	// edges are too coarse for percentiles anyway. Envoy publishes these in
	// milliseconds, unlike the sidecar's seconds.
	mDownstreamRqTimeSum   = "envoy_http_downstream_rq_time_sum"
	mDownstreamRqTimeCount = "envoy_http_downstream_rq_time_count"
	mClusterRqTimeSum      = "envoy_cluster_upstream_rq_time_sum"
	mClusterRqTimeCount    = "envoy_cluster_upstream_rq_time_count"
)

// adminConnManagerPrefix names Envoy's own admin listener in the
// downstream_rq_time label set. Its requests are this harness scraping /stats
// and are excluded so they do not drag the in-Envoy mean toward zero.
const adminConnManagerPrefix = "admin"

var envoyMetrics = map[string]bool{
	mClusterCxActive: true, mClusterCxTotal: true, mClusterCxOverflow: true,
	mClusterCxConnectFail: true, mClusterCxConnectTimeout: true,
	mClusterRqActive: true, mClusterRqTotal: true, mClusterRqTimeout: true,
	mClusterRqRetry: true, mClusterRqPendingActive: true, mClusterRqPendingOverflw: true,
	mClusterCbCxOpen: true, mClusterCbRqOpen: true, mClusterCbPendingOpen: true,
	mServerConcurrency: true, mServerMemoryAllocated: true, mServerMemoryHeapSize: true,
	mServerTotalConns: true, mDownstreamCxActive: true, mDownstreamRqTotal: true,
	mDownstreamRqTimeSum: true, mDownstreamRqTimeCount: true,
	mClusterRqTimeSum: true, mClusterRqTimeCount: true,
}

// ClusterStats is one Envoy cluster's counters and gauges at one instant.
type ClusterStats struct {
	// Gauges.
	CxActive        float64
	RqActive        float64
	RqPendingActive float64
	CbCxOpen        float64
	CbRqOpen        float64
	CbPendingOpen   float64

	// Counters, cumulative since process start.
	CxTotal           float64
	CxOverflow        float64
	CxConnectFail     float64
	CxConnectTimeout  float64
	RqTotal           float64
	RqTimeout         float64
	RqRetry           float64
	RqPendingOverflow float64

	// RqTimeMsTotal and RqTimeCount are the upstream_rq_time histogram's sum and
	// count. Only the actor cluster has them: ext_proc streams all end in a
	// reset, so Envoy never records a completion time for the ate-cluster.
	RqTimeMsTotal float64
	RqTimeCount   float64
}

// EnvoyStats is one scrape of Envoy's admin Prometheus endpoint.
type EnvoyStats struct {
	At time.Time

	// Concurrency is the number of worker threads Envoy started, checked
	// against the run's CPU limit. If the cgroup-aware default did not take,
	// Envoy sizes this from the node's core count and the run would measure CFS
	// throttling instead of the proxy.
	Concurrency      float64
	MemoryAllocated  float64
	MemoryHeapSize   float64
	TotalConnections float64

	DownstreamCxActive float64
	DownstreamRqTotal  float64

	// DownstreamRqTimeMsTotal and DownstreamRqTimeCount are the whole time a
	// request spent inside Envoy, from request headers received to response
	// complete. Every other hop is carved out of this one.
	DownstreamRqTimeMsTotal float64
	DownstreamRqTimeCount   float64

	Clusters map[string]ClusterStats
}

func parseEnvoyStats(r io.Reader, at time.Time) (EnvoyStats, error) {
	out := EnvoyStats{At: at, Clusters: map[string]ClusterStats{}}
	err := scanPromText(r, envoyMetrics, func(s promSample) {
		switch s.Name {
		case mServerConcurrency:
			out.Concurrency = s.Value
			return
		case mServerMemoryAllocated:
			out.MemoryAllocated = s.Value
			return
		case mServerMemoryHeapSize:
			out.MemoryHeapSize = s.Value
			return
		case mServerTotalConns:
			out.TotalConnections = s.Value
			return
		case mDownstreamCxActive:
			out.DownstreamCxActive += s.Value
			return
		case mDownstreamRqTotal:
			out.DownstreamRqTotal += s.Value
			return
		case mDownstreamRqTimeSum:
			if s.Labels["envoy_http_conn_manager_prefix"] != adminConnManagerPrefix {
				out.DownstreamRqTimeMsTotal += s.Value
			}
			return
		case mDownstreamRqTimeCount:
			if s.Labels["envoy_http_conn_manager_prefix"] != adminConnManagerPrefix {
				out.DownstreamRqTimeCount += s.Value
			}
			return
		}

		name := s.Labels["envoy_cluster_name"]
		if name == "" {
			return
		}
		c := out.Clusters[name]
		switch s.Name {
		case mClusterCxActive:
			c.CxActive = s.Value
		case mClusterCxTotal:
			c.CxTotal = s.Value
		case mClusterCxOverflow:
			c.CxOverflow = s.Value
		case mClusterCxConnectFail:
			c.CxConnectFail = s.Value
		case mClusterCxConnectTimeout:
			c.CxConnectTimeout = s.Value
		case mClusterRqActive:
			c.RqActive = s.Value
		case mClusterRqTotal:
			c.RqTotal = s.Value
		case mClusterRqTimeout:
			c.RqTimeout = s.Value
		case mClusterRqRetry:
			c.RqRetry = s.Value
		case mClusterRqPendingActive:
			c.RqPendingActive = s.Value
		case mClusterRqPendingOverflw:
			c.RqPendingOverflow = s.Value
		case mClusterCbCxOpen:
			c.CbCxOpen = s.Value
		case mClusterCbRqOpen:
			c.CbRqOpen = s.Value
		case mClusterCbPendingOpen:
			c.CbPendingOpen = s.Value
		case mClusterRqTimeSum:
			c.RqTimeMsTotal = s.Value
		case mClusterRqTimeCount:
			c.RqTimeCount = s.Value
		}
		out.Clusters[name] = c
	})
	if err != nil {
		return EnvoyStats{}, err
	}
	return out, nil
}

// ClusterDelta is one cluster's behavior over a window: gauges as read at the
// close, counters as differences.
type ClusterDelta struct {
	Cluster string `json:"cluster"`

	CxActive        float64 `json:"cx_active"`
	RqActive        float64 `json:"rq_active"`
	RqPendingActive float64 `json:"rq_pending_active"`

	// CircuitBreakerOpen is true when Envoy reported any default-priority
	// breaker open at the close of the window. Distinguishes "the router is
	// slow" from "the router is refusing work it was configured not to do".
	CircuitBreakerOpen bool `json:"circuit_breaker_open"`

	NewConnections    float64 `json:"new_connections"`
	Requests          float64 `json:"requests"`
	CxOverflow        float64 `json:"cx_overflow"`
	CxConnectFail     float64 `json:"cx_connect_fail"`
	CxConnectTimeout  float64 `json:"cx_connect_timeout"`
	RqTimeout         float64 `json:"rq_timeout"`
	RqRetry           float64 `json:"rq_retry"`
	RqPendingOverflow float64 `json:"rq_pending_overflow"`

	// RqPerCx is requests per upstream connection, cumulative since Envoy
	// started; near 1 means every request opens its own connection and the port
	// budget binds at the request rate. Cumulative because the per-window ratio
	// is undefined in precisely the healthy case (no new connections).
	RqPerCx float64 `json:"rq_per_cx"`

	// WindowRqPerCx is the same ratio confined to this window, nil when the
	// window opened no connections.
	WindowRqPerCx *float64 `json:"window_rq_per_cx,omitempty"`

	NewConnectionsPerSec float64 `json:"new_connections_per_sec"`

	// MeanRqTimeMs is the average request's time on this cluster's hop this
	// window, over RqTimeSamples requests. Zero samples means no rq_time at
	// all, the ext_proc cluster's permanent state (see ClusterStats.RqTimeMsTotal).
	MeanRqTimeMs  float64 `json:"mean_rq_time_ms"`
	RqTimeSamples float64 `json:"rq_time_samples"`
}

// EnvoyDelta is the whole proxy's behavior over a window.
type EnvoyDelta struct {
	Concurrency        float64                 `json:"concurrency"`
	MemoryAllocated    float64                 `json:"memory_allocated"`
	MemoryHeapSize     float64                 `json:"memory_heap_size"`
	DownstreamCxActive float64                 `json:"downstream_cx_active"`
	DownstreamRq       float64                 `json:"downstream_rq"`
	Clusters           map[string]ClusterDelta `json:"clusters"`

	// MeanInEnvoyMs is the mean time a request spent inside Envoy during the
	// window, admin traffic excluded, and InEnvoySamples is the request count it
	// is over. This is the span every other hop is subtracted from.
	MeanInEnvoyMs  float64 `json:"mean_in_envoy_ms"`
	InEnvoySamples float64 `json:"in_envoy_samples"`
}

// envoyDelta differences two scrapes. A counter that went backwards means
// Envoy restarted between scrapes, which invalidates the window rather than
// producing a negative rate.
func envoyDelta(prev, cur EnvoyStats, secs float64) (EnvoyDelta, error) {
	if secs <= 0 {
		return EnvoyDelta{}, fmt.Errorf("envoy delta over a non-positive interval")
	}
	d := EnvoyDelta{
		Concurrency:        cur.Concurrency,
		MemoryAllocated:    cur.MemoryAllocated,
		MemoryHeapSize:     cur.MemoryHeapSize,
		DownstreamCxActive: cur.DownstreamCxActive,
		Clusters:           map[string]ClusterDelta{},
	}
	if cur.DownstreamRqTotal < prev.DownstreamRqTotal {
		return EnvoyDelta{}, fmt.Errorf("envoy downstream_rq_total went backwards (%.0f to %.0f): the proxy restarted mid-window",
			prev.DownstreamRqTotal, cur.DownstreamRqTotal)
	}
	d.DownstreamRq = cur.DownstreamRqTotal - prev.DownstreamRqTotal

	if n := cur.DownstreamRqTimeCount - prev.DownstreamRqTimeCount; n > 0 {
		d.InEnvoySamples = n
		d.MeanInEnvoyMs = (cur.DownstreamRqTimeMsTotal - prev.DownstreamRqTimeMsTotal) / n
	}

	for name, c := range cur.Clusters {
		p := prev.Clusters[name]
		if c.RqTotal < p.RqTotal || c.CxTotal < p.CxTotal {
			return EnvoyDelta{}, fmt.Errorf("envoy cluster %q counters went backwards: the proxy restarted mid-window", name)
		}
		cd := ClusterDelta{
			Cluster:            name,
			CxActive:           c.CxActive,
			RqActive:           c.RqActive,
			RqPendingActive:    c.RqPendingActive,
			CircuitBreakerOpen: c.CbCxOpen > 0 || c.CbRqOpen > 0 || c.CbPendingOpen > 0,
			NewConnections:     c.CxTotal - p.CxTotal,
			Requests:           c.RqTotal - p.RqTotal,
			CxOverflow:         c.CxOverflow - p.CxOverflow,
			CxConnectFail:      c.CxConnectFail - p.CxConnectFail,
			CxConnectTimeout:   c.CxConnectTimeout - p.CxConnectTimeout,
			RqTimeout:          c.RqTimeout - p.RqTimeout,
			RqRetry:            c.RqRetry - p.RqRetry,
			RqPendingOverflow:  c.RqPendingOverflow - p.RqPendingOverflow,
		}
		if c.CxTotal > 0 {
			cd.RqPerCx = c.RqTotal / c.CxTotal
		}
		if cd.NewConnections > 0 {
			w := cd.Requests / cd.NewConnections
			cd.WindowRqPerCx = &w
		}
		if n := c.RqTimeCount - p.RqTimeCount; n > 0 {
			cd.RqTimeSamples = n
			cd.MeanRqTimeMs = (c.RqTimeMsTotal - p.RqTimeMsTotal) / n
		}
		cd.NewConnectionsPerSec = cd.NewConnections / secs
		d.Clusters[name] = cd
	}
	return d, nil
}

// EnvoyClient scrapes Envoy's admin Prometheus endpoint.
type EnvoyClient struct {
	Fetch func(ctx context.Context) (io.ReadCloser, error)
}

// Scrape reads one sample set.
func (c *EnvoyClient) Scrape(ctx context.Context) (EnvoyStats, error) {
	rc, err := c.Fetch(ctx)
	if err != nil {
		return EnvoyStats{}, fmt.Errorf("fetch envoy stats: %w", err)
	}
	defer rc.Close()
	return parseEnvoyStats(rc, time.Now())
}

// RouterStats is the subset of the Go sidecar's own metrics the run cares
// about: parking, and how long the sidecar holds each request. The
// route-duration histogram is the only measurement of the Envoy-to-sidecar hop
// that exists anywhere — Envoy publishes no timer for its ext_proc callout.
type RouterStats struct {
	At time.Time
	// ParkingActive is the live count of parked requests.
	ParkingActive float64
	// ParkingRejectedTotal counts requests shed because the lot was full,
	// cumulative since process start.
	ParkingRejectedTotal float64
	// ParkingWaitSecondsTotal and ParkingWaitCount are the histogram's sum and
	// count. The count is not "requests that had to wait": the router takes a
	// slot around every ResumeActor call (extproc.go handleRequestHeaders), so
	// the observation is the resume round-trip, waiting or not.
	ParkingWaitSecondsTotal float64
	ParkingWaitCount        float64

	// RouteSecondsTotal and RouteCount are the atenet.router.route.duration
	// histogram's sum and count — the whole time the ext_proc handler holds a
	// request, summed across every outcome label, not just "ok".
	// ParkingWaitSecondsTotal is nested inside this; the two must never be
	// added together.
	RouteSecondsTotal float64
	RouteCount        float64

	// Found records whether any parking series was present at all, so a
	// renamed metric shows up as "not measured" instead of "always zero".
	Found bool
	// RouteFound is the same guarantee for the route-duration series, tracked
	// separately because the two instruments can be renamed independently.
	RouteFound bool
}

// Metric-name prefixes, matched by prefix rather than spelled exactly because
// the OpenTelemetry Prometheus exporter appends unit and _total suffixes and
// rewrites dots.
const (
	parkingPrefix = "atenet_router_parking"
	routePrefix   = "atenet_router_route"
)

// parseRouterStats extracts the parking and route-duration series from the
// sidecar's own Prometheus endpoint, classifying series by substring rather
// than exact name for the reason given above.
func parseRouterStats(r io.Reader, at time.Time) (RouterStats, error) {
	out := RouterStats{At: at}
	err := scanPromTextMatch(r,
		func(name string) bool {
			return strings.HasPrefix(name, parkingPrefix) || strings.HasPrefix(name, routePrefix)
		},
		func(s promSample) {
			name := s.Name
			if strings.HasSuffix(name, "_bucket") {
				return
			}
			if strings.HasPrefix(name, routePrefix) {
				out.RouteFound = true
				switch {
				case strings.HasSuffix(name, "_sum"):
					out.RouteSecondsTotal += s.Value
				case strings.HasSuffix(name, "_count"):
					out.RouteCount += s.Value
				}
				return
			}
			out.Found = true
			switch {
			case strings.Contains(name, "rejected"):
				out.ParkingRejectedTotal += s.Value
			case strings.Contains(name, "wait") && strings.HasSuffix(name, "_sum"):
				out.ParkingWaitSecondsTotal += s.Value
			case strings.Contains(name, "wait") && strings.HasSuffix(name, "_count"):
				out.ParkingWaitCount += s.Value
			case strings.Contains(name, "active"):
				out.ParkingActive += s.Value
			}
		})
	if err != nil {
		return RouterStats{}, err
	}
	return out, nil
}

// RouterClient scrapes the atenet-router sidecar's metrics endpoint.
type RouterClient struct {
	Fetch func(ctx context.Context) (io.ReadCloser, error)
}

// Scrape reads one sample set.
func (c *RouterClient) Scrape(ctx context.Context) (RouterStats, error) {
	rc, err := c.Fetch(ctx)
	if err != nil {
		return RouterStats{}, fmt.Errorf("fetch router stats: %w", err)
	}
	defer rc.Close()
	return parseRouterStats(rc, time.Now())
}

// RouterDelta is the parking behavior over one window. Read ParkingRejected
// first: it is the only field that says parking went wrong.
type RouterDelta struct {
	Measured bool `json:"measured"`
	// ParkingActive is the instantaneous slot occupancy at the closing scrape.
	// By Little's Law it is roughly MeanResumeMs x request rate, so single
	// digits at a few thousand QPS is healthy, not a backlog.
	ParkingActive float64 `json:"parking_active"`
	// ParkingRejected counts requests shed this window because the lot was
	// full. It must stay zero: non-zero means the router answered 503 rather
	// than routing.
	ParkingRejected float64 `json:"parking_rejected"`
	// ResumeCalls is how many requests completed a resume in this window,
	// which equals the window's request count in a healthy run. Named for what
	// it is because the underlying "parking" metric name inverts its meaning.
	ResumeCalls float64 `json:"resume_calls"`
	// MeanResumeMs is the mean time a request spent holding a parking slot,
	// i.e. the ResumeActor round trip to ate-api-server. It is part of every
	// request's client-observed latency — a component of p50, not a parking
	// problem.
	MeanResumeMs float64 `json:"mean_resume_ms"`

	// MeanRouteMs is the whole time the sidecar held the average request this
	// window, and RouteCalls the number of requests it is over. The resume
	// nests inside the route per request, but the two means are over different
	// populations, so subtracting them is only valid while the counts agree.
	MeanRouteMs float64 `json:"mean_route_ms"`
	RouteCalls  float64 `json:"route_calls"`
	// RouteMeasured is false when the sidecar exposed no route-duration series,
	// which collapses the span breakdown back to "sidecar and Envoy, fused".
	RouteMeasured bool `json:"route_measured"`
}

func routerDelta(prev, cur RouterStats) RouterDelta {
	d := RouterDelta{
		Measured:        cur.Found,
		RouteMeasured:   cur.RouteFound,
		ParkingActive:   cur.ParkingActive,
		ParkingRejected: cur.ParkingRejectedTotal - prev.ParkingRejectedTotal,
		ResumeCalls:     cur.ParkingWaitCount - prev.ParkingWaitCount,
	}
	if d.ResumeCalls > 0 {
		d.MeanResumeMs = (cur.ParkingWaitSecondsTotal - prev.ParkingWaitSecondsTotal) / d.ResumeCalls * 1000
	}
	if n := cur.RouteCount - prev.RouteCount; n > 0 {
		d.RouteCalls = n
		d.MeanRouteMs = (cur.RouteSecondsTotal - prev.RouteSecondsTotal) / n * 1000
	}
	return d
}
