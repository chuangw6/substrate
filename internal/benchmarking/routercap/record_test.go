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

// Tests for sample assembly and for the flat record that reaches stats.jsonl.

package routercap

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func TestAggregateKeepsMaximaForThrottling(t *testing.T) {
	// One throttled container among many idle ones must survive aggregation.
	// Averaging is what would hide it.
	g := aggregate([]ContainerUsage{
		{Container: "ate-controller", CPUCores: 0.2, CPUUtilization: 0.10},
		{Container: "ate-api-server", CPUCores: 15, CPUUtilization: 0.94,
			ThrottledPeriods: 40, Periods: 100, ThrottledSeconds: 0.2, ThrottledFraction: 0.4},
		{Container: "coredns", CPUCores: 0.1, CPUUtilization: 0.10},
	})

	if g.Containers != 3 {
		t.Errorf("Containers = %d, want 3", g.Containers)
	}
	if want := 15.3; math.Abs(g.CPUCores-want) > 1e-9 {
		t.Errorf("CPUCores = %v, want %v (sum)", g.CPUCores, want)
	}
	if g.ThrottledFractionMax != 0.4 || g.ThrottledMaxOf != "ate-api-server" {
		t.Errorf("throttling max = %v of %q, want 0.4 of ate-api-server", g.ThrottledFractionMax, g.ThrottledMaxOf)
	}
	if g.CPUUtilizationMax != 0.94 || g.CPUUtilizationMaxOf != "ate-api-server" {
		t.Errorf("utilization max = %v of %q, want 0.94 of ate-api-server", g.CPUUtilizationMax, g.CPUUtilizationMaxOf)
	}
}

func TestBuildContainersSplitsRolesAndNamesWhatIsMissing(t *testing.T) {
	t0 := time.UnixMilli(1700000000000)
	t1 := t0.Add(10 * time.Second)

	mk := func(k ContainerKey, at time.Time, cpu float64) ContainerSample {
		return ContainerSample{Key: k, At: at, CPUSecondsTotal: cpu, CPUQuota: 4000000, CPUPeriod: 100000}
	}
	envoy := ContainerKey{"ate-system", "atenet-router-abc", "envoy"}
	sidecar := ContainerKey{"ate-system", "atenet-router-abc", "atenet-router"}
	api := ContainerKey{"ate-system", "ate-api-server-1", "ate-api-server"}
	ctrl := ContainerKey{"ate-system", "ate-controller-1", "ate-controller"}
	gone := ContainerKey{"benchmarking", "routercap-xyz", "loadgen"}

	w := Window{
		T0: t0, T1: t1,
		Prev: CadvisorScrape{Containers: map[ContainerKey]ContainerSample{
			envoy:   mk(envoy, t0, 100),
			sidecar: mk(sidecar, t0, 50),
			api:     mk(api, t0, 10),
			ctrl:    mk(ctrl, t0, 1),
		}},
		Cur: CadvisorScrape{Containers: map[ContainerKey]ContainerSample{
			envoy:   mk(envoy, t1, 120),
			sidecar: mk(sidecar, t1, 60),
			api:     mk(api, t1, 15),
			ctrl:    mk(ctrl, t1, 1.1),
		}},
	}
	targets := []Target{
		{RoleEnvoy, envoy}, {RoleSidecar, sidecar},
		{RoleControlPlane, api}, {RoleControlPlane, ctrl},
		{RoleLoadgen, gone},
	}

	containers, groups, missing, errs := buildContainers(w, targets)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if got := containers[RoleEnvoy].CPUCores; got != 2 {
		t.Errorf("envoy CPUCores = %v, want 2 (20 cpu-seconds over 10s)", got)
	}
	if _, ok := containers[RoleControlPlane]; ok {
		t.Error("a many-container role leaked into Containers instead of Groups")
	}
	if g := groups[RoleControlPlane]; g.Containers != 2 || math.Abs(g.CPUCores-0.51) > 1e-9 {
		t.Errorf("control plane group = %+v, want 2 containers summing to 0.51 cores", g)
	}
	// The loadgen container was never in either scrape. It must be named, not
	// dropped, or "we could not see it" reads as "it used nothing".
	if len(missing) != 1 || !strings.Contains(missing[0], RoleLoadgen) {
		t.Errorf("missing = %v, want the loadgen container named", missing)
	}
	if _, ok := containers[RoleLoadgen]; ok {
		t.Error("a missing container produced a usage entry")
	}
}

// sampleFixture is a filled-in window, the shape buildSample produces.
func sampleFixture() Sample {
	t0 := time.UnixMilli(1700000000000).UTC()
	return Sample{
		CPULimitCores: 4, Rung: 3, RungQPS: 4000,
		T0: t0, T1: t0.Add(10 * time.Second), WindowSeconds: 10,
		Load: GenStats{
			OfferedQPS: 4000, AchievedQPS: 3990, SuccessQPS: 3985,
			InFlightEnd: 52, InFlightMax: 71,
			Latency:     LatencyStats{Count: 39900, P50Ms: 4.1, P95Ms: 21.3, MeanMs: 6.2},
			DispatchLag: LatencyStats{P95Ms: 0.4},
			Outcomes:    map[Outcome]int{OutcomeOK: 39900},
		},
		Client: ClientStats{NewConnections: 12, RequestsPerConnection: 3325, ConnectionsInUse: 71},
		Containers: map[string]ContainerUsage{
			RoleEnvoy:   {Container: "envoy", CPUCores: 3.6, CPULimitCores: 4, CPUUtilization: 0.9, MemoryWorkingSetBytes: 1.5e9},
			RoleSidecar: {Container: "atenet-router", CPUCores: 1.1, CPULimitCores: 8, MemoryWorkingSetBytes: 3e8},
		},
		Spans: &LatencySpans{
			Measured: true, SidecarMeasured: true,
			BeforeEnvoyMs: 1.0, EnvoyInternalMs: 0.6, SidecarMs: 1.2, WorkerMs: 3.4,
			ResumeMs: 0.8, TotalClientMs: 6.2, CountSpread: 0.01, ResolutionMsShare: 0.02,
		},
		Guards: []GuardTrip{{Guard: GuardDispatchLag, Fatal: false}},
	}
}

func TestRecordCarriesTheEnvelopeAndTheMeasurements(t *testing.T) {
	r := sampleFixture().Record("abc1234", "routercap")

	if !r.Timestamp.Equal(time.UnixMilli(1700000000000).UTC().Add(10 * time.Second)) {
		// t1, not the midpoint: a series stamped this way never claims a
		// measurement before it was taken.
		t.Errorf("timestamp = %s, want the window's close", r.Timestamp)
	}
	if r.Tag != "abc1234" || r.TestName != "routercap" {
		t.Errorf("envelope = tag %q, test %q", r.Tag, r.TestName)
	}
	if r.CPULimitCores != 4 || r.Rung != 3 || r.RungQPS != 4000 {
		t.Errorf("run identity did not reach the record: %+v", r)
	}
	if r.OfferedQPS != 4000 || r.AchievedQPS != 3990 || r.SuccessQPS != 3985 {
		t.Errorf("load did not reach the record: %+v", r)
	}
	if r.LatencyP95Ms != 21.3 || r.InFlightMax != 71 || r.DispatchLagP95Ms != 0.4 {
		t.Errorf("latency series did not reach the record: %+v", r)
	}
	if r.EnvoyCPUCores == nil || *r.EnvoyCPUCores != 3.6 {
		t.Errorf("envoy cpu = %v, want 3.6", r.EnvoyCPUCores)
	}
	if r.SidecarMemoryBytes == nil || *r.SidecarMemoryBytes != 3e8 {
		t.Errorf("sidecar memory = %v, want 3e8", r.SidecarMemoryBytes)
	}
	if r.SpanWorkerMs == nil || *r.SpanWorkerMs != 3.4 || r.SpanResumeMs == nil || *r.SpanResumeMs != 0.8 {
		t.Errorf("breakdown did not reach the record: %+v", r)
	}
	if len(r.Guards) != 1 || r.Guards[0] != string(GuardDispatchLag) {
		t.Errorf("guards = %v, want the dispatch-lag trip named", r.Guards)
	}
	if r.GuardFatal {
		t.Error("a non-fatal trip was recorded as fatal")
	}
}

func TestRecordDistinguishesUnmeasuredFromZero(t *testing.T) {
	// "cAdvisor did not report this container" and "this container used no
	// CPU" must never look the same, or a scrape gap reads as an idle proxy.
	s := sampleFixture()
	delete(s.Containers, RoleSidecar)
	s.Spans = nil
	// A window that opened no connections: every request reused the pool, which
	// is the healthy case. Reporting 0 requests per connection for it would say
	// the opposite — a fresh connection for every request.
	s.Client.NewConnections = 0

	r := s.Record("", "routercap")
	if r.SidecarCPUCores != nil || r.SidecarMemoryBytes != nil {
		t.Errorf("a container absent from the scrape produced values: %v %v", r.SidecarCPUCores, r.SidecarMemoryBytes)
	}
	if r.SpanWorkerMs != nil || r.SpanTotalMs != nil {
		t.Error("an unmeasured breakdown produced values")
	}
	if r.ClientRequestsPerConnection != nil {
		t.Errorf("no connection was opened, but requests-per-connection reported %v", *r.ClientRequestsPerConnection)
	}

	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{"sidecar_cpu_cores", "span_worker_ms", "guard_fatal",
		"client_requests_per_connection", `"tag"`} {
		if strings.Contains(string(b), key) {
			t.Errorf("absent field %s was encoded as a null or a zero: %s", key, b)
		}
	}
	// The envoy series is present, so it must be there rather than omitted
	// alongside the absent ones.
	if !strings.Contains(string(b), "envoy_cpu_cores") {
		t.Errorf("a measured field was dropped: %s", b)
	}
}

func TestRecordIsFlat(t *testing.T) {
	// The tools that plot this are not in this repo. A nested record makes
	// every one of them write a walker, so the line stays one level deep.
	b, err := json.Marshal(sampleFixture().Record("abc1234", "routercap"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for k, v := range m {
		switch v.(type) {
		case map[string]any:
			t.Errorf("key %q holds an object; the record must stay flat", k)
		case []any:
			// Only the guard names, which are a list of strings by nature.
			if k != "guards" {
				t.Errorf("key %q holds an array; the record must stay flat", k)
			}
		}
	}
	for _, want := range []string{"timestamp", "test_name", "cpu_limit_cores", "offered_qps", "latency_p50_ms", "envoy_cpu_cores"} {
		if _, ok := m[want]; !ok {
			t.Errorf("record is missing %q: %s", want, b)
		}
	}
}
