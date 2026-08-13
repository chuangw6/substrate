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

// End-to-end tests for the runner against fake cAdvisor, Envoy and actor endpoints.

package routercap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var loadgenKey = ContainerKey{Namespace: "benchmarking", Pod: "routercap-runner-abc", Container: "loadgen"}

// loadgenFixture adds the generator's own container to a cAdvisor payload. The
// run's most important guard reads it, so a fake node without it would exercise
// the orchestration with that guard silently absent.
func loadgenFixture(at time.Time, cpuSeconds, quota float64) string {
	ms := at.UnixMilli()
	var b strings.Builder
	row := func(metric string, v float64) {
		fmt.Fprintf(&b, "%s{container=\"loadgen\",namespace=\"benchmarking\",pod=\"routercap-runner-abc\"} %g %d\n", metric, v, ms)
	}
	row(metricCPUUsageSeconds, cpuSeconds)
	row(metricMemoryWorkingSet, 3e8)
	row(metricCFSPeriods, 1000)
	row(metricCFSThrottledPeriods, 0)
	row(metricSpecCPUQuota, quota)
	row(metricSpecCPUPeriod, 100000)
	return b.String()
}

// fakeNode is a kubelet whose housekeeping timestamp advances on a real
// wall-clock grid, so windows the runner produces are genuine intervals of the
// test's own execution. That lets load and CPU statistics in one record
// describe the same moment — the property under test.
type fakeNode struct {
	start time.Time
	grid  time.Duration

	envoyCores   float64
	routerCores  float64
	loadgenCores float64
	loadgenQuota float64
}

func (f *fakeNode) fetch(context.Context) (io.ReadCloser, error) {
	k := time.Since(f.start) / f.grid
	at := f.start.Add(k * f.grid)
	// Counters are a linear function of the *quantized* instant, so every
	// derived rate comes out at exactly the configured core count regardless of
	// when the fetch happened to land.
	secs := at.Sub(f.start).Seconds()
	body := cadvisorFixture(at, 100+f.envoyCores*secs, 20+f.routerCores*secs) +
		loadgenFixture(at, f.loadgenCores*secs, f.loadgenQuota)
	return io.NopCloser(strings.NewReader(body)), nil
}

// fakeAdmin serves Envoy admin payloads whose counters climb, so consecutive
// scrapes produce a non-degenerate delta.
type fakeAdmin struct {
	mu sync.Mutex
	n  int
}

func (f *fakeAdmin) fetch(context.Context) (io.ReadCloser, error) {
	f.mu.Lock()
	n := f.n
	f.n++
	f.mu.Unlock()
	return io.NopCloser(strings.NewReader(envoyFixture(
		300+float64(n)*10,      // cx_total
		900000+float64(n)*5000, // rq_total
		295,                    // cx_active
	))), nil
}

func staticFetch(body string) func(context.Context) (io.ReadCloser, error) {
	return func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(body)), nil
	}
}

// fakeClient answers instantly and reports whatever transport picture the test
// wants the guards to see.
type fakeClient struct {
	mu         sync.Mutex
	sent       int
	connsInUse int64
	newConns   float64
	reqsPerCx  float64
}

func (c *fakeClient) Send(context.Context) (Outcome, int) {
	c.mu.Lock()
	c.sent++
	c.mu.Unlock()
	return OutcomeOK, 200
}

func (c *fakeClient) Stats() ClientStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ClientStats{
		NewConnections:        c.newConns,
		RequestsPerConnection: c.reqsPerCx,
		ConnectionsInUse:      c.connsInUse,
	}
}

func (c *fakeClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sent
}

type memSink struct {
	mu      sync.Mutex
	records []StatsRecord
}

func (s *memSink) Write(v StatsRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, v)
	return nil
}

func (s *memSink) all() []StatsRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]StatsRecord(nil), s.records...)
}

// newTestRunner wires a runner against fakes for every external source. The
// grid is small so a ladder that would take minutes in a cluster takes a
// fraction of a second here, without changing any of the logic under test.
func newTestRunner(t *testing.T, client Client, sink Sink, ladder LadderSpec) (*Runner, *fakeNode) {
	t.Helper()
	node := &fakeNode{
		start:        time.Now(),
		grid:         60 * time.Millisecond,
		envoyCores:   4,
		routerCores:  0.5,
		loadgenCores: 2,
		loadgenQuota: 100 * 100000, // 100 cores
	}
	guards := DefaultGuardConfig()
	guards.WorkerPods = 100
	// The fakes answer instantly, so the generator cannot fall behind for any
	// reason a cluster would produce; the tiny grid makes the scheduler's own
	// jitter the only source of lag, and it is not what these tests are about.
	guards.DispatchLagP95Ms = 0

	return &Runner{
		CPULimitCores: 40,
		Tag:           "abc1234",
		TestName:      "routercap",
		Rungs:         ladder.Build(),
		Client:        client,
		Sink:          sink,
		Windows: &WindowDriver{
			Client:       &CadvisorClient{Fetch: node.fetch},
			Anchor:       envoyKey,
			PollInterval: 3 * time.Millisecond,
			MaxWait:      5 * time.Second,
		},
		Envoy:  &EnvoyClient{Fetch: (&fakeAdmin{}).fetch},
		Router: &RouterClient{Fetch: staticFetch(routerFixture)},
		Targets: []Target{
			{Role: RoleEnvoy, Key: envoyKey},
			{Role: RoleSidecar, Key: routerKey},
			{Role: RoleLoadgen, Key: loadgenKey},
		},
		Guards:       guards,
		MaxInFlight:  4096,
		TickCap:      time.Millisecond,
		DrainTimeout: 2 * time.Second,
	}, node
}

func TestRunnerProducesAnAlignedSeries(t *testing.T) {
	client := &fakeClient{connsInUse: 40, newConns: 2, reqsPerCx: 500}
	sink := &memSink{}
	ladder := LadderSpec{StartQPS: 200, StepQPS: 200, Rungs: 3, Hold: 150 * time.Millisecond, Warmup: 40 * time.Millisecond}
	r, _ := newTestRunner(t, client, sink, ladder)

	res, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	records := sink.all()
	if len(records) < 3 {
		t.Fatalf("got %d records over a 450ms ladder on a 60ms grid, want at least 3", len(records))
	}
	if res.Windows != len(records) {
		t.Errorf("result counted %d windows but %d were written", res.Windows, len(records))
	}
	if !res.Drained {
		t.Errorf("run ended with requests still in flight against instantaneous fakes")
	}
	if len(res.Rungs) != 3 {
		t.Errorf("ran %d rungs, want all 3", len(res.Rungs))
	}

	t.Run("EveryRecordCarriesTheEnvelope", func(t *testing.T) {
		for i, rec := range records {
			if rec.Tag != "abc1234" || rec.TestName != "routercap" || rec.CPULimitCores != 40 {
				t.Fatalf("record %d envelope = tag %q, test %q, limit %d",
					i, rec.Tag, rec.TestName, rec.CPULimitCores)
			}
			if rec.Timestamp.IsZero() {
				t.Fatalf("record %d has no timestamp", i)
			}
			if i > 0 && !rec.Timestamp.After(records[i-1].Timestamp) {
				t.Errorf("record %d is stamped %v, not after the previous %v: the series does not advance",
					i, rec.Timestamp, records[i-1].Timestamp)
			}
			if got := rec.WindowSeconds; math.Abs(got-0.06) > 1e-6 {
				t.Errorf("record %d: window = %vs, want the 60ms kubelet grid", i, got)
			}
		}
	})

	t.Run("ResourceSeriesAreReadPerContainer", func(t *testing.T) {
		rec := records[len(records)-1]
		if rec.EnvoyCPUCores == nil || math.Abs(*rec.EnvoyCPUCores-4) > 1e-6 {
			t.Errorf("envoy cpu = %v cores, want 4", rec.EnvoyCPUCores)
		}
		if rec.SidecarCPUCores == nil || math.Abs(*rec.SidecarCPUCores-0.5) > 1e-6 {
			t.Errorf("sidecar cpu = %v cores, want 0.5", rec.SidecarCPUCores)
		}
		if rec.EnvoyMemoryBytes == nil || *rec.EnvoyMemoryBytes != 1.5e9 {
			t.Errorf("envoy memory = %v, want 1.5e9", rec.EnvoyMemoryBytes)
		}
	})

	t.Run("LoadAndResourcesShareTheWindow", func(t *testing.T) {
		// The claim the whole design rests on: find a record inside a rung and
		// confirm the load figures for it are non-zero, i.e. computed over the
		// same interval the CPU number came from rather than over a timer's own.
		var found bool
		for _, rec := range records {
			if rec.Rung < 0 {
				continue
			}
			found = true
			if rec.OfferedQPS <= 0 {
				t.Errorf("rung %d record offered %v QPS; the schedule says otherwise", rec.Rung, rec.OfferedQPS)
			}
			if rec.RungQPS <= 0 {
				t.Errorf("rung %d record has no nominal rate", rec.Rung)
			}
			if rec.EnvoyCPUCores == nil || *rec.EnvoyCPUCores <= 0 {
				t.Errorf("rung %d record has load but no CPU: the two series would not line up", rec.Rung)
			}
		}
		if !found {
			t.Error("no record fell inside a rung")
		}
	})

	t.Run("WarmupIsMarkedNotDropped", func(t *testing.T) {
		// A rung's first seconds are where the pool grows; they belong in the
		// file, flagged, so exclusion is the analysis's decision and not the
		// harness's.
		var warm int
		for _, rec := range records {
			if rec.Warmup {
				warm++
			}
		}
		if warm == 0 {
			t.Error("no record was flagged as warmup across three rungs with a 40ms warmup each")
		}
	})

	t.Run("TheBreakdownIsDerivedFromBothScrapes", func(t *testing.T) {
		rec := records[len(records)-1]
		if rec.SpanTotalMs == nil || rec.SpanWorkerMs == nil {
			t.Fatalf("no breakdown on a window where both Envoy and the sidecar answered: %+v", rec)
		}
		if rec.SpanSidecarMs == nil || rec.SpanResumeMs == nil {
			t.Errorf("the sidecar hop is missing though the router fixture reports route duration: %+v", rec)
		}
		if rec.SpanCountSpread == nil || rec.SpanResolutionShare == nil {
			t.Error("the breakdown arrived without its two quality markers, so it cannot be judged")
		}
	})

	t.Run("EveryRequestTheClientSentWasAskedForByThePacer", func(t *testing.T) {
		// 200+400+600 QPS held 150ms each = 30+60+90.
		if got, want := client.count(), 180; got > want {
			t.Errorf("client sent %d requests, more than the %d the ladder scheduled", got, want)
		}
		if client.count() == 0 {
			t.Fatal("the ladder sent nothing")
		}
	})

	if res.EnvoyConcurrency != 40 {
		t.Errorf("result envoy concurrency = %v, want 40", res.EnvoyConcurrency)
	}
	if res.ClockSkewMs < 0 {
		t.Errorf("clock skew = %vms; the sample cannot postdate the fetch that read it", res.ClockSkewMs)
	}
}

func TestRunnerKeepsTheGuardInputsOffTheOutput(t *testing.T) {
	// The guards read the loadgen container, the control plane and the Envoy
	// cluster deltas; none of those belong in the CI series. The internal
	// sample has to stay rich enough to check them while the record stays flat.
	client := &fakeClient{connsInUse: 40, newConns: 2, reqsPerCx: 500}
	r, node := newTestRunner(t, client, &memSink{}, LadderSpec{StartQPS: 100, Rungs: 1, Hold: 60 * time.Millisecond})
	r.sched = &Schedule{}
	r.collector = NewCollector(r.sched)

	ctx := context.Background()
	if err := r.prime(ctx); err != nil {
		t.Fatalf("prime: %v", err)
	}
	// One grid step, so the anchor's timestamp has moved by the time Next runs.
	time.Sleep(node.grid)
	w, err := r.Windows.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	s := r.buildSample(ctx, w)

	if _, ok := s.Containers[RoleLoadgen]; !ok {
		t.Error("the loadgen container is absent from the sample, so its guard cannot run")
	}
	if s.Envoy == nil {
		t.Fatal("no Envoy section on the sample")
	}
	if _, ok := s.Envoy.Clusters[ActorClusterName]; !ok {
		t.Errorf("no %s cluster delta, so the worker connection-rate guard cannot run", ActorClusterName)
	}
	if s.Router == nil || !s.Router.Measured {
		t.Errorf("router parking section = %+v, want it measured", s.Router)
	}

	b, err := json.Marshal(s.Record(r.Tag, r.TestName))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, forbidden := range []string{"loadgen", "ate-system", "clusters", "parking"} {
		if strings.Contains(string(b), forbidden) {
			t.Errorf("the record leaked the guard input %q: %s", forbidden, b)
		}
	}
}

func TestRunnerStopsTheLadderOnAFatalGuard(t *testing.T) {
	// A rig-limited run must stop rather than keep emitting numbers that
	// describe the load generator.
	sink := &memSink{}
	cfg := DefaultGuardConfig()
	client := &fakeClient{
		connsInUse: int64(cfg.ClientConnectionCeiling) + 1,
		newConns:   2,
		reqsPerCx:  500,
	}
	// Long enough that finishing normally would take many seconds; the guard
	// should cut it off inside the first rung.
	ladder := LadderSpec{StartQPS: 100, StepQPS: 100, Rungs: 10, Hold: time.Second}
	r, _ := newTestRunner(t, client, sink, ladder)

	res, err := r.Run(context.Background())

	var rigErr *RigLimitedError
	if !errors.As(err, &rigErr) {
		t.Fatalf("Run returned %v, want a RigLimitedError", err)
	}
	if len(res.FatalTrips) == 0 {
		t.Fatal("result carries no fatal trips to explain the stop")
	}
	if got := res.FatalTrips[0].Guard; got != GuardClientPorts {
		t.Errorf("tripped %s, want %s", got, GuardClientPorts)
	}
	if len(res.Rungs) >= 10 {
		t.Errorf("ran %d of 10 rungs; the guard did not stop the ladder", len(res.Rungs))
	}

	records := sink.all()
	if len(records) == 0 {
		t.Fatal("no record was written; the output would not explain why the run stopped")
	}
	last := records[len(records)-1]
	if !last.GuardFatal {
		t.Errorf("the last written record does not carry the trip: %+v", last.Guards)
	}
	if len(last.Guards) == 0 || last.Guards[0] != string(GuardClientPorts) {
		t.Errorf("guards = %v, want %s named", last.Guards, GuardClientPorts)
	}
}

func TestRunnerStopsOnAnInterrupt(t *testing.T) {
	// Ctrl-C, or the Job being deleted: exit promptly and say it was cut short
	// rather than reporting a complete ladder.
	client := &fakeClient{connsInUse: 40, newConns: 2, reqsPerCx: 500}
	sink := &memSink{}
	ladder := LadderSpec{StartQPS: 100, StepQPS: 100, Rungs: 10, Hold: time.Second}
	r, _ := newTestRunner(t, client, sink, ladder)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(150*time.Millisecond, cancel)

	done := make(chan struct{})
	var res RunResult
	var err error
	go func() {
		defer close(done)
		res, err = r.Run(ctx)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if !res.Interrupted {
		t.Error("result does not record that the run was interrupted")
	}
	if len(res.Rungs) >= 10 {
		t.Errorf("ran %d of 10 rungs after a 150ms cancel", len(res.Rungs))
	}
}

func TestRunnerRejectsAnIncompleteConfiguration(t *testing.T) {
	cases := map[string]func(*Runner){
		"NoClient":  func(r *Runner) { r.Client = nil },
		"NoSink":    func(r *Runner) { r.Sink = nil },
		"NoWindows": func(r *Runner) { r.Windows = nil },
		"NoRungs":   func(r *Runner) { r.Rungs = nil },
	}
	for name, break_ := range cases {
		t.Run(name, func(t *testing.T) {
			r, _ := newTestRunner(t, &fakeClient{}, &memSink{},
				LadderSpec{StartQPS: 10, Rungs: 1, Hold: 10 * time.Millisecond})
			break_(r)
			if _, err := r.Run(context.Background()); err == nil {
				t.Fatal("Run accepted a runner missing one of its sources")
			}
		})
	}
}

func TestRunnerFailsWhenTheAnchorIsAbsent(t *testing.T) {
	// Expected right after a CPU-limit change: the router pod was replaced, so
	// the anchor key no longer resolves and the caller must re-resolve it.
	// Failing at prime is the point — a run that silently measured nothing is
	// worse.
	r, _ := newTestRunner(t, &fakeClient{}, &memSink{},
		LadderSpec{StartQPS: 10, Rungs: 1, Hold: 10 * time.Millisecond})
	r.Windows.Client = &CadvisorClient{Fetch: staticFetch("# empty\n")}

	if _, err := r.Run(context.Background()); !errors.Is(err, ErrAnchorMissing) {
		t.Fatalf("err = %v, want ErrAnchorMissing", err)
	}
}

func TestCollectorReportsThePeakNotTheInstant(t *testing.T) {
	// In-flight is the number that explains a port wall, and it peaks between
	// reads. Reporting the live count at the moment of the read would show a
	// fraction of it.
	c := NewCollector(&Schedule{})
	now := time.Now()
	for i := 0; i < 5; i++ {
		c.RecordDispatch(now, now)
	}
	for i := 0; i < 4; i++ {
		c.RecordCompletion(now, now, OutcomeOK, 200)
	}

	first := c.Stats(now.Add(-time.Second), now.Add(time.Second))
	if first.InFlightMax != 5 {
		t.Errorf("peak = %d, want the 5 that were simultaneously in flight", first.InFlightMax)
	}
	// The read resets the slot to the live count, so the next window reports
	// its own peak rather than inheriting the last one's.
	second := c.Stats(now.Add(-time.Second), now.Add(time.Second))
	if second.InFlightMax != 1 {
		t.Errorf("peak = %d after the previous read, want the live count of 1", second.InFlightMax)
	}
}

func TestLadderSpecBuildsEvenRungs(t *testing.T) {
	l := LadderSpec{StartQPS: 1000, StepQPS: 1000, Rungs: 16, Hold: 45 * time.Second, Warmup: 10 * time.Second}
	rungs := l.Build()
	if len(rungs) != 16 {
		t.Fatalf("built %d rungs, want 16", len(rungs))
	}
	if rungs[0].RateQPS != 1000 || rungs[15].RateQPS != 16000 {
		t.Errorf("rates run %v..%v, want 1000..16000", rungs[0].RateQPS, rungs[15].RateQPS)
	}
	if l.PeakQPS() != 16000 {
		t.Errorf("PeakQPS = %v, want 16000", l.PeakQPS())
	}
	for i, r := range rungs {
		if r.Index != i {
			t.Errorf("rung %d has index %d", i, r.Index)
		}
		if r.Hold != 45*time.Second || r.Warmup != 10*time.Second {
			t.Errorf("rung %d: hold=%v warmup=%v, want the same for every rung", i, r.Hold, r.Warmup)
		}
		if !r.StartAt.IsZero() {
			t.Errorf("rung %d was built with a start time; only the pacer knows when a rung actually begins", i)
		}
	}
}

func TestJSONLSinkWritesOneLinePerRecord(t *testing.T) {
	dir := t.TempDir()
	sink, err := OpenJSONLSink(dir)
	if err != nil {
		t.Fatalf("OpenJSONLSink: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := sink.Write(StatsRecord{CPULimitCores: 40, Rung: i}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// Read before Close: unbuffered writes are what make a killed run's output
	// usable, and a test that only reads after a clean Close would not notice
	// buffering creeping in.
	b, err := os.ReadFile(filepath.Join(dir, StatsFile))
	if err != nil {
		t.Fatalf("read %s: %v", StatsFile, err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines before Close, want 3: output is being buffered", len(lines))
	}
	var got StatsRecord
	if err := json.Unmarshal([]byte(lines[2]), &got); err != nil {
		t.Fatalf("unmarshal line 3: %v", err)
	}
	if got.Rung != 2 {
		t.Errorf("last line is rung %d, want 2", got.Rung)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestStreamSinkTagsEveryLineSoTheStreamsCanBeSplitApart(t *testing.T) {
	// The in-cluster Job's only usable output channel is stdout, and kubectl
	// logs merges it with the binary's own stderr logs. Without the tag, run.sh
	// would have to guess which lines are data.
	var buf bytes.Buffer
	s := NewStreamSink(&buf)
	if err := s.Write(StatsRecord{CPULimitCores: 40, Rung: 2}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	var got struct {
		Stream string      `json:"stream"`
		Record StatsRecord `json:"record"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Stream != StreamStats {
		t.Errorf("stream = %q, want %q", got.Stream, StreamStats)
	}
	if got.Record.Rung != 2 {
		t.Errorf("record did not survive the wrapper: %+v", got.Record)
	}
}

func TestMultiSinkKeepsGoingWhenOneSinkFails(t *testing.T) {
	// A full disk on the laptop copy must not cost us the stdout copy, which in
	// an in-cluster run is the only copy that leaves the pod.
	var buf bytes.Buffer
	m := MultiSink{failingSink{}, NewStreamSink(&buf)}

	if err := m.Write(StatsRecord{CPULimitCores: 40}); err == nil {
		t.Error("MultiSink hid a sink's write failure")
	}
	if n := len(strings.Split(strings.TrimSpace(buf.String()), "\n")); n != 1 {
		t.Errorf("the healthy sink got %d lines, want 1: a failing sink stopped the fan-out", n)
	}
}

type failingSink struct{}

func (failingSink) Write(StatsRecord) error { return errors.New("disk full") }
