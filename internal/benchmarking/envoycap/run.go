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
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Derived and hard-coded values. Deliberately not flags: a knob that has never
// been turned should not be a knob.
const (
	// warmupDiscard is dropped from the front of every step, so what is
	// measured is the step's steady state and not the transient of changing
	// rate.
	warmupDiscard = 8 * time.Second

	// httpTimeout matches the router's own route timeout, so client and Envoy
	// agree on when a request is dead.
	httpTimeout = 10 * time.Second

	// maxInFlight caps the worker pool, and with it the client's concurrent
	// connections to Envoy.
	//
	// The client->Envoy hop is HTTP/1.1 keep-alive, so one in-flight request
	// holds one TCP connection, and the pod has roughly 28k ephemeral ports to
	// a single destination. Asking for more than that does not buy concurrency,
	// it buys connect failures that would read back as router latency.
	maxInFlight = 20000

	// requestBuffer smooths dispatcher-to-worker handoff jitter. Small, so a
	// client that has genuinely fallen behind shows it as dispatch lag rather
	// than hiding it in a deep queue.
	requestBuffer = 1024

	// settleTimeout bounds the one-time readiness wait during setup.
	settleTimeout = 3 * time.Minute

	// stepLeadIn puts the first request of a step slightly in the future, so
	// it is not already late the moment the schedule is computed.
	stepLeadIn = 100 * time.Millisecond

	// envoyStatsSettle is how long to wait after a step's load stops before the
	// closing scrape.
	//
	// The router's Envoy bootstrap sets no stats_flush_interval
	// (cmd/atenet/internal/router/envoyrunner.go), so it runs Envoy's 5 s
	// default, and histograms are only published to the admin endpoint on
	// flush. Scraping the instant the load stops therefore reads a snapshot
	// that predates the step's own tail -- and hands that tail to the *next*
	// step's delta, which is how a 250 QPS step whose client-side maximum was
	// 8 ms came to report an Envoy p95 of 428 ms. One flush interval plus
	// margin.
	envoyStatsSettle = 6 * time.Second

	// gaugePollInterval is how often ext_proc concurrency is sampled while the
	// load is applied. A gauge is a level, not a total: it has to be caught in
	// the act.
	gaugePollInterval = 1 * time.Second
)

// BudgetMS is the p95 latency budget the capacity question is anchored to.
// "Max throughput" is not well defined on a queueing curve; fix the budget and
// read the capacity off where the curve crosses it.
const BudgetMS = 500.0

// ErrRigLimited is returned when a rig guard tripped and the run was aborted.
var ErrRigLimited = errors.New("rig limit reached")

// Config is everything the runner needs. The six experiment knobs come from
// flags; the rest is plumbing supplied by the driver script.
type Config struct {
	Actors       int
	StartQPS     float64
	MaxQPS       float64
	Steps        int
	StepDuration time.Duration
	Repeat       int

	Atespace  string
	RouterURL string

	// RouterPodIP enables the in-band layer decomposition. Empty is allowed —
	// the ladder still runs, without the Envoy and router-side numbers.
	RouterPodIP string
	RouterNode  string
	LoadgenNode string
	Cluster     string
	GitSHA      string
	Image       string

	Stub   ateapipb.ControlClient
	Logger *slog.Logger
}

// Runner executes the ladder.
type Runner struct {
	cfg     Config
	log     *slog.Logger
	pinger  *Pinger
	scraper *Scraper
	pool    *Pool
	cpu     *CPUMeter
	workers int
}

// inFlightBudget sizes the worker pool for the top of the ladder.
//
// Sized at rate x httpTimeout, not at rate x some small tail budget, because an
// open loop has to keep issuing requests on schedule no matter how slow the
// replies are. A pool of N workers can only sustain N/L requests per second at
// latency L, so a pool sized for a 2 s tail quietly turns into a *closed* loop
// the moment latency exceeds 2 s -- the dispatcher blocks, arrivals become
// completion-driven, and the generator stops offering the load it claims to.
// That is precisely the failure mode this whole design exists to avoid, and it
// bites hardest above the knee, which is the part of the curve worth having.
// At the timeout, every request has either answered or been abandoned, so a
// pool this size cannot bind for any reason short of the server going silent.
//
// Clamped by maxInFlight: past that the limit is the client's own socket table
// rather than anything about the server. A run that hits the clamp says so, via
// the dispatch-lag guard.
func inFlightBudget(maxQPS float64) int {
	n := int(math.Ceil(maxQPS * httpTimeout.Seconds()))
	return min(max(n, 1), maxInFlight)
}

// NewRunner wires a runner from cfg.
func NewRunner(cfg Config) *Runner {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	workers := inFlightBudget(cfg.MaxQPS)
	pinger := &Pinger{
		Client:    NewHTTPClient(workers, httpTimeout),
		RouterURL: cfg.RouterURL,
	}
	return &Runner{
		cfg:     cfg,
		log:     cfg.Logger,
		pinger:  pinger,
		scraper: NewScraper(cfg.RouterPodIP),
		cpu:     NewCPUMeter(),
		workers: workers,
		pool: NewPool(PoolConfig{
			Stub:     cfg.Stub,
			Atespace: cfg.Atespace,
			Count:    cfg.Actors,
			Pinger:   pinger,
			Logger:   cfg.Logger,
		}),
	}
}

// Setup creates and resumes the actor pool and waits for it to answer. Not
// measured.
func (r *Runner) Setup(ctx context.Context) error {
	if err := r.pool.Setup(ctx); err != nil {
		return err
	}
	return r.pool.Settle(ctx, settleTimeout)
}

// Teardown suspends and deletes every actor. Safe to call more than once.
func (r *Runner) Teardown(ctx context.Context) {
	r.pool.Teardown(ctx)
}

// Run walks the ladder Repeat times and returns the report.
//
// The whole ladder runs every pass: no stopping at the first SLO breach and no
// bisecting. The shape of the curve above the budget is the interesting part,
// and a ladder that stops at the crossing cannot show it. The only thing that
// cuts a run short is a rig guard, which is a different claim entirely.
func (r *Runner) Run(ctx context.Context) (*Report, error) {
	rates, err := Ladder(r.cfg.StartQPS, r.cfg.MaxQPS, r.cfg.Steps)
	if err != nil {
		return nil, err
	}
	actors := r.pool.Actors()
	if len(actors) == 0 {
		return nil, errors.New("no actors in pool; call Setup first")
	}

	startedAt := time.Now()
	report := &Report{
		Run: RunInfo{
			Cluster:   r.cfg.Cluster,
			GitSHA:    r.cfg.GitSHA,
			Image:     r.cfg.Image,
			StartedAt: startedAt.UTC().Format(time.RFC3339),
			Atespace:  r.cfg.Atespace,
			RouterURL: r.cfg.RouterURL,
			Flags: Flags{
				Actors:       r.cfg.Actors,
				StartQPS:     r.cfg.StartQPS,
				MaxQPS:       r.cfg.MaxQPS,
				Steps:        r.cfg.Steps,
				StepDuration: r.cfg.StepDuration.String(),
				Repeat:       r.cfg.Repeat,
			},
			RouterPodIP:       r.cfg.RouterPodIP,
			RouterNode:        r.cfg.RouterNode,
			LoadgenNode:       r.cfg.LoadgenNode,
			GOMAXPROCS:        runtime.GOMAXPROCS(0),
			WarmupSeconds:     warmupDiscard.Seconds(),
			WorkerGoroutines:  r.workers,
			Actors:            actors,
			DistinctWorkerIPs: len(r.pool.PodIPs()),
			Caveats:           Caveats,
		},
	}

	// Envoy fixes its worker-thread count at process start, so one read before
	// the ladder is both sufficient and free of the hot path.
	if r.scraper.Enabled() {
		report.Run.EnvoyConcurrency = int(r.scraper.Scrape(ctx).Concurrency)
		r.log.Info("envoy worker threads", slog.Int("concurrency", report.Run.EnvoyConcurrency))
	}

	var runErr error
outer:
	for pass := 1; pass <= r.cfg.Repeat; pass++ {
		for i, rate := range rates {
			if err := ctx.Err(); err != nil {
				runErr = err
				break outer
			}
			step, err := r.runStep(ctx, pass, i+1, rate, actors)
			if err != nil {
				runErr = err
				break outer
			}
			report.Steps = append(report.Steps, step)
			r.log.Info("step complete",
				slog.Int("pass", pass), slog.Int("step", i+1),
				slog.Float64("offered_qps", step.OfferedQPS),
				slog.Float64("achieved_qps", step.AchievedQPS),
				slog.Float64("p50_ms", step.P50), slog.Float64("p95_ms", step.P95),
				slog.Float64("p99_ms", step.P99), slog.Int("fail", step.Fail))

			// A constraint on the system under test is a result, not a fault:
			// it is reported and the ladder continues, because the shape above
			// the limit is the interesting part.
			for _, n := range step.ConstraintNotes {
				r.log.Warn("system constraint reached",
					slog.Int("pass", pass), slog.Int("step", i+1),
					slog.Float64("offered_qps", step.OfferedQPS), slog.String("note", n))
			}

			if step.RigLimited {
				fmt.Fprint(os.Stderr, RigBanner(step.RigNotes))
				report.Run.Aborted = true
				report.Run.AbortReason = step.RigNotes[0]
				runErr = ErrRigLimited
				break outer
			}
		}
	}

	report.Run.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
		report.Run.Aborted = true
		report.Run.AbortReason = "interrupted: " + runErr.Error()
	}
	return report, runErr
}

// runStep offers one rate for one step duration and reports the window after
// the warmup.
func (r *Runner) runStep(ctx context.Context, pass, idx int, rate float64, actors []Actor) (StepReport, error) {
	r.cpu.Sample() // close the previous interval; this step's starts now

	count := RequestCount(rate, r.cfg.StepDuration)
	stepStart := time.Now().Add(stepLeadIn)

	// Server-side sampling runs alongside the load on its own goroutine: the
	// opening scrape at the end of the warmup, then the ext_proc gauges for as
	// long as the load is applied. See probeStep.
	probe := &stepProbe{}
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		r.probeStep(ctx, stepStart.Add(warmupDiscard), stepStart.Add(r.cfg.StepDuration), probe)
	}()

	reqs := make(chan Request, requestBuffer)
	perWorker := make([][]Sample, r.workers)
	var wg sync.WaitGroup
	for w := range r.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]Sample, 0, int(count)/r.workers+8)
			for req := range reqs {
				local = append(local, r.do(ctx, req, actors))
			}
			perWorker[w] = local
		}()
	}

	dispatchErr := Dispatch(ctx, stepStart, rate, count, len(actors), reqs)
	close(reqs)
	wg.Wait()

	cores, gomaxprocs := r.cpu.Sample()
	<-probeDone

	// Let Envoy publish the step's own tail before the closing scrape, or the
	// delta is short by up to one flush interval at both ends. Skipped when
	// there is nothing to scrape, so a run without a router pod IP does not pay
	// six seconds a step for nothing.
	if r.scraper.Enabled() {
		if !sleepUntil(ctx, time.Now().Add(envoyStatsSettle)) {
			return StepReport{}, ctx.Err()
		}
	}
	after := r.scraper.Scrape(ctx)

	if dispatchErr != nil {
		return StepReport{}, dispatchErr
	}

	total := 0
	for _, s := range perWorker {
		total += len(s)
	}
	samples := make([]Sample, 0, total)
	for _, s := range perWorker {
		samples = append(samples, s...)
	}

	return r.summarize(pass, idx, rate, stepStart, samples, probe, after, cores, gomaxprocs), nil
}

// stepProbe is the server-side sampling for one step, filled in by probeStep on
// its own goroutine and read only after that goroutine has finished.
type stepProbe struct {
	// Before is the opening scrape, taken at the end of the warmup rather than
	// at the start of the step, so that Envoy's window is the same window the
	// client percentiles are computed over.
	Before Snapshot
	// ExtProcPeaks is the element-wise maximum of the ext_proc gauge samples
	// taken while the load was applied, and GaugeSamples is how many samples
	// went into it. Zero samples leaves the peaks nil: a step whose gauges were
	// never read must not report a comfortable zero.
	ExtProcPeaks map[string]float64
	GaugeSamples int
}

// probeStep takes the opening scrape at windowStart, then polls the ext_proc
// gauges until loadEnd, keeping their peak.
//
// Peak rather than a single reading, because concurrency against a cap is a
// level that has to be caught while it is happening: sampled once after the
// load stops it is necessarily zero, which reads back as headroom that was
// never measured.
func (r *Runner) probeStep(ctx context.Context, windowStart, loadEnd time.Time, out *stepProbe) {
	if !sleepUntil(ctx, windowStart) {
		return
	}
	out.Before = r.scraper.Scrape(ctx)
	if !r.scraper.Enabled() {
		return
	}
	for sleepUntil(ctx, time.Now().Add(gaugePollInterval)) && time.Now().Before(loadEnd) {
		sample := r.scraper.ScrapeExtProcGauges(ctx)
		if sample == nil {
			continue // a failed read is not a reading of zero
		}
		out.GaugeSamples++
		out.ExtProcPeaks = mergePeak(out.ExtProcPeaks, sample)
	}
}

// mergePeak folds one gauge sample into a running element-wise maximum,
// allocating on first use so that "never sampled" stays distinguishable from
// "sampled, and zero".
func mergePeak(peaks, sample map[string]float64) map[string]float64 {
	if peaks == nil {
		peaks = map[string]float64{}
	}
	for name, v := range sample {
		if cur, seen := peaks[name]; !seen || v > cur {
			peaks[name] = v
		}
	}
	return peaks
}

// sleepUntil waits for t, reporting false if the context ended first.
func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// do issues one paced request and records it, whatever the outcome.
func (r *Runner) do(ctx context.Context, req Request, actors []Actor) Sample {
	sentAt := time.Now()
	actor := actors[req.ActorIdx]
	// A monotonically increasing token, not a UUID: the echo check needs
	// uniqueness within the step, and crypto/rand on every request would be
	// the load generator's own bottleneck.
	token := strconv.FormatInt(req.Seq, 36)

	outcome, class := r.pinger.Ping(ctx, actor.Host, token)
	done := time.Now()
	return Sample{
		ScheduledAt:   req.ScheduledAt,
		CompletedAt:   done,
		LatencyMS:     MSFloat(done.Sub(req.ScheduledAt)),
		DispatchLagMS: MSFloat(sentAt.Sub(req.ScheduledAt)),
		Outcome:       outcome,
		Class:         class,
		ActorIdx:      req.ActorIdx,
	}
}

// summarize reduces one step's raw samples and scrape deltas to a StepReport.
func (r *Runner) summarize(pass, idx int, rate float64, stepStart time.Time, samples []Sample, probe *stepProbe, after Snapshot, cores float64, gomaxprocs int) StepReport {
	before := probe.Before
	window := AfterWarmup(samples, stepStart, warmupDiscard)
	windowStart := stepStart.Add(warmupDiscard)

	latencies := make([]float64, len(window))
	lags := make([]float64, len(window))
	failByClass := map[string]int{}
	ok := 0
	lastCompletion := windowStart
	for i, s := range window {
		latencies[i] = s.LatencyMS
		lags[i] = s.DispatchLagMS
		if s.Outcome == OutcomeOK {
			ok++
		} else {
			failByClass[s.Class]++
		}
		if s.CompletedAt.After(lastCompletion) {
			lastCompletion = s.CompletedAt
		}
	}

	// Achieved rate is a completion rate: how many of the window's requests
	// came back, over how long it actually took to get them all back. When the
	// system keeps up this tracks the offered rate; when it saturates the
	// dispatcher blocks, the tail stretches, and this falls off y = x. That
	// divergence, read alongside the latency curve, is what separates queueing
	// from saturation.
	achieved := 0.0
	if elapsed := lastCompletion.Sub(windowStart).Seconds(); elapsed > 0 {
		achieved = float64(len(window)) / elapsed
	}

	step := StepReport{
		Repeat:       pass,
		Step:         idx,
		OfferedQPS:   rate,
		AchievedQPS:  achieved,
		WarmupS:      warmupDiscard.Seconds(),
		MeasuredS:    (r.cfg.StepDuration - warmupDiscard).Seconds(),
		Count:        len(window),
		OK:           ok,
		Fail:         len(window) - ok,
		FailByClass:  failByClass,
		Quantiles:    Summarize(latencies),
		LoadgenCores: ptr(cores),
		GOMAXPROCS:   gomaxprocs,
		Envoy:        EnvoyStep{Counters: map[string]float64{}},
		ExtProc: ExtProcStep{
			Counters:    map[string]float64{},
			MaxRequests: ExtProcMaxRequests,
		},
	}
	slices.Sort(lags)
	step.DispatchLagP99MS = Percentile(lags, 0.99)

	distinctIPs := len(r.pool.PodIPs())
	guards := GuardInputs{
		OfferedQPS:        rate,
		DistinctWorkerIPs: distinctIPs,
		DispatchLagP99MS:  step.DispatchLagP99MS,
		LoadgenCores:      cores,
		GOMAXPROCS:        gomaxprocs,
	}

	// Envoy counter deltas over this step alone.
	for name, afterVal := range after.EnvoyCounters {
		delta := afterVal - before.EnvoyCounters[name]
		step.Envoy.Counters[name] = delta
	}
	guards.EnvoyDeltas = step.Envoy.Counters

	// The ext_proc callout cluster. Counters are differenced over the step;
	// gauges are the peak of the samples taken while the load was applied, not
	// a reading from the closing scrape -- by then the load has stopped and
	// every gauge is zero.
	for name, afterVal := range after.ExtProcCounters {
		step.ExtProc.Counters[name] = afterVal - before.ExtProcCounters[name]
	}
	step.ExtProc.Gauges = probe.ExtProcPeaks
	step.ExtProc.GaugeSamples = probe.GaugeSamples
	guards.ExtProcDeltas = step.ExtProc.Counters
	guards.ExtProcGauges = step.ExtProc.Gauges
	step.ExtProc.ActiveFraction = ptr(guards.ExtProcHeadroom())

	if cx, okCx := step.Envoy.Counters["upstream_cx_total"]; okCx && cx > 0 {
		if rq, okRq := step.Envoy.Counters["upstream_rq_total"]; okRq {
			step.Envoy.UpstreamRqPerCx = ptr(rq / cx)
		}
	}

	if h := after.EnvoyRqTime.Sub(before.EnvoyRqTime); h != nil && h.Count > 0 {
		// Envoy's downstream_rq_time buckets are already milliseconds.
		step.Envoy.DownstreamRqTimeP50MS = ptr(h.Quantile(0.50))
		step.Envoy.DownstreamRqTimeP95MS = ptr(h.Quantile(0.95))
		step.Envoy.DownstreamRqTimeP99MS = ptr(h.Quantile(0.99))
	}

	// The worker hop and the handshake inside it, both already in milliseconds.
	if h := after.UpstreamRqTime.Sub(before.UpstreamRqTime); h != nil && h.Count > 0 {
		step.Envoy.UpstreamRqTimeP50MS = ptr(h.Quantile(0.50))
		step.Envoy.UpstreamRqTimeP95MS = ptr(h.Quantile(0.95))
		step.Envoy.UpstreamRqTimeP99MS = ptr(h.Quantile(0.99))
	}
	if h := after.UpstreamCxConnect.Sub(before.UpstreamCxConnect); h != nil && h.Count > 0 {
		step.Envoy.UpstreamCxConnectP50MS = ptr(h.Quantile(0.50))
		step.Envoy.UpstreamCxConnectP95MS = ptr(h.Quantile(0.95))
		step.Envoy.UpstreamCxConnectP99MS = ptr(h.Quantile(0.99))
	}

	resumeMean := time.Duration(0)
	if h := after.RouteDuration.Sub(before.RouteDuration); h != nil && h.Count > 0 {
		ms := h.Scale(1000) // route.duration is in seconds
		step.RouteDurationP50MS = ptr(ms.Quantile(0.50))
		step.RouteDurationP95MS = ptr(ms.Quantile(0.95))
		resumeMean = time.Duration(h.Mean() * float64(time.Second))
	}
	step.SingleflightCollapse = ptr(SingleflightCollapse(rate, len(r.pool.Actors()), resumeMean))

	if wall := after.At.Sub(before.At).Seconds(); wall > 0 && after.SidecarCPUSeconds > 0 {
		step.SidecarCPUCores = ptr((after.SidecarCPUSeconds - before.SidecarCPUSeconds) / wall)
	}

	// Watchdog misses over this step alone. Recorded even when zero, because
	// zero is the load-bearing reading: it is what says Envoy's event loops
	// were never starved, and so that a slow step cannot be blamed on the
	// router's node without evidence.
	step.Watchdog = map[string]float64{}
	for name, afterVal := range after.Watchdog {
		step.Watchdog[name] = afterVal - before.Watchdog[name]
	}
	guards.WatchdogDeltas = step.Watchdog

	step.PerWorkerIPRPS = guards.PerWorkerIPRPS()
	step.RigNotes = guards.Evaluate()
	step.ConstraintNotes = guards.Constraints()
	// Only *exhaustion* aborts. A dispatcher that fell behind a server which
	// had already opened a circuit breaker is reporting that server's latency,
	// not the rig's health -- see GuardInputs.RigExhausted.
	step.RigLimited = guards.RigExhausted(len(step.ConstraintNotes) > 0)
	step.ScrapeErrs = append(append([]string(nil), before.Errs...), after.Errs...)
	return step
}
