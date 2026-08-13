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

// The runner: primes the sources, walks the ladder, and emits one aligned record per
// cAdvisor window until the schedule ends or a fatal guard stops it.

package routercap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Client is what the runner needs from the load generator: a way to issue one
// request, and a way for the generator to report on itself. *Sender is the
// implementation; the interface exists so the orchestration can be tested
// without a socket.
type Client interface {
	Send(ctx context.Context) (Outcome, int)
	Stats() ClientStats
}

// Runner executes one run: one Envoy CPU limit, one ladder, one output stream.
// Different CPU limits are different runs in different processes, so this
// binary needs no write access to anything in the cluster.
type Runner struct {
	// CPULimitCores is the Envoy container's CPU limit under test, and Tag and
	// TestName identify the run. All three are stamped on every record so
	// several runs' files concatenate into one series without losing which is
	// which.
	CPULimitCores int
	Tag           string
	TestName      string

	Rungs  []Rung
	Client Client
	Sink   Sink

	Windows *WindowDriver
	// Envoy and Router are optional. A nil one leaves its section off every
	// record rather than filling it with zeros.
	Envoy  *EnvoyClient
	Router *RouterClient

	Targets []Target
	Guards  GuardConfig

	// MaxInFlight bounds the generator's own concurrency; reaching it is a rig
	// failure recorded as shed requests, not a result.
	MaxInFlight int64
	// TickCap bounds the pacer's sleep and so bounds the dispatch lag the
	// dispatch loop itself can introduce.
	TickCap time.Duration
	// DrainTimeout bounds the wait for in-flight requests at the end of the
	// ladder. Whether it emptied is recorded, because a run that ended with
	// requests outstanding hands them to whatever runs next.
	DrainTimeout time.Duration

	Log *slog.Logger

	sched     *Schedule
	collector *Collector

	// Envoy and router scrapes are taken and differenced entirely inside the
	// sampler goroutine, so they need no lock.
	prevEnvoy  EnvoyStats
	prevRouter RouterStats
	haveEnvoy  bool
	haveRouter bool
}

// RunResult is what the run produced, for the exit status and the closing log
// line. It is not written to the output: stats.jsonl carries the measurements.
type RunResult struct {
	CPULimitCores int

	Rungs   []Rung
	Windows int

	// EnvoyConcurrency is the worker-thread count Envoy reported; it must equal
	// CPULimitCores. Envoy sizes it from the cgroup CPU limit, and a mismatch
	// means the run measured CFS throttling instead of the proxy.
	EnvoyConcurrency float64
	// ClockSkewMs is the residual error in the alignment claim; see
	// WindowDriver.Skew.
	ClockSkewMs float64

	// Drained says whether every request had completed when the run ended.
	Drained bool
	// Interrupted marks a run cut short by its context rather than by the
	// ladder finishing.
	Interrupted bool
	FatalTrips  []GuardTrip
}

func (r *Runner) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}

func (r *Runner) validate() error {
	switch {
	case r.Client == nil:
		return fmt.Errorf("runner needs a client")
	case r.Sink == nil:
		return fmt.Errorf("runner needs a sink")
	case r.Windows == nil:
		return fmt.Errorf("runner needs a window driver: the whole series is aligned off its clock")
	case len(r.Rungs) == 0:
		return fmt.Errorf("runner needs at least one rung")
	}
	return nil
}

// Run executes the ladder and returns once it has finished, the guards have
// stopped it, or ctx is cancelled. The pacer and the cAdvisor-clocked sampler
// run concurrently over one shared collector of raw request events.
func (r *Runner) Run(ctx context.Context) (RunResult, error) {
	res := RunResult{CPULimitCores: r.CPULimitCores}
	if err := r.validate(); err != nil {
		return res, err
	}

	r.sched = &Schedule{}
	r.collector = NewCollector(r.sched)

	if err := r.prime(ctx); err != nil {
		return res, err
	}
	if skew, ok := r.Windows.Skew(); ok {
		res.ClockSkewMs = float64(skew) / float64(time.Millisecond)
	}

	// loadCtx is cancelled by a fatal guard; the sampler keeps its own ctx so
	// it can still write the record that explains why the load stopped.
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()

	finishing := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- r.sampleLoop(ctx, finishing, stopLoad, &res) }()

	pacer := &Pacer{Collector: r.collector, MaxInFlight: r.MaxInFlight, TickCap: r.TickCap}
	ladderErr := r.runLadder(loadCtx, pacer)
	res.Rungs = r.sched.Rungs()

	drainTimeout := r.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 30 * time.Second
	}
	res.Drained = pacer.Drain(ctx, drainTimeout)
	if !res.Drained {
		r.log().Warn("run ended with requests still in flight",
			"cpu_limit_cores", r.CPULimitCores, "in_flight", r.collector.InFlight())
	}

	// One more window after the load stops, so the ladder's final rung is
	// covered by an aligned record rather than truncated mid-interval.
	close(finishing)
	sampleErr := <-done

	switch {
	case sampleErr != nil:
		return res, sampleErr
	case ladderErr != nil && !errors.Is(ladderErr, context.Canceled):
		return res, ladderErr
	case ctx.Err() != nil:
		res.Interrupted = true
		return res, ctx.Err()
	}
	return res, nil
}

// prime establishes the first boundary for every differenced source, so the
// first emitted window is a real interval rather than a delta against zero.
func (r *Runner) prime(ctx context.Context) error {
	if err := r.Windows.Prime(ctx); err != nil {
		return fmt.Errorf("prime cadvisor window: %w", err)
	}
	if r.Envoy != nil {
		s, err := r.Envoy.Scrape(ctx)
		if err != nil {
			// Fatal here, unlike mid-run: an admin endpoint unreachable before
			// any load has been offered is a broken rig.
			return fmt.Errorf("prime envoy stats: %w", err)
		}
		r.prevEnvoy, r.haveEnvoy = s, true
	}
	if r.Router != nil {
		s, err := r.Router.Scrape(ctx)
		if err != nil {
			return fmt.Errorf("prime router stats: %w", err)
		}
		r.prevRouter, r.haveRouter = s, true
	}
	return nil
}

// runLadder walks the rungs back to back. Rungs are not drained between steps:
// idling the system at a boundary would make the next rung's first seconds
// measure a cold connection pool rather than a running one.
func (r *Runner) runLadder(ctx context.Context, p *Pacer) error {
	for _, rung := range r.Rungs {
		if err := ctx.Err(); err != nil {
			return err
		}
		started := r.sched.Begin(rung, time.Now())
		r.log().Info("rung start",
			"cpu_limit_cores", r.CPULimitCores, "rung", started.Index,
			"offered_qps", started.RateQPS, "hold", started.Hold)
		if err := p.RunRung(ctx, started, r.Client.Send); err != nil {
			return err
		}
	}
	return nil
}

// sampleLoop emits the series. It ticks off cAdvisor's housekeeping clock
// rather than a local timer, which is the whole reason load, latency and CPU
// in one record describe one interval.
func (r *Runner) sampleLoop(ctx context.Context, finishing <-chan struct{}, stopLoad func(), res *RunResult) error {
	for {
		w, err := r.Windows.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		s := r.buildSample(ctx, w)
		res.Windows++
		if s.Envoy != nil && s.Envoy.Concurrency > 0 {
			res.EnvoyConcurrency = s.Envoy.Concurrency
		}
		if err := r.Sink.Write(s.Record(r.Tag, r.TestName)); err != nil {
			return fmt.Errorf("write record: %w", err)
		}
		// The sampler is the only consumer of the raw events, so everything up
		// to this window's close has been summarized and can go.
		r.collector.Prune(w.T1)

		if fatal := FatalTrips(s.Guards); len(fatal) > 0 {
			// Stop the load first, then report. The record that names the trip
			// is already written, so the output explains itself.
			res.FatalTrips = fatal
			stopLoad()
			for _, t := range fatal {
				r.log().Error("rig guard tripped", "cpu_limit_cores", r.CPULimitCores, "guard", t.Guard, "detail", t.Detail)
			}
			return &RigLimitedError{Trips: fatal}
		}

		select {
		case <-finishing:
			return nil
		default:
		}
	}
}

// buildSample assembles one aligned record. Every source is asked for the same
// [T0, T1) the window defines, and anything that could not be read lands in
// Missing or Errors rather than being silently zero.
func (r *Runner) buildSample(ctx context.Context, w Window) Sample {
	containers, groups, missing, errs := buildContainers(w, r.Targets)

	s := Sample{
		CPULimitCores: r.CPULimitCores,
		Rung:          -1,
		T0:            w.T0,
		T1:            w.T1,
		WindowSeconds: w.Duration().Seconds(),
		Load:          r.collector.Stats(w.T0, w.T1),
		Client:        r.Client.Stats(),
		Containers:    containers,
		Groups:        groups,
		Missing:       missing,
		Errors:        errs,
	}
	// Rung -1 is a window outside any rung: before the first starts or after
	// the last ends. Kept, because an idle window immediately after saturation
	// is one of the more informative records in the run.
	if rung, warm, ok := r.sched.RungAt(w.Mid()); ok {
		s.Rung, s.RungQPS, s.Warmup = rung.Index, rung.RateQPS, warm
	}

	if r.Envoy != nil {
		cur, err := r.Envoy.Scrape(ctx)
		switch {
		case err != nil:
			s.Errors = append(s.Errors, err.Error())
		case !r.haveEnvoy:
			r.prevEnvoy, r.haveEnvoy = cur, true
		default:
			// Rated over the two scrapes' own interval, not the window's: the
			// scrapes bracket the window, and using the window's length would
			// bias the per-second connection rate the worker guard reads.
			secs := cur.At.Sub(r.prevEnvoy.At).Seconds()
			d, derr := envoyDelta(r.prevEnvoy, cur, secs)
			if derr != nil {
				s.Errors = append(s.Errors, derr.Error())
			} else {
				s.Envoy = &d
			}
			r.prevEnvoy = cur
		}
	}

	if r.Router != nil {
		cur, err := r.Router.Scrape(ctx)
		switch {
		case err != nil:
			s.Errors = append(s.Errors, err.Error())
		case !r.haveRouter:
			r.prevRouter, r.haveRouter = cur, true
		default:
			d := routerDelta(r.prevRouter, cur)
			s.Router = &d
			r.prevRouter = cur
		}
	}

	// After both scrapes: the breakdown needs Envoy's totals and the sidecar's
	// route duration together, and is nil if either is absent.
	s.Spans = latencySpans(s.Load, s.Envoy, s.Router)

	// Last, so the guards see the Envoy section they depend on.
	s.Guards = r.Guards.Check(&s)
	return s
}
