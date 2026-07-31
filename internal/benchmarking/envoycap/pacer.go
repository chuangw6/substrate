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

// Package envoycap measures the latency-versus-offered-load curve of a single
// atenet-router instance: an open-loop load generator, an exact-percentile
// pipeline, and the rig telemetry needed to tell "the system ran out" from
// "the test rig ran out".
package envoycap

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"time"
)

// spinThreshold is how long before a request's scheduled send time the
// dispatcher stops sleeping and busy-waits instead.
//
// At the top of the ladder the inter-arrival gap is 500us, which is close
// enough to the OS timer granularity that sleeping the whole way would add a
// systematic delay to every request and quietly depress the achieved rate.
// Spinning the last stretch costs at most spinThreshold x rate of a core
// (~0.3 of one core at 2000 QPS); that is charged to the load generator's own
// CPU guard, which is why the guard's threshold is set well above it.
const spinThreshold = 150 * time.Microsecond

// Ladder returns the offered rates of a linear ramp from startQPS to maxQPS in
// steps rungs.
//
// Linear, not multiplicative: the deliverable is a curve, and evenly spaced
// x-values are what a curve wants. Multiplicative stepping is for
// binary-searching an unknown ceiling, which is not what this measures.
func Ladder(startQPS, maxQPS float64, steps int) ([]float64, error) {
	switch {
	case steps < 1:
		return nil, fmt.Errorf("steps must be >= 1, got %d", steps)
	case startQPS <= 0:
		return nil, fmt.Errorf("start-qps must be > 0, got %g", startQPS)
	case maxQPS < startQPS:
		return nil, fmt.Errorf("max-qps (%g) must be >= start-qps (%g)", maxQPS, startQPS)
	}
	if steps == 1 {
		return []float64{startQPS}, nil
	}

	inc := (maxQPS - startQPS) / float64(steps-1)
	rates := make([]float64, steps)
	for i := range rates {
		// Computed from the start each time rather than accumulated, so the
		// rungs cannot drift.
		rates[i] = startQPS + inc*float64(i)
	}
	// Float arithmetic can leave the last rung a hair short of maxQPS; the
	// chart's x-axis should read back the number the operator asked for.
	rates[steps-1] = maxQPS
	return rates, nil
}

// scheduleOffset returns how far request i sits from the start of its step.
//
// scheduled(i) = stepStart + i/rate, computed arithmetically from the step's
// start. Drift-free (no accumulated error) and non-bursty (no token bucket, no
// burst allowance — we are not testing burst behavior).
func scheduleOffset(i int64, rate float64) time.Duration {
	return time.Duration(math.Round(float64(i) / rate * float64(time.Second)))
}

// RequestCount is how many requests a step of the given rate and duration
// offers.
func RequestCount(rate float64, d time.Duration) int64 {
	return int64(math.Round(rate * d.Seconds()))
}

// Request is one paced arrival handed from the dispatcher to a worker.
type Request struct {
	// Seq is the request's index within its step, from 0.
	Seq int64
	// ScheduledAt is when the request was due to be sent. Latency is measured
	// from here, never from when a worker actually picked it up — that is what
	// keeps coordinated omission out of the numbers.
	ScheduledAt time.Time
	// ActorIdx indexes the actor pool, assigned round-robin by the dispatcher
	// so the distribution cannot drift.
	ActorIdx int
}

// Dispatch paces count requests onto out, one every 1/rate seconds measured
// from stepStart, and returns when they have all been handed over.
//
// It never drops and never skips. If out is full it blocks, and the resulting
// lateness shows up twice: as the dispatch lag a worker records, and inside
// the latency number itself. A generator that silently skipped arrivals when
// it fell behind would report a struggling server as a healthy one.
func Dispatch(ctx context.Context, stepStart time.Time, rate float64, count int64, numActors int, out chan<- Request) error {
	if numActors < 1 {
		return fmt.Errorf("numActors must be >= 1, got %d", numActors)
	}
	if rate <= 0 {
		return fmt.Errorf("rate must be > 0, got %g", rate)
	}

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()

	for i := int64(0); i < count; i++ {
		at := stepStart.Add(scheduleOffset(i, rate))
		if err := waitUntil(ctx, timer, at); err != nil {
			return err
		}
		select {
		case out <- Request{Seq: i, ScheduledAt: at, ActorIdx: int(i % int64(numActors))}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// waitUntil blocks until at, or until ctx is done. Already-late deadlines
// return immediately, so a dispatcher that has fallen behind catches up
// instead of stretching the step.
func waitUntil(ctx context.Context, timer *time.Timer, at time.Time) error {
	if d := time.Until(at) - spinThreshold; d > 0 {
		timer.Reset(d)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	for time.Now().Before(at) {
		if err := ctx.Err(); err != nil {
			return err
		}
		runtime.Gosched()
	}
	return nil
}
