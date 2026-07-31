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
	"slices"
	"testing"
	"time"
)

func TestLadder(t *testing.T) {
	tests := []struct {
		name    string
		start   float64
		max     float64
		steps   int
		want    []float64
		wantErr bool
	}{
		{
			name:  "default ladder is linear with even spacing",
			start: 250, max: 2000, steps: 8,
			want: []float64{250, 500, 750, 1000, 1250, 1500, 1750, 2000},
		},
		{
			name:  "smoke ladder",
			start: 10, max: 20, steps: 2,
			want: []float64{10, 20},
		},
		{
			name:  "single step is the start rate",
			start: 100, max: 900, steps: 1,
			want: []float64{100},
		},
		{
			name:  "start equal to max repeats the rate",
			start: 500, max: 500, steps: 3,
			want: []float64{500, 500, 500},
		},
		{
			name:  "rates that do not divide evenly still land exactly on max",
			start: 100, max: 1000, steps: 4,
			want: []float64{100, 400, 700, 1000},
		},
		{name: "zero steps is an error", start: 250, max: 2000, steps: 0, wantErr: true},
		{name: "zero start is an error", start: 0, max: 2000, steps: 8, wantErr: true},
		{name: "max below start is an error", start: 2000, max: 250, steps: 8, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Ladder(tc.start, tc.max, tc.steps)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Ladder(%g, %g, %d) = %v, want error", tc.start, tc.max, tc.steps, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Ladder(%g, %g, %d) returned unexpected error: %v", tc.start, tc.max, tc.steps, err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("Ladder(%g, %g, %d) = %v, want %v", tc.start, tc.max, tc.steps, got, tc.want)
			}
		})
	}
}

// TestScheduleOffsetIsDriftFree checks the property the whole open loop rests
// on: the i-th arrival sits exactly i/rate from the step start, however far
// into the step it is. An accumulated schedule would slowly slip and quietly
// lower the offered rate.
func TestScheduleOffsetIsDriftFree(t *testing.T) {
	rates := []float64{250, 333, 1000, 1750, 2000}
	for _, rate := range rates {
		for _, i := range []int64{0, 1, 7, 1000, 12345, 60000} {
			want := time.Duration(float64(i) / rate * float64(time.Second))
			got := scheduleOffset(i, rate)
			// Allow a nanosecond for the rounding to integer duration; the
			// point is that error does not grow with i.
			if diff := got - want; diff > time.Nanosecond || diff < -time.Nanosecond {
				t.Errorf("scheduleOffset(%d, %g) = %v, want ~%v", i, rate, got, want)
			}
		}
	}
}

func TestScheduleOffsetIsMonotonicAndNonBursty(t *testing.T) {
	const rate = 2000.0
	const n = 5000
	wantGap := time.Duration(float64(time.Second) / rate)

	prev := scheduleOffset(0, rate)
	if prev != 0 {
		t.Fatalf("scheduleOffset(0, %g) = %v, want 0", rate, prev)
	}
	for i := int64(1); i < n; i++ {
		cur := scheduleOffset(i, rate)
		gap := cur - prev
		if gap <= 0 {
			t.Fatalf("schedule went backwards at i=%d: %v then %v", i, prev, cur)
		}
		// Even pacing, not bursts: every gap is the nominal inter-arrival time,
		// give or take integer-nanosecond rounding. A token bucket with a burst
		// allowance would produce runs of zero-length gaps here.
		if diff := gap - wantGap; diff > time.Nanosecond || diff < -time.Nanosecond {
			t.Fatalf("gap at i=%d = %v, want ~%v", i, gap, wantGap)
		}
		prev = cur
	}
}

func TestRequestCount(t *testing.T) {
	tests := []struct {
		name string
		rate float64
		dur  time.Duration
		want int64
	}{
		{name: "whole rate over whole seconds", rate: 250, dur: 30 * time.Second, want: 7500},
		{name: "top of the default ladder", rate: 2000, dur: 30 * time.Second, want: 60000},
		{name: "fractional rate rounds", rate: 333.5, dur: 2 * time.Second, want: 667},
		{name: "sub-second duration", rate: 1000, dur: 250 * time.Millisecond, want: 250},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := RequestCount(tc.rate, tc.dur); got != tc.want {
				t.Errorf("RequestCount(%g, %v) = %d, want %d", tc.rate, tc.dur, got, tc.want)
			}
		})
	}
}

// TestDispatchRoundRobin checks that every actor gets an equal share over a
// whole number of cycles. Clustering would mean the run measures the workers
// rather than the router.
func TestDispatchRoundRobin(t *testing.T) {
	const actors = 7
	const count = actors * 13

	got := collectDispatch(t, 100000, count, actors)
	if len(got) != count {
		t.Fatalf("dispatched %d requests, want %d", len(got), count)
	}

	perActor := make([]int, actors)
	for i, req := range got {
		if req.Seq != int64(i) {
			t.Fatalf("request %d carried Seq %d, want %d (order must be preserved)", i, req.Seq, i)
		}
		perActor[req.ActorIdx]++
	}
	for idx, n := range perActor {
		if n != count/actors {
			t.Errorf("actor %d got %d requests, want %d", idx, n, count/actors)
		}
	}
}

// TestDispatchNeverSkips is the no-coordinated-omission property at the
// dispatcher: a consumer slower than the offered rate must still receive every
// scheduled request, late, rather than have arrivals silently dropped.
func TestDispatchNeverSkips(t *testing.T) {
	const count = 50
	ctx := context.Background()
	reqs := make(chan Request) // unbuffered: the consumer sets the pace

	errCh := make(chan error, 1)
	go func() {
		// 10 kQPS nominal against a consumer that takes ~1 ms per request.
		errCh <- Dispatch(ctx, time.Now(), 10000, count, 3, reqs)
		close(reqs)
	}()

	var seen []int64
	for req := range reqs {
		time.Sleep(time.Millisecond)
		seen = append(seen, req.Seq)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}
	if len(seen) != count {
		t.Fatalf("received %d requests, want %d: the dispatcher dropped arrivals", len(seen), count)
	}
	for i, seq := range seen {
		if seq != int64(i) {
			t.Fatalf("received Seq %d at position %d: the dispatcher skipped ahead", seq, i)
		}
	}
}

func TestDispatchStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reqs := make(chan Request, 8)

	errCh := make(chan error, 1)
	go func() {
		errCh <- Dispatch(ctx, time.Now(), 10, 1000, 2, reqs)
	}()

	// Let a couple through, then cancel; the dispatcher must return promptly.
	<-reqs
	cancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Dispatch returned nil after cancellation, want a context error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dispatch did not return within 5s of cancellation")
	}
}

func TestDispatchRejectsBadArgs(t *testing.T) {
	tests := []struct {
		name   string
		rate   float64
		actors int
	}{
		{name: "no actors", rate: 100, actors: 0},
		{name: "negative actors", rate: 100, actors: -1},
		{name: "zero rate", rate: 0, actors: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := make(chan Request, 1)
			if err := Dispatch(context.Background(), time.Now(), tc.rate, 1, tc.actors, out); err == nil {
				t.Errorf("Dispatch(rate=%g, actors=%d) = nil, want error", tc.rate, tc.actors)
			}
		})
	}
}

// collectDispatch runs a dispatch to completion against a fast consumer.
func collectDispatch(t *testing.T, rate float64, count int64, actors int) []Request {
	t.Helper()
	reqs := make(chan Request, count)
	if err := Dispatch(context.Background(), time.Now(), rate, count, actors, reqs); err != nil {
		t.Fatalf("Dispatch returned unexpected error: %v", err)
	}
	close(reqs)
	var got []Request
	for req := range reqs {
		got = append(got, req)
	}
	return got
}
