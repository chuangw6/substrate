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
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	oneToHundred := make([]float64, 100)
	for i := range oneToHundred {
		oneToHundred[i] = float64(i + 1)
	}

	tests := []struct {
		name   string
		sorted []float64
		q      float64
		want   float64
	}{
		{name: "p50 of 1..100", sorted: oneToHundred, q: 0.50, want: 50},
		{name: "p95 of 1..100", sorted: oneToHundred, q: 0.95, want: 95},
		{name: "p99 of 1..100", sorted: oneToHundred, q: 0.99, want: 99},
		{name: "max of 1..100", sorted: oneToHundred, q: 1.0, want: 100},
		{name: "single sample is every percentile", sorted: []float64{42}, q: 0.95, want: 42},
		{name: "all equal", sorted: []float64{7, 7, 7, 7}, q: 0.99, want: 7},
		{name: "empty is zero", sorted: nil, q: 0.95, want: 0},
		{name: "q at or below zero is the minimum", sorted: oneToHundred, q: 0, want: 1},
		{name: "q above one is the maximum", sorted: oneToHundred, q: 1.5, want: 100},
		{name: "nearest-rank rounds up on a short slice", sorted: []float64{1, 2, 3}, q: 0.95, want: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Percentile(tc.sorted, tc.q); got != tc.want {
				t.Errorf("Percentile(%v, %g) = %g, want %g", tc.sorted, tc.q, got, tc.want)
			}
		})
	}
}

func TestSummarizeSortsAndReportsAllFour(t *testing.T) {
	// Deliberately unsorted on the way in.
	values := []float64{100, 3, 50, 1, 99, 2, 95, 4, 5, 6}
	got := Summarize(values)
	// Sorted: 1 2 3 4 5 6 50 95 99 100. Nearest rank puts p50 at index
	// ceil(0.5*10)-1 = 4.
	want := Quantiles{P50: 5, P95: 100, P99: 100, Max: 100}
	if got != want {
		t.Errorf("Summarize = %+v, want %+v", got, want)
	}
	for i := 1; i < len(values); i++ {
		if values[i-1] > values[i] {
			t.Fatalf("Summarize did not sort its input: %v", values)
		}
	}
}

func TestAfterWarmup(t *testing.T) {
	start := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	warmup := 8 * time.Second

	// One sample per second for 30 s, tagged by its offset.
	samples := make([]Sample, 30)
	for i := range samples {
		samples[i] = Sample{
			ScheduledAt: start.Add(time.Duration(i) * time.Second),
			// Completion is deliberately far in the future for the last
			// warmup sample: a request offered during warmup must not join the
			// window just because it landed late.
			CompletedAt: start.Add(time.Duration(i)*time.Second + 20*time.Second),
			LatencyMS:   float64(i),
		}
	}

	got := AfterWarmup(samples, start, warmup)
	if len(got) != 22 {
		t.Fatalf("AfterWarmup kept %d samples, want 22 (30 offered, 8 s discarded)", len(got))
	}
	if got[0].LatencyMS != 8 {
		t.Errorf("first kept sample is offset %g s, want 8: the boundary sample must be kept", got[0].LatencyMS)
	}
	if got[len(got)-1].LatencyMS != 29 {
		t.Errorf("last kept sample is offset %g s, want 29", got[len(got)-1].LatencyMS)
	}
}

func TestAfterWarmupEdgeCases(t *testing.T) {
	start := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		samples []Sample
		warmup  time.Duration
		want    int
	}{
		{name: "no warmup keeps everything", samples: []Sample{{ScheduledAt: start}}, warmup: 0, want: 1},
		{
			name:    "warmup longer than the step keeps nothing",
			samples: []Sample{{ScheduledAt: start}, {ScheduledAt: start.Add(time.Second)}},
			warmup:  time.Minute,
			want:    0,
		},
		{name: "no samples", samples: nil, warmup: 8 * time.Second, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := len(AfterWarmup(tc.samples, start, tc.warmup)); got != tc.want {
				t.Errorf("AfterWarmup kept %d samples, want %d", got, tc.want)
			}
		})
	}
}

func TestMSFloatKeepsSubMillisecondPrecision(t *testing.T) {
	// The existing boomer metrics path truncates to whole milliseconds, which
	// erases the difference between a 3.1 ms and a 3.9 ms floor.
	if got := MSFloat(3_900_000 * time.Nanosecond); got != 3.9 {
		t.Errorf("MSFloat(3.9ms) = %g, want 3.9", got)
	}
	if got := MSFloat(250 * time.Microsecond); got != 0.25 {
		t.Errorf("MSFloat(250us) = %g, want 0.25", got)
	}
}
