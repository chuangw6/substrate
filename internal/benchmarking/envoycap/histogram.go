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
	"math"
	"slices"
)

// Histogram is a cumulative Prometheus histogram, reduced to what the step
// report needs.
//
// These are the server's own view of latency (Envoy's downstream_rq_time and
// the router's route.duration). They are bucketed, so their percentiles are
// approximations — unlike the client-side numbers, which come from raw
// samples. They exist to attribute latency to a layer, not to be the headline.
type Histogram struct {
	// Bounds are the finite bucket upper bounds, ascending.
	Bounds []float64
	// Cumulative[i] is the count of observations <= Bounds[i].
	Cumulative []float64
	// Count and Sum are the histogram totals.
	Count float64
	Sum   float64
}

// Sub returns the delta between this snapshot and an earlier one, so a step's
// numbers are not diluted by everything the process did before it.
// Bucket-boundary mismatches (a scrape that saw a different bucket set) yield
// nil rather than a silently wrong answer.
func (h *Histogram) Sub(prev *Histogram) *Histogram {
	if h == nil {
		return nil
	}
	if prev == nil {
		return h
	}
	if !slices.Equal(h.Bounds, prev.Bounds) {
		return nil
	}
	out := &Histogram{
		Bounds:     slices.Clone(h.Bounds),
		Cumulative: make([]float64, len(h.Cumulative)),
		Count:      h.Count - prev.Count,
		Sum:        h.Sum - prev.Sum,
	}
	for i := range h.Cumulative {
		out.Cumulative[i] = h.Cumulative[i] - prev.Cumulative[i]
	}
	return out
}

// Quantile estimates the q-quantile by linear interpolation inside the
// bracketing bucket, the same approximation histogram_quantile uses.
//
// Observations that fell past the last finite bound cannot be placed, so the
// result saturates there; that is a floor on the true value, not an estimate
// of it.
func (h *Histogram) Quantile(q float64) float64 {
	if h == nil || h.Count <= 0 || len(h.Bounds) == 0 {
		return math.NaN()
	}
	want := q * h.Count
	prevBound, prevCum := 0.0, 0.0
	for i, bound := range h.Bounds {
		cum := h.Cumulative[i]
		if cum >= want {
			if cum == prevCum {
				return bound
			}
			return prevBound + (bound-prevBound)*(want-prevCum)/(cum-prevCum)
		}
		prevBound, prevCum = bound, cum
	}
	return h.Bounds[len(h.Bounds)-1]
}

// Mean returns Sum/Count, or NaN for an empty histogram.
func (h *Histogram) Mean() float64 {
	if h == nil || h.Count <= 0 {
		return math.NaN()
	}
	return h.Sum / h.Count
}

// Scale returns a copy with every bound, and the sum, multiplied by f — used
// to put the router's seconds-based histogram on the same millisecond axis as
// everything else.
func (h *Histogram) Scale(f float64) *Histogram {
	if h == nil {
		return nil
	}
	out := &Histogram{
		Bounds:     make([]float64, len(h.Bounds)),
		Cumulative: slices.Clone(h.Cumulative),
		Count:      h.Count,
		Sum:        h.Sum * f,
	}
	for i, b := range h.Bounds {
		out.Bounds[i] = b * f
	}
	return out
}
