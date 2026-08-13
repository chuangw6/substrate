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

// The Sample record: one window of load, latency, CPU and memory, aligned.

package routercap

import (
	"sort"
	"time"
)

// Roles name the containers the run watches, so the output is keyed by what a
// container is rather than by a pod name that changes on every rollout.
const (
	RoleEnvoy        = "envoy"
	RoleSidecar      = "atenet-router"
	RoleLoadgen      = "loadgen"
	RoleControlPlane = "ate-system"
	RoleWorker       = "worker"
)

// Target is one container the sampler watches and the role it plays.
type Target struct {
	Role string
	Key  ContainerKey
}

// GroupUsage aggregates a role that has many containers — the control plane and
// the worker pods. Sums for the resources, maxima for throttling: averaging a
// single throttled container across a dozen idle ones would hide it.
type GroupUsage struct {
	Containers int     `json:"containers"`
	CPUCores   float64 `json:"cpu_cores"`
	// CPUUtilizationMax is the worst single container's use against its own
	// limit. The sum would be meaningless across differently-sized containers.
	CPUUtilizationMax     float64 `json:"cpu_utilization_max"`
	CPUUtilizationMaxOf   string  `json:"cpu_utilization_max_of,omitempty"`
	MemoryWorkingSetBytes float64 `json:"memory_working_set_bytes"`
	ThrottledPeriods      float64 `json:"throttled_periods"`
	ThrottledSeconds      float64 `json:"throttled_seconds"`
	ThrottledFractionMax  float64 `json:"throttled_fraction_max"`
	ThrottledMaxOf        string  `json:"throttled_max_of,omitempty"`
}

func aggregate(us []ContainerUsage) GroupUsage {
	g := GroupUsage{Containers: len(us)}
	for _, u := range us {
		g.CPUCores += u.CPUCores
		g.MemoryWorkingSetBytes += u.MemoryWorkingSetBytes
		g.ThrottledPeriods += u.ThrottledPeriods
		g.ThrottledSeconds += u.ThrottledSeconds
		if u.CPUUtilization > g.CPUUtilizationMax {
			g.CPUUtilizationMax, g.CPUUtilizationMaxOf = u.CPUUtilization, u.Container
		}
		if u.ThrottledFraction > g.ThrottledFractionMax {
			g.ThrottledFractionMax, g.ThrottledMaxOf = u.ThrottledFraction, u.Container
		}
	}
	return g
}

// Sample is everything true of one interval, from every source, over the same
// [T0, T1). It is the run's internal record; StatsRecord is the flattened
// subset that reaches stats.jsonl. Fields the output drops are still here
// because the guards read them.
type Sample struct {
	CPULimitCores int `json:"cpu_limit_cores"`
	Rung          int `json:"rung"`
	// RungQPS is the rung's nominal rate. Load.OfferedQPS is what the schedule
	// actually asked for over this window, which differs at a rung boundary.
	RungQPS float64 `json:"rung_qps"`
	// Warmup marks a window inside a rung's discarded head, kept in the file
	// so an analysis can show the settling or exclude it.
	Warmup bool `json:"warmup"`

	T0            time.Time `json:"t0"`
	T1            time.Time `json:"t1"`
	WindowSeconds float64   `json:"window_seconds"`

	Load GenStats `json:"load"`
	// Client is the generator measuring its own transport. A generator
	// churning connections is heading for its own port wall, and that cliff
	// would be the rig's rather than the router's.
	Client ClientStats `json:"client"`

	// Containers holds the single-container roles, keyed by role.
	Containers map[string]ContainerUsage `json:"containers"`
	// Groups holds the many-container roles, keyed by role.
	Groups map[string]GroupUsage `json:"groups,omitempty"`

	Envoy  *EnvoyDelta  `json:"envoy,omitempty"`
	Router *RouterDelta `json:"router,omitempty"`
	// Spans divides the mean request across the hops of the request path. Nil
	// when Envoy reported no request-time samples for the window.
	Spans *LatencySpans `json:"spans,omitempty"`

	Guards []GuardTrip `json:"guards,omitempty"`
	// Missing names containers cAdvisor did not report this window. Present so
	// "the router used no CPU" and "we could not see the router" never look the
	// same in the output.
	Missing []string `json:"missing,omitempty"`
	// Errors are non-fatal problems encountered building this record.
	Errors []string `json:"errors,omitempty"`
}

// buildContainers resolves a window's container usage into the sample's roles.
// Usage is split into single roles and aggregated groups, and anything missing
// or unreadable is recorded on the sample rather than dropped.
func buildContainers(w Window, targets []Target) (containers map[string]ContainerUsage, groups map[string]GroupUsage, missing []string, errs []string) {
	keys := make([]ContainerKey, 0, len(targets))
	byKey := make(map[ContainerKey]string, len(targets))
	for _, t := range targets {
		keys = append(keys, t.Key)
		byKey[t.Key] = t.Role
	}
	usage, _, missingKeys, uerrs := w.Usage(keys)

	for _, k := range missingKeys {
		missing = append(missing, byKey[k]+"="+k.String())
	}
	sort.Strings(missing)
	for _, e := range uerrs {
		errs = append(errs, e.Error())
	}
	sort.Strings(errs)

	containers = map[string]ContainerUsage{}
	grouped := map[string][]ContainerUsage{}
	for k, u := range usage {
		role := byKey[k]
		switch role {
		case RoleControlPlane, RoleWorker:
			grouped[role] = append(grouped[role], u)
		default:
			containers[role] = u
		}
	}
	if len(grouped) > 0 {
		groups = map[string]GroupUsage{}
		for role, us := range grouped {
			groups[role] = aggregate(us)
		}
	}
	return containers, groups, missing, errs
}
