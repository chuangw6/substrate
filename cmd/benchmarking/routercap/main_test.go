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

// Tests for the startup checks and for the target list the binary assembles.

package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/benchmarking/routercap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestCheckConcurrencyRefusesAMislabeledRun(t *testing.T) {
	// Envoy sizes its worker threads from the cgroup CPU limit. If that default
	// did not take it uses the node's core count instead, so a 10-core run can
	// have 40 event loops and measure CFS throttling. Labeling that series
	// "10 cores" is worse than having no series.
	const body = "envoy_server_concurrency{} 40\n"
	c := &routercap.EnvoyClient{Fetch: func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(body)), nil
	}}

	if err := checkConcurrency(context.Background(), c, 40); err != nil {
		t.Errorf("matching CPU limit rejected: %v", err)
	}
	err := checkConcurrency(context.Background(), c, 10)
	if err == nil {
		t.Fatal("a 10-core limit running 40 Envoy workers was accepted")
	}
	if !strings.Contains(err.Error(), "40") || !strings.Contains(err.Error(), "10") {
		t.Errorf("error %q does not name both the observed and the intended concurrency", err)
	}
	// --cpu-limit unset is the smoke-test path: nothing was patched, so there
	// is nothing to disagree with.
	if err := checkConcurrency(context.Background(), c, 0); err != nil {
		t.Errorf("unset CPU limit rejected: %v", err)
	}
}

func testPod(ns, name, node string, containers ...string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name,
			Labels: map[string]string{"ate.dev/worker-pool": "benchmark-ateom", "app": name},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"},
		Spec:   corev1.PodSpec{NodeName: node},
	}
	for _, c := range containers {
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: c})
	}
	return p
}

func TestResolveTargetsCoversEveryNodeTheRunWatches(t *testing.T) {
	// A node missing from the scrape list leaves its guard permanently silent,
	// which reads exactly like the guard passing.
	router := routercap.PodRef{
		Namespace: "ate-system", Name: "atenet-router-7d9", IP: "10.0.0.5", Node: "router-node",
		Containers: []string{"envoy", "atenet-router"},
	}
	cs := fake.NewSimpleClientset(
		testPod("ate-system", "atenet-router-7d9", "router-node", "envoy", "atenet-router"),
		testPod("ate-system", "ate-api-server-1", "system-node", "ate-api-server"),
		testPod("benchmark-workloads", "worker-1", "worker-node-a", "ateom"),
		testPod("benchmark-workloads", "worker-2", "worker-node-b", "ateom"),
	)
	cfg := &config{
		routerNamespace:  "ate-system",
		workerNamespace:  "benchmark-workloads",
		workerSelector:   "ate.dev/worker-pool",
		loadgenPod:       "routercap-runner-abc",
		loadgenNS:        "benchmarking",
		loadgenNode:      "loadgen-node",
		loadgenContainer: "routercap",
		guards:           routercap.DefaultGuardConfig(),
	}

	targets, nodes, err := resolveTargets(context.Background(), cs, cfg, router, quietLog())
	if err != nil {
		t.Fatalf("resolveTargets: %v", err)
	}

	byRole := map[string]int{}
	for _, tg := range targets {
		byRole[tg.Role]++
	}
	want := map[string]int{
		routercap.RoleEnvoy:        1,
		routercap.RoleSidecar:      1,
		routercap.RoleLoadgen:      1,
		routercap.RoleControlPlane: 1, // the api-server; the router pod itself is excluded
		routercap.RoleWorker:       2,
	}
	for role, n := range want {
		if byRole[role] != n {
			t.Errorf("role %s has %d targets, want %d", role, byRole[role], n)
		}
	}

	set := map[string]bool{}
	for _, n := range nodes {
		set[n] = true
	}
	for _, n := range []string{"router-node", "system-node", "worker-node-a", "worker-node-b", "loadgen-node"} {
		if !set[n] {
			t.Errorf("node %s is not scraped, so every container on it is invisible", n)
		}
	}

	// Counted from the cluster, not configured: the per-worker connection-rate
	// guard divides a cluster-wide rate by this, so a stale flag would move the
	// threshold without anyone noticing.
	if cfg.guards.WorkerPods != 2 {
		t.Errorf("guard worker pods = %d, want the 2 that were found", cfg.guards.WorkerPods)
	}
}

func TestResolveTargetsWithoutTheDownwardAPI(t *testing.T) {
	// Run outside a pod with no --loadgen-* flags: the loadgen target is
	// absent, and setup turns that into a startup failure rather than a
	// silently disabled guard.
	router := routercap.PodRef{
		Namespace: "ate-system", Name: "r", IP: "10.0.0.5", Node: "router-node",
		Containers: []string{"envoy", "atenet-router"},
	}
	cs := fake.NewSimpleClientset(testPod("ate-system", "r", "router-node", "envoy", "atenet-router"))
	cfg := &config{routerNamespace: "ate-system", workerNamespace: "benchmark-workloads", guards: routercap.DefaultGuardConfig()}

	targets, _, err := resolveTargets(context.Background(), cs, cfg, router, quietLog())
	if err != nil {
		t.Fatalf("resolveTargets: %v", err)
	}
	if hasRole(targets, routercap.RoleLoadgen) {
		t.Error("a loadgen target was invented from nothing")
	}
}
