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

// envoycap measures the latency-versus-offered-load curve of one
// atenet-router instance and reports the offered rate at which p95 latency
// crosses the 500 ms budget.
//
// It is an open-loop generator: the arrival rate is the input and latency is
// the output. Requests go out on a fixed schedule whether or not earlier ones
// have come back, and latency is measured from each request's scheduled send
// time. A closed loop would stop offering load exactly when the server slows
// down, which makes a struggling server look healthy.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/glutton"
	"github.com/agent-substrate/substrate/internal/benchmarking/envoycap"
)

// teardownBudget bounds the suspend+delete of the actor pool on the way out.
// It must fit inside the Job's terminationGracePeriodSeconds: a killed run
// that leaks actors leaves them holding worker slots.
const teardownBudget = 120 * time.Second

// Exit codes. Distinct so the driver script can tell the three failure modes
// apart without parsing logs.
const (
	exitOK          = 0
	exitError       = 1
	exitInterrupted = 2
	exitRigLimited  = 3
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		// The six experiment knobs.
		actors       = flag.Int("actors", 40, "Number of glutton actors to create, resume, and round-robin over. Capped by WorkerPool replicas: one actor per worker pod.")
		startQPS     = flag.Float64("start-qps", 250, "Offered rate of the first rung of the ladder.")
		maxQPS       = flag.Float64("max-qps", 2000, "Offered rate of the last rung of the ladder.")
		steps        = flag.Int("steps", 8, "Number of evenly spaced rungs from --start-qps to --max-qps.")
		stepDuration = flag.Duration("step-duration", 30*time.Second, "How long to hold each rung, including the warmup that gets discarded.")
		repeat       = flag.Int("repeat", 3, "How many times to walk the whole ladder. A single pass visits each rate once and cannot tell a rate that reliably holds a latency from one that varies.")

		// Plumbing, same names and defaults as the existing boomer rig.
		apiEndpoint = flag.String("api-endpoint", "dns:///api.ate-system.svc.cluster.local:443", "ateapi gRPC dial target.")
		routerURL   = flag.String("router-url", "http://atenet-router.ate-system.svc.cluster.local", "atenet HTTP router base URL (no trailing slash).")
		atespace    = flag.String("atespace", "benchmark", "Atespace the run's actors live in.")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(log)

	conn, stub, err := glutton.DialControl(*apiEndpoint)
	if err != nil {
		log.Error("failed to dial ateapi", slog.String("err", err.Error()))
		return exitError
	}
	defer conn.Close()

	runner := envoycap.NewRunner(envoycap.Config{
		Actors:       *actors,
		StartQPS:     *startQPS,
		MaxQPS:       *maxQPS,
		Steps:        *steps,
		StepDuration: *stepDuration,
		Repeat:       *repeat,
		Atespace:     *atespace,
		RouterURL:    *routerURL,
		// Injected by the driver script. The Envoy admin port and the router's
		// metrics port are on the pod but not in the Service, and the
		// deployment is measured as shipped, so they are reached by pod IP.
		RouterPodIP: os.Getenv("ROUTER_POD_IP"),
		RouterNode:  os.Getenv("ROUTER_NODE"),
		LoadgenNode: os.Getenv("NODE_NAME"),
		Cluster:     os.Getenv("CLUSTER_NAME"),
		GitSHA:      os.Getenv("GIT_SHA"),
		Image:       os.Getenv("IMAGE"),
		Stub:        stub,
		Logger:      log,
	})

	// SIGTERM cancels the ladder but not the teardown: the pool must be
	// suspended and deleted even on a killed run.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	defer func() {
		tdCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownBudget)
		defer cancel()
		runner.Teardown(tdCtx)
	}()

	if err := runner.Setup(ctx); err != nil {
		log.Error("setup failed", slog.String("err", err.Error()))
		return exitError
	}

	report, runErr := runner.Run(ctx)
	if report != nil {
		if err := report.WriteTable(os.Stdout); err != nil {
			log.Error("failed to write step table", slog.String("err", err.Error()))
		}
		fmt.Fprintln(os.Stdout)
		printCrossings(report)
		fmt.Fprintln(os.Stdout)
		if err := report.WriteJSON(os.Stdout); err != nil {
			log.Error("failed to write report JSON", slog.String("err", err.Error()))
			return exitError
		}
	}

	switch {
	case runErr == nil:
		return exitOK
	case errors.Is(runErr, envoycap.ErrRigLimited):
		// The report is still valid and still worth harvesting; the exit code
		// says plainly that the rig, not the system, is what ran out.
		return exitRigLimited
	case errors.Is(runErr, context.Canceled):
		log.Warn("run interrupted; report covers the steps that completed")
		return exitInterrupted
	default:
		log.Error("run failed", slog.String("err", runErr.Error()))
		return exitError
	}
}

// printCrossings reports the answer the measurement exists to produce, per
// pass, so a scattered set of crossings is visible on stdout and not only in
// the chart overlay.
func printCrossings(report *envoycap.Report) {
	byPass := map[int][]envoycap.StepReport{}
	var order []int
	for _, s := range report.Steps {
		if _, ok := byPass[s.Repeat]; !ok {
			order = append(order, s.Repeat)
		}
		byPass[s.Repeat] = append(byPass[s.Repeat], s)
	}
	fmt.Fprintf(os.Stdout, "p95 crosses %.0f ms at:\n", envoycap.BudgetMS)
	for _, pass := range order {
		if qps := envoycap.P95CrossingQPS(byPass[pass], envoycap.BudgetMS); qps > 0 {
			fmt.Fprintf(os.Stdout, "  pass %d: %.0f QPS\n", pass, qps)
		} else {
			fmt.Fprintf(os.Stdout, "  pass %d: not reached at or below %.0f QPS\n", pass, report.Run.Flags.MaxQPS)
		}
	}
}
