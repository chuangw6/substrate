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

// routercap measures one atenet-router configuration: one Envoy CPU limit, one
// ladder of offered load, one time series out. Changing the CPU limit is
// run.sh's job, so this binary needs no write access to the cluster.
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
	"github.com/agent-substrate/substrate/internal/benchmarking/routercap"
	"k8s.io/client-go/kubernetes"
)

// Exit codes. Distinct so automation can tell "we could not measure this" from
// "the router fell over" without parsing a log.
const (
	exitOK          = 0
	exitFailed      = 1
	exitInterrupted = 2
	exitRigLimited  = 3
	exitPreflight   = 4
)

type config struct {
	// What is being measured.
	cpuLimit int

	// Where things are.
	kubeconfig       string
	apiEndpoint      string
	routerNamespace  string
	routerSelector   string
	workerNamespace  string
	workerSelector   string
	loadgenPod       string
	loadgenNS        string
	loadgenNode      string
	loadgenContainer string

	// The ladder.
	ladder routercap.LadderSpec

	// The actor pool.
	atespace        string
	actors          int
	warmConcurrency int

	// The generator's transport.
	maxInFlight    int64
	requestTimeout time.Duration
	drainTimeout   time.Duration
	tickCap        time.Duration

	// Sampling.
	pollInterval time.Duration
	maxWait      time.Duration

	// Guards.
	guards routercap.GuardConfig

	// Output.
	outputDir       string
	name            string
	tag             string
	recordsToStdout bool
}

func main() {
	cfg := parseFlags()
	// Logs on stderr, records on stdout. Keeping them apart is what lets run.sh
	// treat the pod's stdout as a data stream rather than something to grep.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// After flags, before anything runs: the client-port guard's ceiling comes
	// from this pod's own source-port range, which the Job spec widens via
	// sysctl.
	cfg.guards.ResolveClientCeiling(os.ReadFile, slog.Default())

	code := run(cfg)
	os.Exit(code)
}

func parseFlags() *config {
	c := &config{guards: routercap.DefaultGuardConfig()}

	flag.IntVar(&c.cpuLimit, "cpu-limit", 0, "Envoy container CPU limit in cores. Stamped on every record, and checked against envoy_server_concurrency at startup: Envoy sizes its worker threads from the cgroup limit, so a mismatch means the patch did not take and the series would be mislabeled.")

	flag.StringVar(&c.kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only used when not running in-cluster.")
	flag.StringVar(&c.apiEndpoint, "api-endpoint", "dns:///api.ate-system.svc.cluster.local:443", "ateapi gRPC dial target, used to warm the actor pool.")
	flag.StringVar(&c.routerNamespace, "router-namespace", "ate-system", "Namespace of the router pod under test.")
	flag.StringVar(&c.routerSelector, "router-selector", "app=atenet-router", "Label selector for the router pod. Must match exactly one running pod: two means a rollout is in progress and measuring the blend would label two configurations as one.")
	flag.StringVar(&c.workerNamespace, "worker-namespace", "benchmark-workloads", "Namespace of the worker pods the actors run on.")
	flag.StringVar(&c.workerSelector, "worker-selector", "ate.dev/worker-pool", "Label selector for worker pods. Their CPU is recorded because atunnel now terminates mTLS in the request path.")
	flag.StringVar(&c.loadgenPod, "loadgen-pod", os.Getenv("POD_NAME"), "This pod's name, for measuring the load generator itself. Defaults to $POD_NAME.")
	flag.StringVar(&c.loadgenNS, "loadgen-namespace", os.Getenv("POD_NAMESPACE"), "This pod's namespace. Defaults to $POD_NAMESPACE.")
	flag.StringVar(&c.loadgenNode, "loadgen-node", os.Getenv("NODE_NAME"), "The node this pod runs on. Defaults to $NODE_NAME.")
	flag.StringVar(&c.loadgenContainer, "loadgen-container", "routercap", "This container's name, as cAdvisor reports it.")

	flag.Float64Var(&c.ladder.StartQPS, "start-qps", 1000, "First rung's offered rate.")
	flag.Float64Var(&c.ladder.StepQPS, "step-qps", 1000, "Rate added by each subsequent rung.")
	flag.IntVar(&c.ladder.Rungs, "rungs", 16, "Number of rungs. No early stop: the flat region above saturation is data.")
	flag.DurationVar(&c.ladder.Hold, "hold", 45*time.Second, "How long each rung runs.")
	flag.DurationVar(&c.ladder.Warmup, "warmup", 10*time.Second, "Leading part of each rung flagged as warmup. Still written: a rung's first seconds are where the connection pool grows.")

	flag.StringVar(&c.atespace, "atespace", "routercap", "Atespace the run's actors live in.")
	flag.IntVar(&c.actors, "actors", 100, "Actors to warm, one per worker pod. Every actor is created and resumed before the ladder starts, so every request measures the warm path.")
	flag.IntVar(&c.warmConcurrency, "warm-concurrency", 16, "Parallelism for actor setup and teardown.")

	flag.Int64Var(&c.maxInFlight, "max-in-flight", 70000, "Generator's own concurrency cap. Reaching it is a rig failure recorded as shed requests, never a statement about the router; set above the widened source-port budget (the Job spec's ip_local_port_range sysctl, 64,511 ports) so the router's limits bind first.")
	flag.DurationVar(&c.requestTimeout, "request-timeout", 30*time.Second, "Per-request timeout. Timeouts count as failures and contribute their full latency to the percentiles.")
	flag.DurationVar(&c.drainTimeout, "drain-timeout", 30*time.Second, "How long to wait for in-flight requests after the last rung.")
	flag.DurationVar(&c.tickCap, "tick-cap", 2*time.Millisecond, "Upper bound on the pacer's sleep, which bounds the dispatch lag the dispatch loop itself can add.")

	flag.DurationVar(&c.pollInterval, "cadvisor-poll", time.Second, "How often to re-fetch cAdvisor while waiting for the anchor's timestamp to advance. Well below the kubelet's ~10s cadence so window boundaries are the kubelet's, not the poller's.")
	flag.DurationVar(&c.maxWait, "cadvisor-max-wait", 2*time.Minute, "How long a stuck cAdvisor timestamp is tolerated before the run fails.")

	flag.Float64Var(&c.guards.LoadgenCPUUtilization, "guard-loadgen-cpu", c.guards.LoadgenCPUUtilization, "Trip when the generator container exceeds this fraction of its own CPU limit. Zero disables.")
	flag.Float64Var(&c.guards.WorkerNewConnsPerSec, "guard-worker-conns-per-sec", c.guards.WorkerNewConnsPerSec, "Trip above this mean new-connection rate per worker pod. Zero disables.")
	flag.Float64Var(&c.guards.MinRequestsPerConnection, "guard-min-rq-per-cx", c.guards.MinRequestsPerConnection, "Trip when the generator averages fewer requests per connection than this, meaning keep-alive is not holding. Zero disables.")
	flag.IntVar(&c.guards.ClientConnectionCeiling, "guard-client-connections", c.guards.ClientConnectionCeiling, "Trip when the generator holds more connections than this, past its own source-port headroom. Zero disables.")
	flag.Float64Var(&c.guards.DispatchLagP95Ms, "guard-dispatch-lag-ms", c.guards.DispatchLagP95Ms, "Trip when the generator falls this far behind its own schedule at p95, unless the system is demonstrably saturated. Zero disables.")
	flag.Float64Var(&c.guards.SaturationLatencyP95Ms, "saturation-latency-p95-ms", c.guards.SaturationLatencyP95Ms, "p95 latency at or above which the system counts as saturated, suspending the dispatch-lag guard.")
	flag.Float64Var(&c.guards.SaturationAchievedRatio, "saturation-achieved-ratio", c.guards.SaturationAchievedRatio, "Achieved-over-offered ratio below which the system counts as saturated.")

	flag.StringVar(&c.outputDir, "output-dir", "", "Directory to write stats.jsonl into. Required unless --records-to-stdout is set.")
	flag.StringVar(&c.name, "name", "routercap", "Test name, stamped on every record.")
	flag.StringVar(&c.tag, "tag", "", "Run tag — the commit under test — stamped on every record.")
	flag.BoolVar(&c.recordsToStdout, "records-to-stdout", false, "Also write every record to stdout as tagged JSONL. The generator image is distroless, so kubectl cp cannot retrieve a Job's files; this is how an in-cluster run's output gets out. Logs go to stderr either way.")

	flag.Parse()
	return c
}

// run is main's body, returning an exit code rather than calling os.Exit, so
// every deferred teardown actually runs.
func run(cfg *config) int {
	log := slog.Default()

	if cfg.outputDir == "" && !cfg.recordsToStdout {
		log.Error("preflight failed", "err", "one of --output-dir or --records-to-stdout is required")
		return exitPreflight
	}

	// SIGTERM is how the Job is deleted and Ctrl-C is how a laptop run ends;
	// both must unwind through the actor teardown rather than abandon a
	// hundred running actors on the worker pods.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rig, err := setup(ctx, cfg, log)
	if rig != nil {
		defer rig.close(log)
	}
	if err != nil {
		log.Error("preflight failed", "err", err)
		return exitPreflight
	}

	var files *routercap.JSONLSink
	sinks := routercap.MultiSink{}
	if cfg.outputDir != "" {
		files, err = routercap.OpenJSONLSink(cfg.outputDir)
		if err != nil {
			log.Error("preflight failed", "err", err)
			return exitPreflight
		}
		defer files.Close()
		sinks = append(sinks, files)
	}
	if cfg.recordsToStdout {
		sinks = append(sinks, routercap.NewStreamSink(os.Stdout))
	}

	runner := &routercap.Runner{
		CPULimitCores: cfg.cpuLimit,
		Tag:           cfg.tag,
		TestName:      cfg.name,
		Rungs:         cfg.ladder.Build(),
		Client:        rig.sender,
		Sink:          sinks,
		Windows:       rig.windows,
		Envoy:         rig.envoy,
		Router:        rig.routerStats,
		Targets:       rig.targets,
		Guards:        cfg.guards,
		MaxInFlight:   cfg.maxInFlight,
		TickCap:       cfg.tickCap,
		DrainTimeout:  cfg.drainTimeout,
		Log:           log,
	}

	log.Info("run start",
		"cpu_limit_cores", cfg.cpuLimit, "rungs", cfg.ladder.Rungs,
		"peak_qps", cfg.ladder.PeakQPS(), "actors", cfg.actors, "output", cfg.outputDir)

	res, runErr := runner.Run(ctx)

	if files != nil {
		if err := files.Close(); err != nil {
			log.Error("close output file", "err", err)
		}
	}

	var rigErr *routercap.RigLimitedError
	switch {
	case errors.As(runErr, &rigErr):
		log.Error("run was rig-limited", "cpu_limit_cores", cfg.cpuLimit, "windows", res.Windows, "err", runErr)
		return exitRigLimited
	case errors.Is(runErr, context.Canceled):
		log.Warn("run interrupted", "cpu_limit_cores", cfg.cpuLimit, "windows", res.Windows)
		return exitInterrupted
	case runErr != nil:
		log.Error("run failed", "cpu_limit_cores", cfg.cpuLimit, "err", runErr)
		return exitFailed
	}

	log.Info("run complete",
		"cpu_limit_cores", cfg.cpuLimit, "windows", res.Windows,
		"drained", res.Drained, "clock_skew_ms", res.ClockSkewMs)
	return exitOK
}

// rig is everything the runner needs from the cluster, resolved once.
type rig struct {
	router      routercap.PodRef
	targets     []routercap.Target
	windows     *routercap.WindowDriver
	envoy       *routercap.EnvoyClient
	routerStats *routercap.RouterClient
	sender      *routercap.Sender
	pool        *routercap.ActorPool
	closers     []func() error
}

// close tears the rig down. Actors first and always: actors left running hold
// worker pods the next run needs, and the run exits through a signal far more
// often than through a clean finish.
func (r *rig) close(log *slog.Logger) {
	if r.pool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		r.pool.Teardown(ctx)
	}
	if r.sender != nil {
		r.sender.CloseIdleConnections()
	}
	for _, c := range r.closers {
		if err := c(); err != nil {
			log.Warn("close", "err", err)
		}
	}
}

// setup resolves every source and warms the actor pool. It returns a partially
// built rig even on failure so the caller can still tear down what was created.
func setup(ctx context.Context, cfg *config, log *slog.Logger) (*rig, error) {
	r := &rig{}

	cs, _, err := routercap.NewKubeClient(cfg.kubeconfig)
	if err != nil {
		return r, err
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	router, err := routercap.WaitForPod(waitCtx, cs, cfg.routerNamespace, cfg.routerSelector, 2*time.Second)
	if err != nil {
		return r, fmt.Errorf("resolve router pod: %w", err)
	}
	r.router = router
	log.Info("router pod", "pod", router.Name, "ip", router.IP, "node", router.Node,
		"image", router.Images[routercap.RoleEnvoy])

	targets, nodes, err := resolveTargets(ctx, cs, cfg, router, log)
	if err != nil {
		return r, err
	}
	r.targets = targets

	// The generator's own container is the one target whose absence would
	// disable the guard that matters most, so it is checked here.
	if !hasRole(targets, routercap.RoleLoadgen) {
		return r, fmt.Errorf("cannot identify this pod's own container: set --loadgen-pod/--loadgen-namespace/--loadgen-node or the POD_NAME/POD_NAMESPACE/NODE_NAME downward-API env vars, or the guard that measures the load generator cannot run")
	}

	r.windows = &routercap.WindowDriver{
		Client:       routercap.NewMultiNodeCadvisorClient(cs, nodes),
		Anchor:       router.Key(routercap.RoleEnvoy),
		PollInterval: cfg.pollInterval,
		MaxWait:      cfg.maxWait,
	}

	hc := routercap.NewScrapeHTTPClient()
	r.envoy = routercap.NewEnvoyClient(hc, router)
	r.routerStats = routercap.NewRouterClient(hc, router)

	if err := checkConcurrency(ctx, r.envoy, cfg.cpuLimit); err != nil {
		return r, err
	}

	conn, api, err := glutton.DialControl(cfg.apiEndpoint, false)
	if err != nil {
		return r, fmt.Errorf("dial ateapi: %w", err)
	}
	r.closers = append(r.closers, conn.Close)

	r.pool = &routercap.ActorPool{
		API:         api,
		Atespace:    cfg.atespace,
		Concurrency: cfg.warmConcurrency,
		Log:         log,
	}
	if err := r.pool.Warm(ctx, cfg.actors); err != nil {
		return r, err
	}

	r.sender, err = routercap.NewSender(routercap.SenderConfig{
		RouterURL: fmt.Sprintf("http://%s:8080", router.IP),
		Actors:    r.pool.Actors(),
		// The idle pool must hold the run's peak concurrency, or Go's idle
		// eviction churns connections at exactly the worst load.
		MaxConnections: int(cfg.maxInFlight),
		RequestTimeout: cfg.requestTimeout,
	})
	if err != nil {
		return r, err
	}
	return r, nil
}

// resolveTargets lists every container the sampler watches and every node whose
// cAdvisor has to be scraped to see them.
func resolveTargets(ctx context.Context, cs kubernetes.Interface, cfg *config, router routercap.PodRef, log *slog.Logger) ([]routercap.Target, []string, error) {
	var targets []routercap.Target
	nodes := []string{router.Node}

	for _, c := range router.Containers {
		if c != routercap.RoleEnvoy && c != routercap.RoleSidecar {
			continue
		}
		targets = append(targets, routercap.Target{Role: c, Key: router.Key(c)})
	}

	if cfg.loadgenPod != "" && cfg.loadgenNS != "" && cfg.loadgenNode != "" {
		targets = append(targets, routercap.Target{
			Role: routercap.RoleLoadgen,
			Key: routercap.ContainerKey{
				Namespace: cfg.loadgenNS, Pod: cfg.loadgenPod, Container: cfg.loadgenContainer,
			},
		})
		nodes = append(nodes, cfg.loadgenNode)
		if cfg.loadgenNode == router.Node {
			// Not fatal — a laptop-driven run against a one-node cluster is
			// still useful — but it changes what is being measured, so it must
			// not pass silently.
			log.Warn("load generator and router share a node; the isolation pools did not take and the two are competing for the same cores",
				"node", router.Node)
		}
	}

	// Every other pod in the router's namespace is the control plane, listed
	// rather than named so new ate-system pods stay covered. A throttled
	// ate-api-server is indistinguishable from a slow router at the client.
	cp, err := routercap.FindPods(ctx, cs, cfg.routerNamespace, "")
	if err != nil {
		return nil, nil, fmt.Errorf("list control-plane pods: %w", err)
	}
	for _, p := range cp {
		if p.Name == router.Name {
			continue
		}
		for _, c := range p.Containers {
			targets = append(targets, routercap.Target{Role: routercap.RoleControlPlane, Key: p.Key(c)})
		}
		nodes = append(nodes, p.Node)
	}

	workers, err := routercap.FindPods(ctx, cs, cfg.workerNamespace, cfg.workerSelector)
	if err != nil {
		return nil, nil, fmt.Errorf("list worker pods: %w", err)
	}
	for _, p := range workers {
		for _, c := range p.Containers {
			targets = append(targets, routercap.Target{Role: routercap.RoleWorker, Key: p.Key(c)})
		}
		nodes = append(nodes, p.Node)
	}
	// Counted, not configured. The per-worker connection-rate guard divides a
	// cluster-wide rate by this number, so a flag that disagreed with the
	// cluster would move the threshold without anyone noticing.
	if len(workers) > 0 {
		cfg.guards.WorkerPods = len(workers)
	}

	return targets, nodes, nil
}

func hasRole(targets []routercap.Target, role string) bool {
	for _, t := range targets {
		if t.Role == role {
			return true
		}
	}
	return false
}

// checkConcurrency confirms Envoy's worker-thread count matches the CPU limit
// under test. Envoy >= 1.37 sizes threads from the cgroup CPU limit, but only
// when the router manifest passes --cpuset-threads and no --concurrency; if
// that did not take, Envoy runs one event loop per *node* core and the run
// measures CFS throttling instead of the proxy.
//
// Equality rather than a tolerance because Envoy floors the quota, so the two
// agree exactly for a whole number of cores — which is the only thing run.sh
// will patch. A fractional limit would floor to something smaller and this
// would reject it, correctly: the run would be labeled with a thread count it
// is not running.
func checkConcurrency(ctx context.Context, c *routercap.EnvoyClient, want int) error {
	s, err := c.Scrape(ctx)
	if err != nil {
		return fmt.Errorf("scrape envoy admin: %w", err)
	}
	if want <= 0 {
		return nil
	}
	if int(s.Concurrency) != want {
		return fmt.Errorf("envoy_server_concurrency is %g but the CPU limit under test is %d core(s): Envoy did not size its worker threads from the cgroup, and this run would be labeled with a thread count it is not running",
			s.Concurrency, want)
	}
	return nil
}
