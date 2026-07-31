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
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// templateNS and templateName address the glutton ActorTemplate installed
	// by benchmarking/workloads/manifests/workloads.yaml.tmpl.
	templateNS   = "benchmark-workloads"
	templateName = "glutton"

	// actorDomain is the suffix atenet's ext_proc filter parses out of the
	// Host header to find the actor.
	actorDomain = "actors.resources.substrate.ate.dev"

	// settleInterval is how often the readiness loop retries an actor that is
	// resumed but not yet accepting connections.
	settleInterval = 250 * time.Millisecond
)

// Actor is one resumed glutton actor in the pool.
type Actor struct {
	// Name is the actor's name within the atespace.
	Name string `json:"name"`
	// PodIP is the worker pod the scheduler placed it on, from the
	// ResumeActor reply. One actor per worker pod is a hard cap upstream, so
	// these should all be distinct — and distinct destination IPs are what
	// give the rig its ephemeral-port headroom.
	PodIP string `json:"pod_ip"`
	// Host is the Host header that routes to this actor.
	Host string `json:"-"`
}

// PoolConfig configures actor setup.
type PoolConfig struct {
	Stub     ateapipb.ControlClient
	Atespace string
	Count    int
	Pinger   *Pinger
	Logger   *slog.Logger
}

// Pool owns the actors for a run: created and resumed once during setup, left
// running for the whole ladder so every step measures steady-state ping
// throughput rather than cold-start cost, then suspended and deleted on exit.
type Pool struct {
	cfg PoolConfig

	mu      sync.Mutex
	created []string // every actor we called CreateActor for, for teardown
	actors  []Actor  // successfully resumed, in stable index order
}

// NewPool returns an empty pool. Call Setup to populate it.
func NewPool(cfg PoolConfig) *Pool {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Pool{cfg: cfg}
}

// Actors returns the resumed actors in round-robin index order.
func (p *Pool) Actors() []Actor {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Actor(nil), p.actors...)
}

// PodIPs returns the distinct worker pod IPs backing the pool.
func (p *Pool) PodIPs() []string {
	seen := map[string]struct{}{}
	var out []string
	for _, a := range p.Actors() {
		if _, ok := seen[a.PodIP]; ok {
			continue
		}
		seen[a.PodIP] = struct{}{}
		out = append(out, a.PodIP)
	}
	return out
}

// Setup ensures the atespace, then creates and resumes every actor at once.
//
// There is no separate concurrency knob: the actors are the parallelism. Any
// actor that fails to resume fails the whole run — a short pool would quietly
// concentrate load on fewer worker pods and turn a router measurement into a
// worker measurement.
func (p *Pool) Setup(ctx context.Context) error {
	if p.cfg.Count < 1 {
		return fmt.Errorf("actors must be >= 1, got %d", p.cfg.Count)
	}
	if err := p.ensureAtespace(ctx); err != nil {
		return err
	}

	runID := uuid.NewString()[:8]
	names := make([]string, p.cfg.Count)
	for i := range names {
		names[i] = fmt.Sprintf("envoycap-%s-%04d", runID, i)
	}

	p.mu.Lock()
	p.created = append(p.created, names...)
	p.mu.Unlock()

	start := time.Now()
	var createGrp errgroup.Group
	for _, name := range names {
		createGrp.Go(func() error {
			_, err := p.cfg.Stub.CreateActor(ctx, &ateapipb.CreateActorRequest{
				Actor: &ateapipb.Actor{
					Metadata:               &ateapipb.ResourceMetadata{Atespace: p.cfg.Atespace, Name: name},
					ActorTemplateNamespace: templateNS,
					ActorTemplateName:      templateName,
				},
			})
			if err != nil {
				return fmt.Errorf("CreateActor %s: %w", name, err)
			}
			return nil
		})
	}
	if err := createGrp.Wait(); err != nil {
		return err
	}
	p.cfg.Logger.Info("actors created", slog.Int("count", len(names)),
		slog.Duration("elapsed", time.Since(start)))

	start = time.Now()
	resumed := make([]Actor, len(names))
	var resumeGrp errgroup.Group
	for i, name := range names {
		resumeGrp.Go(func() error {
			// Boot: true skips the golden snapshot and boots the workload from
			// scratch — the same first-resume behavior the existing boomer rig
			// uses, and known to work with this template. Setup is not measured,
			// so the extra seconds cost nothing.
			resp, err := p.cfg.Stub.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
				Actor: p.ref(name),
				Boot:  true,
			})
			if err != nil {
				return fmt.Errorf("ResumeActor %s: %w", name, err)
			}
			actor := resp.GetActor()
			if actor == nil {
				return fmt.Errorf("ResumeActor %s: reply carried no actor", name)
			}
			if actor.Status != ateapipb.Actor_STATUS_RUNNING {
				return fmt.Errorf("ResumeActor %s: status %s, want STATUS_RUNNING", name, actor.Status)
			}
			if actor.AteomPodIp == "" {
				return fmt.Errorf("ResumeActor %s: reply carried no ateom_pod_ip", name)
			}
			resumed[i] = Actor{
				Name:  name,
				PodIP: actor.AteomPodIp,
				Host:  strings.Join([]string{name, p.cfg.Atespace, actorDomain}, "."),
			}
			return nil
		})
	}
	if err := resumeGrp.Wait(); err != nil {
		return err
	}

	p.mu.Lock()
	p.actors = resumed
	p.mu.Unlock()

	ips := p.PodIPs()
	p.cfg.Logger.Info("actors resumed", slog.Int("count", len(resumed)),
		slog.Int("distinct_pod_ips", len(ips)),
		slog.Duration("elapsed", time.Since(start)))
	if len(ips) != len(resumed) {
		// Not fatal — but it lowers the rig's per-destination-IP port ceiling,
		// so it has to be visible rather than inferred from the numbers later.
		p.cfg.Logger.Warn("actors share worker pod IPs; per-IP connection ceiling is lower than planned",
			slog.Int("actors", len(resumed)), slog.Int("distinct_pod_ips", len(ips)))
	}
	return nil
}

// Settle pings every actor until it answers, or until the deadline.
//
// The shipped glutton ActorTemplate declares no readiness probe, so
// ResumeActor can return before the workload's port 80 is accepting. Paying
// that once here turns a per-request confound into a one-time setup cost.
func (p *Pool) Settle(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	start := time.Now()

	var grp errgroup.Group
	for _, a := range p.Actors() {
		grp.Go(func() error {
			var last string
			for attempt := 0; ; attempt++ {
				outcome, class := p.cfg.Pinger.Ping(ctx, a.Host, fmt.Sprintf("settle-%d", attempt))
				if outcome == OutcomeOK {
					return nil
				}
				last = string(outcome) + "/" + class
				if time.Now().After(deadline) {
					return fmt.Errorf("actor %s (%s) never became ready: last %s", a.Name, a.PodIP, last)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(settleInterval):
				}
			}
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	p.cfg.Logger.Info("actors ready", slog.Duration("elapsed", time.Since(start)))
	return nil
}

// Teardown suspends and deletes every actor this pool created, concurrently.
//
// Concurrently because DeleteActor requires the actor to be SUSPENDED first
// and each suspend takes the actor's distributed lock; serially, a pool of 40
// would outlast the pod's termination grace period and leak actors holding
// worker slots. Errors are logged, not returned: teardown runs on the way out
// and there is nothing left to abort.
func (p *Pool) Teardown(ctx context.Context) {
	p.mu.Lock()
	names := append([]string(nil), p.created...)
	p.created = nil
	p.actors = nil
	p.mu.Unlock()

	if len(names) == 0 {
		return
	}
	start := time.Now()

	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.cfg.Stub.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: p.ref(name)}); err != nil {
				p.cfg.Logger.Warn("SuspendActor failed", slog.String("actor", name), slog.String("err", err.Error()))
			}
			if _, err := p.cfg.Stub.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: p.ref(name)}); err != nil {
				p.cfg.Logger.Error("DeleteActor failed; actor may be leaking a worker slot",
					slog.String("actor", name), slog.String("err", err.Error()))
			}
		}()
	}
	wg.Wait()
	p.cfg.Logger.Info("actors torn down", slog.Int("count", len(names)),
		slog.Duration("elapsed", time.Since(start)))
}

func (p *Pool) ref(name string) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: p.cfg.Atespace, Name: name}
}

func (p *Pool) ensureAtespace(ctx context.Context) error {
	_, err := p.cfg.Stub.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: p.cfg.Atespace}},
	})
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok && s.Code() == codes.AlreadyExists {
		return nil
	}
	return fmt.Errorf("CreateAtespace %s: %w", p.cfg.Atespace, err)
}
