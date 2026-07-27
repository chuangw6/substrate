# Benchmark findings

Results from running the glutton scale test against GKE. Each entry records
what was measured, the evidence, and what it means. See
[ONBOARDING.md](ONBOARDING.md) for what the test is and how to run it.

---

## 2026-07-27 — 50 users on GKE

**Setup.** 50 VUs, 50 workers, ramp 5/s, `trace-probability` 1.0, `GluttonUser`,
two `c3-standard-4` nodes. Workers matched users, so no VU ever waited for
capacity — the run has none of the `no free workers available` noise that
undersized runs produce (§10 of ONBOARDING).

**Measured.**

```
ResumeActor           median 3800ms   0% failures
SuspendActor          median 2400ms   0% failures
ResumeActorColdStart  median 1400ms   0% failures
GluttonPing           median   22ms  42% failures
```

Two independent findings, one of them a genuine bottleneck and one a bug.

---

### Finding 1 — resume latency is the bottleneck, not the network

Waking an actor takes **3.8 seconds** at 50 concurrent, against a ~100ms design
target — roughly 38× off. Suspend is 2.4s. Meanwhile the actual network round
trip, `GluttonPing`, is **22ms** and healthy.

> **Corrected by [Finding 4](#finding-4--resume-cost-is-fixed-not-contention).**
> The paragraph below concluded "contention" from cold starts being faster than
> warm resumes. That inference is wrong — cold start is the *cheaper* code path,
> not a more expensive one, so the gap says nothing about load. A 10-VU run
> against the same 50 workers stayed slow, ruling contention out. The 3.8s figure
> stands; the explanation does not.

~~This is contention, not path cost. The cold starts are *faster* than the warm
resumes (1400ms vs 3800ms), which is backwards — a cold start does strictly more
work. The difference is when they ran: cold starts happened during the 5/s ramp
with only a few in flight, warm resumes at full 50-VU concurrency. Same work,
less contention, three times quicker.~~

What holds regardless: the suspend/resume control path is where substrate stops
meeting its target, and the data plane is not the constraint at this size.

**Not yet known:** which part of resume consumes the 3.8s. The run had
`trace-probability` at 1.0, so the spans exist in Cloud Trace and the breakdown
is recoverable.

---

### Finding 2 — Envoy connection reuse causes ~40% of pings to 503

Not a capacity limit. Requests are being sent down TCP connections that are
already dead.

**Evidence.** From the router's Envoy admin interface. There is no `curl` in the
sidecar, so port-forward and query from your workstation:

```bash
kubectl port-forward -n ate-system deploy/atenet-router 19901:9901 &
curl -s localhost:19901/stats | grep dynamic_forward_proxy_cluster
```

In Envoy's naming, **upstream** is whatever Envoy forwards *to* (here, the
actor), **rq** is request, and **cx** is connection.

| Counter | Value | Meaning |
|---|---|---|
| `cluster.dynamic_forward_proxy_cluster.upstream_rq_total` | 1126 | Requests Envoy tried to forward to actors |
| `cluster.dynamic_forward_proxy_cluster.upstream_cx_total` | 599 | Connections Envoy opened to carry them |
| `cluster.dynamic_forward_proxy_cluster.upstream_rq_503` | 479 | Requests that failed with a 503 |
| `cluster.dynamic_forward_proxy_cluster.external.upstream_rq_503` | 479 | Same failures, counted against external origins — identical here, so none were internally originated |
| `cluster.dynamic_forward_proxy_cluster.upstream_cx_destroy_remote` | 480 | Connections closed by the **far end** rather than by Envoy |
| `cluster.dynamic_forward_proxy_cluster.upstream_cx_destroy_remote_with_active_rq` | 450 | Of those, the ones closed **while a request was still in flight** |
| `cluster.dynamic_forward_proxy_cluster.upstream_cx_connect_fail` | 29 | Connections Envoy could not open at all |
| `cluster.dynamic_forward_proxy_cluster.upstream_rq_pending_failure_eject` | 29 | Requests dropped because their connection never opened — the same 29 |

Two things fall out. `450 + 29 = 479` accounts for **every** 503, and the split
matches the two error strings locust reports:

| Counter | Locust message |
|---|---|
| `cluster.dynamic_forward_proxy_cluster.upstream_cx_destroy_remote_with_active_rq` (450) | `reset reason: connection termination` |
| `cluster.dynamic_forward_proxy_cluster.upstream_cx_connect_fail` (29) | `delayed connect error: 111` (`ECONNREFUSED`) |

And 1126 requests over 599 connections means each connection carried about
**two** requests before dying, with 480 of the 599 killed by the far end rather
than closed normally by Envoy.

**Mechanism.** Envoy pools HTTP/1.1 connections by destination address so it can
skip the TCP handshake on the next request. That is safe when an address means
one stable server. Here it doesn't:

- The **worker pod is long-lived and keeps its IP** — one was observed running
  123 minutes across hundreds of actors.
- The **actor sandbox inside it is destroyed and recreated** every
  suspend/resume, and a different actor takes the slot.

So `10.0.1.5:80` looks stable to Envoy while the thing behind port 80 keeps
being swapped out:

1. Actor A is awake on `10.0.1.5`. Envoy pings it and files the still-open
   connection in its pool.
2. A suspends; its sandbox is destroyed. The connection is dead, but Envoy still
   has it filed.
3. Another ping needs `10.0.1.5:80`. Envoy finds the pooled connection and
   reuses it.
4. The write goes into a channel with nothing on the other end. Envoy gives up
   and synthesizes a 503.

Steps 2–4 are a race between the far end's hang-up notice arriving and Envoy
picking that connection to reuse. At 50 actors churning continuously, reuse wins
about 40% of the time. The 29 connect failures are the same race a beat earlier:
connecting to a worker whose sandbox is not bound yet.

**Ruled out.** Worth recording, because all three are plausible and all three are
wrong:

- *Stale routing.* ext_proc reads `actor.GetAteomPodIp()` fresh on every request
  and rewrites `:authority` to `workerIP:80`
  ([extproc.go](../cmd/atenet/internal/router/extproc.go)). The address Envoy
  gets is always current.
- *DNS cache staleness.* The authority is already an IP literal, so the DFP DNS
  cache never resolves an actor hostname. Its TTL is irrelevant here.
- *xDS churn.* The snapshot holds two or three static clusters and no per-actor
  resources ([xds.go](../cmd/atenet/internal/router/xds.go)), so actor lifecycle
  events do not regenerate it.

**Fix.** Two options. They address the problem at different points — one
prevents the race, the other recovers from it — so they are not mutually
exclusive.

#### Option A — disable connection reuse (applied)

Set `max_requests_per_connection: 1` on the dynamic forward proxy cluster, so
Envoy opens a fresh connection per request and never holds one whose actor may
have gone away.

| | |
|---|---|
| **Pros** | Eliminates the race rather than recovering from it, so failures go to zero instead of being retried. No wasted round trips. Config-only, one field. Behaviour is deterministic and easy to reason about — no "usually fine, sometimes 503". |
| **Cons** | Pays a TCP handshake on every request. Roughly one extra round trip: sub-millisecond in-cluster, against successful pings of ~22ms, so well under 5% overhead — but it is not free, and it grows as a share of total latency if actor-side handling ever gets much faster. More sockets opened and closed, which means more ephemeral port churn on the router at very high request rates. |

Applied in [xds.go](../cmd/atenet/internal/router/xds.go),
`buildDynamicForwardProxyCluster`.

#### Option B — retry on connection failure

Add a route-level `RetryPolicy` with
`retry_on: connect-failure,refused-stream,reset` and `num_retries: 2`. The dead
connection is discarded once it fails, so the retry opens a fresh one and
succeeds.

| | |
|---|---|
| **Pros** | Keeps connection reuse and its handshake savings for the requests that do not hit the race. Also covers unrelated transient upstream failures — a worker restarting, a genuinely refused connect — which Option A does nothing about. |
| **Cons** | Recovers from the bug instead of preventing it, so it stays in the system as latency rather than disappearing. At the measured 40% hit rate that is a wasted round trip on nearly half of all requests, to save a handshake worth far less — a bad trade at this failure rate. Retried requests inflate p99 and make the data plane look slower than it is. Retrying a non-idempotent request is only safe because `/ping` is; it would need care if the actor API grew side effects. |

**Recommendation.** Option A as the fix, since it makes the failure mode
impossible. Option B is worth adding underneath as defense in depth for the
transient failures Option A cannot prevent — but on its own it would leave a
40% retry rate in place and call it healthy.

---

## 2026-07-27 — 50 users again, with the connection-reuse fix deployed

Same shape as the run above: 50 VUs, 50 workers, ramp 5/s, `trace-probability`
1.0. Sampled mid-run, so locust and Envoy totals were taken about a minute apart
and do not reconcile exactly.

```
ResumeActor           median 3900ms   0% failures
SuspendActor          median 2200ms   0% failures
ResumeActorColdStart  median 1600ms   0% failures
GluttonPing           median   28ms   4.6% failures   (was 42%)
```

**Finding 2 is confirmed fixed.** The counters show the mechanism gone, not just
the symptom:

| Counter | Before | After |
|---|---|---|
| `upstream_rq_total` / `upstream_cx_total` | 1126 / 599 ≈ 1.9 req per connection | 618 / 636 ≈ 1.0 |
| `upstream_cx_destroy_remote_with_active_rq` | 450 | **0** |
| `upstream_rq_503` | 479 | 18 |

One request per connection, and zero connections killed by the far end
mid-request. The locust error string changed too: `connection termination` is
gone entirely.

**Finding 1 is unchanged**, which is the useful part. Resume held at 3900ms
against 3800ms before. The two findings were independent, as expected — fixing
the data plane moved nothing in the control plane, and the resume bottleneck is
now measurable without 42% of pings polluting the data.

---

### Finding 3 — pings can arrive before the actor is accepting connections

Every remaining failure is `upstream_cx_connect_fail` /
`upstream_rq_pending_failure_eject`, surfacing as `delayed connect error: 111`
(`ECONNREFUSED`). This mode was always present — 29 occurrences in the previous
run — but was buried under the connection-reuse noise.

`ECONNREFUSED` means the worker IP is reachable and **nothing is listening on
port 80**. Inbound port 80 reaches an actor through an nftables DNAT rule that
ateom installs on the worker pod when it activates the actor
([main.go](../cmd/ateom-gvisor/main.go)), which then forwards into the sandbox
where glutton listens.

Nothing in that path gates on the listener actually accepting. The glutton
`ActorTemplate` declares no readiness probe, so `ResumeActor` returns success
once the workflow completes rather than once port 80 is live. boomer pings
immediately afterwards and loses the race about 4.6% of the time.

**Not yet distinguished:** whether the gap is the DNAT rule not yet installed or
glutton not yet bound inside the sandbox. Both produce `ECONNREFUSED` and the
remedies differ.

**Fix.** A readiness probe on the template is the real answer — it also drops
the golden actor's 20s warmup to zero. Option B from Finding 2, retry on
`connect-failure`, is now clearly worth adding underneath: the economics
inverted with the reuse bug gone. At a 40% failure rate a retry wasted a round
trip on nearly half of all requests; at 4.6% it costs one retry in 22 requests
to eliminate a transient that is genuinely retry-shaped, and `/ping` is
idempotent. The draft sits commented out in
[xds.go](../cmd/atenet/internal/router/xds.go).

**Status: fixed.** The `readyz` probe was added to the glutton template and
deployed on 2026-07-27, and the failures are gone. Note the probe closes only one
of the two possible gaps — `readyz.WaitAll` is called by ateom over the actor's
veth address, so it proves glutton is bound but says nothing about whether the
nftables DNAT rule on the worker pod is installed yet. That it worked means the
listener was the cause. Had `upstream_cx_connect_fail` survived, the remaining
suspect would have been `installActorNftablesRules` in
[ateom-gvisor/main.go](../cmd/ateom-gvisor/main.go).

A side effect worth knowing: a template where every container declares `readyz`
gets a zero golden-actor warmup instead of 20s. The golden actor snapshot
timestamps confirm it — glutton took 5s from template creation to snapshot,
sleep (no probe) took 25s.

---

### Finding 4 — resume cost is fixed, not contention

**Retracted hypothesis.** An earlier draft of this finding argued the latency was
CPU starvation: 50 workers over 8 vCPU, every pod `BestEffort`. A 10-VU run
against the same 50 workers refuted it — latency stayed high with a fifth of the
concurrency. The resource problem below is real and worth fixing, but it is not
what makes resume slow.

**The reasoning that was wrong**, recorded because it is an easy trap. Finding 1
observed that cold starts (1400–1600ms) are *faster* than warm resumes
(3800–3900ms) and called that backwards, on the assumption that a cold start does
strictly more work. It does not. Compare
[`RunWorkload`](../cmd/ateom-gvisor/main.go) and `RestoreWorkload` in the same
file: identical `setupActorNetwork`, identical pause-plus-app container sequence,
identical `readyz.WaitAll`. The only differences are `cmdStart` vs `cmdRestore`,
and that the warm path additionally needs atelet to have fetched the checkpoint.
Cold start is the *cheaper* path. The cold/warm gap was never evidence of
contention, and any inference resting on it is void.

**What resume actually costs.** A fixed, serial pipeline, per resume:

| Stage | Where |
|---|---|
| Assign a worker — valkey read, optimistic-locked write | `AssignWorker` in [workflow_resume.go](../cmd/ateapi/internal/controlapi/workflow_resume.go) |
| Fetch checkpoint from GCS, lay out OCI bundles, ensure the `runsc` binary | atelet (see the "Contract with atelet" comment in ateom) |
| Build a veth pair and netns | `setupActorNetwork` |
| Four `runsc` subprocess invocations — create+restore for pause, create+restore for the app container | `RestoreWorkload` |
| Poll until the app answers `/readyz` | [readyz.WaitAll](../internal/readyz/readyz.go) |

**Not the snapshot.** The obvious suspect — pulling a `Full` snapshot from GCS on
every resume, since [workflow_suspend.go](../cmd/ateapi/internal/controlapi/workflow_suspend.go)
hardcodes `CHECKPOINT_TYPE_EXTERNAL` — does not survive measurement. The glutton
snapshot is **1 MB** total:

```
$ gcloud storage ls -l "gs://<bucket>/benchmark-workloads/glutton/<id>/**"
     71362  checkpoint.img.zstd
       245  manifest.json
    985176  pages.img.zstd
      1751  pages_meta.img.zstd
TOTAL: 4 objects, 1058534 bytes (1.01MiB)
```

In-region that is tens of milliseconds. Bandwidth is not the constraint.

That leaves per-operation overhead — four `runsc` process spawns, namespace
setup, and the round trips between ateapi, atelet and ateom — as the remaining
candidate. **Unmeasured.** The spans at `trace-probability` 1.0 split it:
`AssignWorker` versus `CallAteletRestore` inside `ResumeActor`, then the atelet
and ateom spans underneath.

**Separately: nothing in the cluster declares resources.** All 63 pods — every
worker *and* the entire control plane — are QoS class `BestEffort`.

```
$ kubectl get pods -A -o custom-columns='QOS:.status.qosClass' --no-headers | sort | uniq -c
  63 BestEffort
```

The scheduler places pods by summing **requests**, so pods requesting nothing fit
anywhere — the nodes report 63% and 12% CPU requested while hosting 26 and 24
workers. And the kernel weights contended CPU by request, so under saturation
BestEffort gets the floor, control plane included. This will distort results at
higher concurrency even though it is not the cause at 50 VUs. Fix it before
running a scaling sweep, or the sweep measures the node pool.

---

### What the first run does *not* tell you

The 42% ping failure rate makes `GluttonPing` latency unreliable — 22ms is the
median of the successes only, and the failures return fast, so any aggregate
including them is skewed optimistic. Fix Finding 2 before quoting any data-plane
number from a run of this shape.
