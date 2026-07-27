# Scale test report: glutton on GKE

Three findings from running the glutton benchmark against GKE, in the order we
found them. The first two are fixed. The third is open, and it's the one blocking
the 100ms wake target.

Setup: 50 virtual users, 50 workers, ramp 5/s, `trace-probability` 1.0,
`GluttonUser`, two `c3-standard-4` nodes. Users matched workers, so nobody queued
for capacity.

| Operation | First run | After both fixes |
|---|---|---|
| `GluttonPing` | 22ms, 42% fail | 28ms, 0% fail |
| `ResumeActor` | 3800ms, 0% fail | 3900ms, 0% fail |
| `SuspendActor` | 2400ms, 0% fail | 2200ms, 0% fail |
| `ResumeActorColdStart` | 1400ms, 0% fail | 1600ms, 0% fail |

The data plane is fine now. The control path is about 38x off target and we don't
yet know why.

---

## 1. Envoy connection reuse (fixed)

### Symptoms

42% of pings returned `HTTP 503 ... reset reason: connection termination`.

### Evidence

Envoy counters on the router, cluster `dynamic_forward_proxy_cluster`:

| Counter | Value |
|---|---|
| `upstream_rq_total` / `upstream_cx_total` | 1126 / 599, about 1.9 requests per connection |
| `upstream_cx_destroy_remote_with_active_rq` | 450 |
| `upstream_cx_connect_fail` | 29 |
| `upstream_rq_503` | 479 |

450 + 29 = 479, so those two counters account for every 503. The split also
matches the two error strings the load generator reported.

The cause is connection pooling. Envoy pools HTTP/1.1 connections by destination
address, which assumes an address means one stable server. Here the worker pod is
long-lived and keeps its IP (we saw one up for 123 minutes across hundreds of
actors), but the actor sandbox inside it is destroyed and recreated on every
suspend/resume. Envoy files a connection to `10.0.1.5:80`, the actor behind it
goes away, and the next request reuses the dead connection and gets a 503.

We ruled out three other explanations: stale routing (ext_proc re-reads the worker
IP on every request), DNS cache staleness (the authority is already an IP literal,
so the DFP cache never resolves anything), and xDS churn (there are no per-actor
xDS resources).

### Solution

Set `max_requests_per_connection: 1` on the dynamic forward proxy cluster, in
[`buildDynamicForwardProxyCluster`](../cmd/atenet/internal/router/xds.go). Envoy
opens a fresh connection per request and never holds one whose actor might be
gone.

After deploying: failures dropped from 42% to 4.6%,
`upstream_cx_destroy_remote_with_active_rq` went from 450 to 0, and the ratio went
from 1.9 to 1.0 requests per connection.

The cost is a TCP handshake per request. In-cluster that's sub-millisecond against
22ms pings, so under 5% overhead.

### Notes

Envoy rejects a `dynamic_forward_proxy` cluster carrying `HttpProtocolOptions`
unless `auto_sni` and `auto_san_validation` are both true, or
`allow_insecure_cluster_options` is set. We hit this on the first attempt and it
was hard to spot: CDS gets NACKed, the cluster is silently absent from the config
dump, every request 503s, and the router pod still shows `2/2 Running`. There are
now two regression tests for it in
[xds_test.go](../cmd/atenet/internal/router/xds_test.go).

A route-level retry policy (`retry_on: connect-failure,refused-stream,reset`) is
drafted but commented out. It's worth adding as defence in depth. We left it off
so the effect of this fix could be attributed cleanly.

---

## 2. Pings arriving before the actor accepts connections (fixed)

### Symptoms

The 4.6% of ping failures left over after fix #1, all reporting
`delayed connect error: 111` (`ECONNREFUSED`). This was always happening (29
occurrences in the first run) but was hidden under the connection-reuse noise.

### Evidence

Every remaining failure was `upstream_cx_connect_fail` /
`upstream_rq_pending_failure_eject`. `ECONNREFUSED` means the worker IP is
reachable and nothing is listening on port 80.

Nothing in the resume path waited for the listener.
[`readyz.WaitAll`](../internal/readyz/readyz.go) skips any container that doesn't
declare a probe, and the glutton `ActorTemplate` didn't declare one. So
`ResumeActor` returned as soon as the workflow finished rather than when port 80
was live, and the load generator pinged into the gap.

### Solution

A `/readyz` handler on glutton's port 80 listener
([glutton/main.go](../cmd/benchmarking/glutton/main.go)), plus a `readyz` probe on
the glutton template
([workloads.yaml.tmpl](workloads/manifests/workloads.yaml.tmpl)) pointing at path
`/readyz` port 80.

The handler has to be on that listener rather than the metrics port. The metrics
server starts independently, so a 200 there wouldn't tell you anything about
whether `/ping` is reachable.

`RestoreWorkload` now blocks on the probe before returning, and the failures are
gone.

### Notes

The probe only covers half the path. `readyz.WaitAll` runs inside ateom and polls
the actor's veth address, so it confirms glutton is bound but not that the
nftables DNAT rule on the worker pod is installed. Both would produce
`ECONNREFUSED`. Since the fix worked, the listener was the cause.

Some things we learned along the way that aren't obvious:

* A template where every container declares `readyz` gets a zero golden-actor
  warmup instead of 20s. The snapshot timestamps confirm it: glutton took 5s from
  template creation to golden snapshot, sleep (no probe) took 25s.
* `ActorTemplate.spec` is immutable, so adding the probe needs a delete and
  recreate, not an update.
* Deleting an ActorTemplate doesn't garbage-collect its golden actor. Recreating a
  template leaves an orphan behind. They're harmless, since the controller selects
  by the `status.goldenSnapshot` pointer on the template rather than by name, but
  they make `kubectl ate get actors` confusing to read.
* We left `full_workloads.yaml.tmpl` alone because its glutton runs in gRPC mode,
  where an HTTP probe would never pass. Worth knowing separately: that also means
  `GluttonUser` can't work against that manifest at all.

---

## 3. Suspend/resume latency (open)

### Symptoms

`ResumeActor` at 3.9s and `SuspendActor` at 2.2s median, against a 100ms design
target. No failures. This is the actual bottleneck and we haven't explained it
yet.

### Evidence

Mostly what it isn't, so far.

It's not the data plane. Resume held at 3900ms against 3800ms before the two fixes
above, so the problems are independent.

It's not contention. We ran 10 VUs against the same 50 workers and latency stayed
high, at a fifth of the concurrency.

It's not snapshot transfer. Every suspend writes a `Full` snapshot to GCS and
every resume reads it back, since
[workflow_suspend.go](../cmd/ateapi/internal/controlapi/workflow_suspend.go)
hardcodes `CHECKPOINT_TYPE_EXTERNAL`. That looked like an obvious fixed cost, but
glutton's snapshot is 1 MB total, which is tens of milliseconds in-region.

It's not worker-assignment collisions. The worker cache is eventually consistent,
so two concurrent resumes can pick the same free worker and one loses an
optimistic version check. That's real, but the retry budget caps out near 600ms
and then fails, and resume failures are 0%.

One warning for anyone re-reading this data. `ResumeActorColdStart` (1.6s) is
faster than `ResumeActor` (3.9s), which looks like proof of contention on the
assumption that a cold start does more work. It doesn't. `RunWorkload` and
`RestoreWorkload` in [ateom-gvisor/main.go](../cmd/ateom-gvisor/main.go) are
almost the same function: same network setup, same container sequence, same
`readyz` wait. They differ only in `cmdStart` vs `cmdRestore`, plus a checkpoint
fetch on the warm path. Cold start is the cheaper path, so the gap tells you
nothing about load. An earlier version of this report got that wrong.

### What's left

Fixed per-operation cost in a serial pipeline. Per resume:

| Stage | Where |
|---|---|
| Assign a worker: valkey read, optimistic-locked write | `AssignWorker`, [workflow_resume.go](../cmd/ateapi/internal/controlapi/workflow_resume.go) |
| Fetch the checkpoint from GCS, lay out OCI bundles, ensure the `runsc` binary | atelet |
| Build a veth pair and netns | `setupActorNetwork` |
| Four `runsc` subprocess invocations: create+restore for pause, create+restore for the app container | `RestoreWorkload` |
| Poll until the app answers `/readyz` | `readyz.WaitAll` |

Four process spawns of a large Go binary plus namespace setup is our current
guess. We haven't measured it.

### Next step

Read the traces. Spans were captured at `trace-probability` 1.0 and go to Cloud
Trace through the in-cluster `gke-managed-otel` collector. `runner.py` writes
every sampled `trace_id` and its `duration_ms` to `traces.txt`, so sort by
duration and open the worst `ResumeActor`. Use the 10-VU run, since there's no
queueing noise in it.

The first split to look at is `AssignWorker` vs `CallAteletRestore`, then the
atelet and ateom spans underneath.

### Notes

Unrelated to the latency, but fix it before running a scaling sweep: all 63 pods,
including every worker and the whole control plane, are QoS class `BestEffort`
with no CPU requests or limits. Two consequences. The scheduler places pods by
summing requests, so the nodes report 63% and 12% CPU requested while running 26
and 24 restore-heavy workers. And the kernel weights contended CPU by request, so
under load ateapi and valkey get starved by the data plane they're scheduling. 50
workers over 8 vCPU won't distort the numbers at 50 VUs, but it will at higher
concurrency.
