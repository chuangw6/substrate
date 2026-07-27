# Troubleshooting the benchmark

Symptom → where to look → what to change and how to redeploy.

Companion to [FINDINGS.md](FINDINGS.md), which records what past runs concluded.
This file is about getting from a confusing error to the right layer.

---

## Method

Four habits that resolve most of these faster than guessing.

**Get the denominator.** "A lot of `GluttonPing` failures" and "66 failures out
of 130 requests" lead to different conclusions than "66 out of 100,000". Locust's
UI shows failures prominently and totals quietly; always pull both.

**Ask the layer that owns the failure.** boomer reports what it *observed* — a
503, a timeout. Envoy knows *why* it produced that 503, and its counters are
exact. Same for actor state: boomer sees `FailedPrecondition`, but
`kubectl ate get workers` shows whether a worker was actually free.

**Make the numbers reconcile.** If failures split across causes, the parts should
sum to the whole. `450 + 29 = 479` total 503s meant nothing was unexplained. When
they don't add up, there is a mechanism you haven't found yet.

**Rule things out in writing.** Plausible-but-wrong theories get re-derived by
the next person, including you next week. Record what you eliminated and how.

**Confirm the fix is live, not merely deployed.** A green rollout means the
image is running, not that the config took. Envoy will run indefinitely with a
cluster it rejected, reporting `2/2 Running` the whole time. Read the change
back out of the component itself before spending a benchmark run on it.

---

## Diagnostic commands

`kubectl ate` needs `export PATH="$PATH:$HOME/go/bin"`.

**Locust totals, failure rates, and the error table.** More reliable than reading
the UI, and works over an existing session:

```bash
kubectl exec -n benchmarking deploy/locust -c locust-master -- python -c "
import json,urllib.request
d=json.load(urllib.request.urlopen('http://localhost:8089/stats/requests'))
print('state:',d.get('state'),'users:',d.get('user_count'))
for s in d.get('stats',[]):
    n,f=s['num_requests'],s['num_failures']
    print(f\"{s['name'][:24]:26s} req={n:7d} fail={f:6d} ({100*f/max(n,1):5.1f}%) med={s.get('median_response_time')}\")
for e in d.get('errors',[]): print(e.get('occurrences'), e.get('name'), '|', str(e.get('error'))[:110])
"
```

**Traces.** The only thing that breaks a multi-second latency down into its
parts. Spans go from each component's `OTEL_EXPORTER_OTLP_ENDPOINT` to the
in-cluster `gke-managed-otel` collector, which forwards to
`telemetry.googleapis.com` — so they land in **Cloud Trace**:

```
https://console.cloud.google.com/traces/explorer?project=<PROJECT>
```

Browsing by span name (`ResumeActor`, `AssignWorker`, `CallAteletRestore`) works,
but jumping straight to a known-bad trace is faster. `runner.py` writes every
sampled span to `<name>_traces.txt` — TSV, columns
`time, name, duration_ms, latency_source, trace_id, err` — in
`/tmp/<timestamp>-locust-runner/`, and uploads it as `traces.txt` next to the
run's stats. Sort by `duration_ms`, take the worst, and open it directly:

```
https://console.cloud.google.com/traces/list?tid=<TRACE_ID>&project=<PROJECT>
```

Comparing a slow steady-state trace against an early ramp trace is usually more
informative than either one alone: same operation, different contention.

If Cloud Trace is empty, check that the collector has a `traces` pipeline (it
handles logs and metrics separately, and a config can carry some but not others)
and that `cloudtrace.googleapis.com` is enabled on the project.

**Envoy counters.** The authority on anything routing-related. There is no `curl`
in the sidecar, so port-forward:

```bash
kubectl port-forward -n ate-system deploy/atenet-router 19901:9901 &
curl -s localhost:19901/stats | grep dynamic_forward_proxy_cluster
curl -s localhost:19901/config_dump          # what xDS actually pushed
```

Two things about reading `config_dump`. It preserves **snake_case** proto field
names — `max_requests_per_connection`, not the lower-camel-case that protobuf's
JSON mapping produces by default. And grepping it is a weak check either way: an
empty result means the field is absent, the cluster is missing, *or* you spelled
the name wrong, and those call for very different responses. Select the cluster
instead, so a missing one is loud rather than silent:

```bash
curl -s localhost:19901/config_dump \
| jq '.configs[] | select(."@type"|test("ClustersConfigDump"))
      | .dynamic_active_clusters[] | select(.cluster.name=="dynamic_forward_proxy_cluster")
      | .cluster.typed_extension_protocol_options'
```

**Actor and worker state.** `-A` matters — golden actors live in `ate-golden`
and a single-atespace listing hides them:

```bash
kubectl ate get actors -A
kubectl ate get workers                       # FREE vs ASSIGNED, and to whom
kubectl get actortemplate -A -o custom-columns='NS:.metadata.namespace,NAME:.metadata.name,PHASE:.status.phase'
```

**Component logs.**

```bash
kubectl logs -n benchmarking deploy/locust -c boomer-glutton --tail=500
kubectl logs -n ate-system -l app=atelet --tail=200       # image pulls, sandbox start
kubectl logs -n ate-system -l app=ateapi --tail=200       # control-plane workflows
kubectl logs -n ate-system -l app=atenet-router --tail=200
```

boomer only logs *sampled* spans, so raise `trace-probability` before relying on
its log volume for ratios.

**Capacity.** Nodes, not just pods — ateom pods declare no resource requests, so
the scheduler will not warn you:

```bash
kubectl top nodes
kubectl get nodes -o custom-columns='NAME:.metadata.name,CPU:.status.allocatable.cpu,PODS:.status.allocatable.pods'
kubectl get pods -A -o wide --no-headers | awk '{print $8}' | sort | uniq -c
# Everything BestEffort means requested CPU tells you nothing about real load.
kubectl get pods -A -o custom-columns='QOS:.status.qosClass' --no-headers | sort | uniq -c
```

`kubectl top nodes` is only meaningful **during** a run. Run it in a second
terminal while the test is in flight; afterwards the nodes look idle no matter
how starved they were.

---

## Symptoms

### `FailedPrecondition: no free workers available`

**Look at** `kubectl ate get workers`. Two very different causes.

*Every worker `ASSIGNED`, user count > worker count* — plain capacity. One
running actor holds one worker exclusively. Redeploy with
`--worker-count ≥ peak users`.

*Workers free, or one worker pinned to an actor in `ate-golden`* — a golden actor
is stuck and holding a slot. Check
`kubectl get actortemplate -A` for a template below `Ready`, then
`kubectl logs -n ate-system -l app=atelet` — the usual cause is that atelet
cannot pull the actor image. Note that atelet pulls actor images **in-process
under its own workload-identity principal**, which is a different grant from the
kubelet's node service account; the cluster looks healthy right up until an actor
starts. `go run ./tools/setup-gcp create iam` grants it.

**Fix:** capacity → redeploy workloads. Image pull → IAM. Neither needs a build.

---

### Resume/suspend latency is seconds, and nothing fails

`ResumeActor` / `SuspendActor` medians sit in the seconds with a 0% failure rate.
Before theorising, run the one experiment that splits the two possible worlds:
**hold the worker count fixed and cut the user count** (50 VUs → 10). If latency
drops, it is contention. If it stays put, it is fixed per-operation cost, and no
amount of extra hardware will help.

Resist a tempting but invalid shortcut here. `ResumeActorColdStart` is *faster*
than `ResumeActor`, which looks like proof of contention — surely a cold start
does more work, so if it wins, load must be the difference. It is not more work.
Compare `RunWorkload` and `RestoreWorkload` in
[ateom-gvisor/main.go](../cmd/ateom-gvisor/main.go): same network setup, same
container sequence, same `readyz` wait, differing only in `cmdStart` vs
`cmdRestore` plus a checkpoint fetch on the warm path. Cold start is the cheaper
path. The gap says nothing about load.

**If it is fixed cost**, the pipeline to account for is: worker assignment
(valkey, optimistic-locked) → atelet fetching the checkpoint and laying out OCI
bundles → `setupActorNetwork` → four `runsc` subprocess invocations → the
`readyz` poll. Get the split from traces rather than guessing; see **Traces**
above. Check the snapshot size before blaming the download — glutton's is 1 MB,
which is nothing in-region.

**If it is contention**, look at whether any CPU is provisioned at all:

```bash
kubectl get pods -A -o custom-columns='QOS:.status.qosClass' --no-headers | sort | uniq -c
kubectl top nodes   # during the run, not after
```

Worker pods, atelet, ateapi and valkey all declare no `resources.requests` and no
`resources.limits`, which puts every one of them in the **BestEffort** QoS class.
That fails in two places at once:

- *Scheduling.* The scheduler packs pods by summing their **requests**. Pods that
  request nothing fit anywhere, so `kubectl describe node` can report 12% CPU
  requested on a node running 26 restore-heavy workers. You get no warning and no
  `Pending` pod — the signal a normal capacity problem would give you.
- *Runtime.* The kernel divides contended CPU by cgroup weight, derived from the
  request. No request means the floor. Under saturation BestEffort pods get
  whatever is left, and because the *control plane is BestEffort too*, ateapi and
  valkey are starved by the data plane they are trying to schedule.

Sanity-check the arithmetic: 50 workers on two `c3-standard-4` is 50 concurrent
gVisor restores over **8 vCPU total**. `kubectl top nodes` pinned near `4000m`
per node confirms saturation.

**Fix:** fixed cost → profile the pipeline, there is nothing to configure.
Contention → more nodes, a bigger machine type, or fewer concurrent users.

Either way, **do not** record a latency number from a saturated pool as a
substrate measurement.

---

### `Actor ... is not suspended (STATUS_RESUMING)` / `Aborted: another operation is in progress`

**Look at** nothing — this is expected. An actor mid-workflow holds a lock, so
neither delete nor suspend will work, and neither changes anything.

**Fix:** repair the underlying cause and let the controller's retry finish. Do
not fight the lock.

---

### 42% of `GluttonPing` fail with `HTTP 503 ... connection termination`

**Look at** the Envoy counters. `upstream_cx_destroy_remote_with_active_rq` and
`upstream_cx_connect_fail` should sum to `upstream_rq_503`; if they do, the far
end is closing connections rather than anything being overloaded. Compare
`upstream_rq_total` to `upstream_cx_total` — a low ratio means heavy connection
reuse against short-lived actors.

**Fix:** [xds.go](../cmd/atenet/internal/router/xds.go),
`buildDynamicForwardProxyCluster`. Full analysis in
[FINDINGS.md](FINDINGS.md#finding-2--envoy-connection-reuse-causes-40-of-pings-to-503).

**Rebuild:** `./hack/install-ate.sh --deploy-atenet`

---

### Enormous request counts, nearly all failing, from few actors

**Look at** the ratio of `CreateActor` count to total requests. ~50 actors
producing 120k requests in two minutes is retries, not work.

Any early return in boomer's `iterate()` skips the pacing sleep unless it runs in
a `defer`, so a failing VU re-enters immediately and spins at gRPC round-trip
speed. This is fixed
([lifecycle.go](../internal/benchmarking/boomer/glutton/lifecycle.go)), but the
same shape recurs whenever a new early-exit path is added.

**Fix:** keep the wait in the `defer`. Note it only *paces* — set both wait
bounds to `0` and the storm returns by design.

**Rebuild:** `./benchmarking/locust/build_and_push.sh` then
`kubectl rollout restart deploy/locust -n benchmarking`

---

### A user class fails immediately with `NOT_FOUND`

**Look at** `kubectl get actortemplate -A`. Only `sleep` and `glutton` ship in
the default deploy. `usermem` and `kernelmem` live in a template no script
applies; `counter` is a separate demo.

**Fix:** deploy the template the class needs — see §9 of
[ONBOARDING.md](ONBOARDING.md).

---

### Actors left over after a run

**Look at** `kubectl ate get actors -A`. Expect only the `ate-golden` entries.

boomer deletes its actors when `boomer.Run` returns, which needs SIGTERM — the
web UI's Stop button is not that, and its in-memory actor map survives into the
next run.

**Fix:** `kubectl rollout restart deploy/locust -n benchmarking`. Full procedure
including manual deletion in §11 of [ONBOARDING.md](ONBOARDING.md).

---

### Pod stuck `ImagePullBackOff` with "no match for platform in manifest"

**Look at** the image architecture. Only the locust image is built with `docker`,
so an Apple Silicon workstation pushes arm64 to amd64 nodes. The push succeeds;
the failure surfaces two steps later.

**Fix:** [build_and_push.sh](locust/build_and_push.sh) pins `linux/amd64`;
override with `LOCUST_IMAGE_PLATFORM` for arm64 node pools.

---

### `ImagePullBackOff` with a 403, on an image you just pushed successfully

**Look at** the two principals separately. Your workstation credentials do the
push; a *different* identity does the pull, and only the pull is failing.

```bash
kubectl describe pod -n ate-system <pod> | grep -A3 Failed
gcloud container clusters list --format='value(name,nodeConfig.serviceAccount)'
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten='bindings[].members' \
  --filter="bindings.members:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --format='value(bindings.role)'
gcloud artifacts repositories get-iam-policy gcr.io --location=us
```

Empty output from either of the last two means nothing grants read access.

The trap is that this can appear on a cluster that has been pulling happily for
weeks. `gcr.io` pushes now redirect into Artifact Registry, and the AR repo is
created on first push with an **empty IAM policy**. Images pulled before the
redirect came from the legacy path; the first pull afterwards is the first one
that ever needed an AR grant. Check the repo's `createTime` against the age of
the last pod that started successfully — if they are minutes apart, this is it.

Two principals need it, and they fail at different times:

- the **node service account**, for substrate's own images — fails immediately,
  at rollout
- **atelet's workload-identity principal**, for *actor* images, which atelet
  pulls in-process — fails later, surfacing as golden actors stuck below `Ready`
  and then `no free workers available`

**Fix:** `go run ./tools/setup-gcp create iam` grants
`roles/artifactregistry.reader` to both, project-wide. Repo-scoped is narrower:

```bash
gcloud artifacts repositories add-iam-policy-binding gcr.io \
  --location=us --member=serviceAccount:<PROJECT_NUMBER>-compute@developer.gserviceaccount.com \
  --role=roles/artifactregistry.reader
```

---

### Every actor request 503s, and `dynamic_forward_proxy_cluster` is absent from the config dump

**Look at** whether Envoy is rejecting the config rather than misrouting it. The
router looks completely healthy — pod `2/2 Running`, new image, no restarts —
because a rejected cluster is silently dropped, not surfaced.

```bash
curl -s localhost:19901/stats | grep -E "cds.update_(rejected|success)"
kubectl logs -n ate-system deploy/atenet-router -c envoy | grep -i reject | tail -3
```

`update_rejected` climbing while `update_success` stays at 0 is conclusive. The
Envoy log line names the offending cluster and the exact reason.

Note that CDS applies clusters one at a time and NACKs the whole push on the
first failure, so the clusters added *before* the bad one survive. A config dump
showing some of your clusters is not evidence the push succeeded — always
compare against the list in
[`UpdateSnapshot`](../cmd/atenet/internal/router/xds.go).

Real instance: adding `HttpProtocolOptions` to the dynamic forward proxy cluster
tripped a validator requiring `auto_sni` and `auto_san_validation`, or
`allow_insecure_cluster_options`. Unit tests passed and `snap.Consistent()` was
happy — the proto is well-formed, and only Envoy enforces the rule.
`TestDynamicForwardProxyCluster_EnvoyAcceptsHttpProtocolOptions` now guards it.

**Rebuild:** `./hack/install-ate.sh --deploy-atenet`

---

### Envoy latency tiles dark on the Cloud Monitoring dashboard

**Look at** whether `envoy_http_downstream_rq_time` is being scraped at all. The
PodMonitoring manifest exists but no script or kustomization applies it, so it is
almost certainly absent.

**Fix:** `kubectl apply -f manifests/ate-install/atenet-router-monitoring.yaml`

---

## Rebuild matrix

Substrate images build with `ko`, which needs **no container runtime**. The
locust image is the sole exception — it shells out to `docker build`.

| Changed | Rebuild and redeploy |
|---|---|
| boomer Go code, locust Python, test classes | `./benchmarking/locust/build_and_push.sh` then `kubectl rollout restart deploy/locust -n benchmarking` |
| atenet router, Envoy xDS config | `./hack/install-ate.sh --deploy-atenet` |
| ateapi / control plane | `./hack/install-ate.sh --deploy-ate-apiserver` |
| atelet | `./hack/install-ate.sh --deploy-atelet` |
| ateom, glutton, ActorTemplates, worker count | `./benchmarking/workloads/deploy.sh --deploy --worker-count N` |
| Everything | `./hack/install-ate.sh --deploy-ate-system` |

Config-only changes to the locust stack — args, env, replicas — need no image
build; `kubectl apply` the manifest and restart.

After redeploying the router or the control plane, clean up leftover actors
before the next run, or you will be reading two runs' data at once.
