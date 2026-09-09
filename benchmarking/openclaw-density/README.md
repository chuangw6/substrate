# OpenClaw actor density

How many OpenClaw actors can a fixed set of workers host with
suspend/resume before resumes and suspends stop being quick?

The traffic follows the OpenClaw capacity model. Each actor runs a
30-minute cycle: resume, stay active 3 minutes, suspend, then sit idle
until its slot in the next cycle. A quarter of the actors (the herd)
resume in the same second at the top of the cycle; the rest each own a
fixed slot spread evenly over the cycle. Slot assignment is by rank among
the alive actors (`cohort_and_offset` in `locust/common/openclaw_math.py`).

## Components

| Piece | Path |
|---|---|
| Locust test: actor lifecycle, node watcher, summary output | `locust/tests/openclaw_cycle.py` |
| Load shape: band ladder and bisection | `locust/shapes/openclaw_density.py`, `locust/common/openclaw_ladder.py` |
| Slot math and pass/fail rules (pure Python, unit tested) | `locust/common/openclaw_math.py`, `openclaw-density/tests/` |
| WorkerPool + ActorTemplate manifest | `workloads/manifests/openclaw-density.yaml.tmpl` |
| Deploy script | `openclaw-density/deploy.sh` |

## Setup

1. **A dedicated node pool** for the workers, tainted so nothing else
   lands there. The control plane and the load generator run elsewhere.
   Use a large SSD boot disk: snapshot staging for hundreds of actors
   overruns GKE's default 100 GB disk and evicts the atelet.

   ```sh
   gcloud container node-pools create openclaw-density \
     --cluster "$CLUSTER_NAME" --location "$CLUSTER_LOCATION" \
     --machine-type n2-standard-48 --num-nodes 2 \
     --disk-type pd-ssd --disk-size 1000 \
     --node-labels bench.substrate.dev/pool=openclaw-density \
     --node-taints bench.substrate.dev/pool=openclaw-density:NoSchedule
   ```

2. **Postgres connections.** Substrate's Postgres runs with PostgreSQL's
   default of 100 connections. The two ateapi replicas alone can open 96
   (their pool defaults to one per core), and a herd fills them; the
   database then refuses connections and resumes fail. Raise
   `max_connections` (500 was enough for 704 actors) before running. The
   setting needs a Postgres restart; the StatefulSet recreates the pod
   and the data volume persists:

   ```sh
   kubectl exec -n ate-system postgres-0 -c postgres -- \
     psql -U postgres -c "ALTER SYSTEM SET max_connections = 500;"
   kubectl delete pod -n ate-system postgres-0
   kubectl exec -n ate-system postgres-0 -c postgres -- \
     psql -U postgres -tA -c "show max_connections;"   # expect 500
   ```

3. **A digest-pinned OpenClaw image.** The ActorTemplate rejects tag-only
   references. `deploy.sh` resolves a tag when docker is available, or
   pass `--openclaw-image ghcr.io/openclaw/openclaw@sha256:...`.

4. **Deploy and find the worker count.** Each worker requests 2 GiB and
   0.25 vCPU (the model's reservations); an n2-standard-48 fits 88.

   ```sh
   openclaw-density/deploy.sh --deploy --worker-count 200 --discover-workers --wait-timeout 900s
   # prints WORKERS=<X>; then redeploy at exactly X
   openclaw-density/deploy.sh --deploy --worker-count <X> --wait-timeout 900s
   ```

## Running

From the locust runner image, the same way the automation runs it:

```sh
python3 runner.py \
  -f /app/tests/openclaw_cycle.py,/app/shapes/openclaw_density.py \
  -t 450m -u 704 --name openclaw-density --tag "$(git rev-parse --short HEAD)" \
  --dest "gs://$BUCKET_NAME/openclaw-density-results" \
  --workers <X> --points 2x,3x,4x --stagger-window full --bisect true
```

Locust runs one user per actor in a single process; each user sleeps
until its slot, calls resume, waits 3 minutes, calls suspend. Latency is
wall time from the first call to its return, which for a resume means the
actor is running and its health check answers.

**The ladder.** Sizes run smallest first. Before each size, the new
actors are booted, suspended, and given one unmeasured practice cycle
(resume, idle 3 minutes, suspend); the size starts only when every actor
is suspended, every worker is free, and the machines have been quiet for
2 minutes. One measured 30-minute cycle per size is enough because
nothing else happens inside it. After the ladder, `--bisect true` probes
the midpoint between the last pass and the first fail until they are
within `--bisect-resolution` actors.

**Pass or fail.** A size passes only if all three hold:

| Rule | Bar | Why |
|---|---|---|
| A. On time | no missed slots, no failed ops | the model's own bar |
| B. Resumes stay quick | staggered resume p90 ≤ 10 s, counting only slots ≥ 15 min after the chime (outside the herd's work) | a size can be on time while every resume is ten times slower than idle |
| C. Suspends stay quick | suspend p90 ≤ 10 s for the same actors as rule B (their suspends come 3 min after their resumes); herd suspends are excluded because they land in one wave | suspend contention shows first |
| D. Herd drains fast (optional, off by default) | herd resume p90 ≤ `--herd-resume-p90-max-seconds` | the herd's cost is a known property of a synchronized wake-up; enable this when the headline number should be bounded by it |

The summary names the rule that failed, so the knee comes with a reason.

**Time.** At 176 workers: first warm-up ~35 min, each pause ~25 min,
each size 31.5 min. The 2x/3x/4x ladder is about 2 h 45 m; each probe
adds about 55 min.

### Knobs (locust flags; defaults in `tests/openclaw_cycle.py`)

| Flag | Default | Meaning |
|---|---|---|
| `--points` | `2x,3x,4x` | Sizes, smallest first; `Nx` multiplies `--workers`, integers are absolute |
| `--workers` | 0 | Worker count; required for multiplier points |
| `--herd-fraction` | 0.25 | Share of actors resuming at the chime |
| `--cycle-seconds` | 1800 | Cycle length |
| `--active-dwell-seconds` | 180 | Active period |
| `--stagger-window` | `remaining` | Where staggered slots land: `remaining` (after the herd's active window, the model's prose) or `full` (whole cycle, the model's formula) |
| `--cycles-per-point` | 1 | Measured cycles per size |
| `--first-boundary-lead-seconds` | 90 | Gap from a size's start to its first chime |
| `--warmup-concurrency` | 64 | Simultaneous warm-ups during a pause |
| `--practice-dwell-seconds` | 180 | Idle time in the practice cycle; 0 disables it |
| `--warmup-settle-seconds` | 120 | Quiet time required after the last warm-up op before a size starts |
| `--barrier-max-seconds` | 3600 | Longest a pause may wait before starting the size anyway (marked `suspect`). Must exceed the first warm-up |
| `--calm-start-seconds` | 900 | Rules B and C count staggered actors whose slot is at least this far after the chime |
| `--staggered-p90-max-seconds` | 10 | Rule B bar; negative disables |
| `--staggered-suspend-p90-max-seconds` | 10 | Rule C bar; negative disables |
| `--herd-resume-p90-max-seconds` | off | Rule D bar; negative (default) disables |
| `--bisect` | false | Probe midpoints after the ladder |
| `--bisect-resolution` | workers/4 | Stop when pass and fail are this close |
| `--delete-actors-on-stop` | true | Delete every actor at the end |

## Reading the results

Uploaded under `<dest>/runs/<name>/run_date=…/run_ts=…/run_tag=…/`:

* `summary.json`: `bands` (each size and probe with its verdict, rule
  values, failing rules, missed slots, node CPU at start), `knee` (last
  pass, first fail, probes), `points` (per size and op: count, p50, p90,
  p99, max, failures; `resume_staggered_calm` and `suspend_staggered_calm` are rules B and C's population),
  `warmup_by_band`, hard-failure counters, and the run config.
* `stats.csv`: locust percentiles per request name; `[n=...]` splits by
  size. `boot`, `suspend_warmup`, `practice_*` are warm-up ops.
* `node_ts.csv`: node CPU and memory, ready workers, restarts, every 10 s.
* `model_params.json`: measured resume/suspend/boot distributions for
  re-running the capacity model with real inputs.

## Things that bit us

* **Suspending an actor that just started.** OpenClaw answers its health
  check 2 s after start but keeps starting for ~45 s, 30 of them with
  its event loop blocked. A snapshot taken in that window resumes into
  the rest of that startup and answers nothing for 30 to 50 s. The
  practice cycle exists for this. Rule of thumb: a snapshot of a busy
  program resumes busy.
* **Restore throughput cap.** ateom-gvisor passes `-direct` to
  `runsc restore`, so the checkpoint pages file, just written by the
  atelet and sitting in the page cache, is read from disk with O_DIRECT.
  All restores on a node then share about 130 MB/s: 88 at once take
  190 s instead of 15 s. Tracked in agent-substrate/substrate issue 1530.
  Results measured with the flag on carry that cap.
* **Medians mislead.** Staggered resume times are two-humped (seconds or
  minutes, depending on whether the slot overlaps the herd). Quote rule
  B's calm-window numbers and p90, which reproduce across runs.
* Actors here are idle fresh boots (~765 MB), with no conversation
  state. Densities are an upper bound for busier actors.
