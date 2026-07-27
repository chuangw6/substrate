# Clean Redeploy

How to take the benchmark environment down to nothing and rebuild it from a
given checkout, so that a measurement describes one build and not a mixture.

[clean_redeploy.sh](clean_redeploy.sh) does all of this in one command:

```bash
./benchmarking/clean_redeploy.sh --yes
```

The manual steps are below, because the script will eventually drift and because
knowing what it does is the difference between fixing a failure and re-running
it hoping for better luck.

If you only need first-time setup, read [GKE_SETUP.md](GKE_SETUP.md) instead.
This document assumes the cluster and the GCP resources already exist.

---

## Why not just re-apply

Two things make in-place updates unreliable, and both of them fail in ways that
look like something else.

**`ActorTemplate.spec` is immutable.** `ko` resolves images to digests, and the
digest moves whenever any transitive dependency changes — not just when you edit
the actor's own source. So `apply` fails with:

```
The ActorTemplate "glutton" is invalid: spec: Invalid value: Spec is immutable
```

even on a redeploy where nothing you wrote has changed. The error names the
template, not the digest, so it reads like a manifest problem.

**A partial rollout mixes builds.** The WorkerPool accepts a new `ateomImage`
immediately. If the new image cannot be pulled, the pods that already have the
old one cached keep running and the rest sit in `ImagePullBackOff`. Half the
capacity is on one build and half is absent, which is a plausible-looking
cluster and a meaningless measurement.

---

## 0. Grant the nodes registry read access

Do this first. Without it, every freshly built image fails to pull while the
already-cached ones keep working, so the symptom appears in step 7 with no
obvious connection to IAM.

```bash
go run ./tools/setup-gcp create iam
```

That grants the node service account `roles/artifactregistry.reader` and
`roles/storage.objectViewer`, and gives `atelet` its bucket access. It is
idempotent — it logs and skips when the policy already has the bindings.

Verify, and give it a minute to propagate:

```bash
gcloud projects get-iam-policy "${PROJECT_ID}" --format=json \
  | jq -r --arg sa "${PROJECT_NUMBER}-compute" \
      '.bindings[] | select(.members[]? | test($sa)) | .role'
# expect: roles/artifactregistry.reader, roles/storage.objectViewer
```

An empty result is normal rather than surprising: if your org enforces
`constraints/iam.automaticIamGrantsForDefaultServiceAccounts`, the default
Compute Engine service account starts with no project roles at all and there is
no Editor role to fall back on.

### What the failure looks like if you skip it

```
Failed to pull image "gcr.io/PROJECT/ate-images/ateom-gvisor-...@sha256:...":
failed to authorize: failed to fetch oauth token: unexpected status from GET
request to https://gcr.io/v2/token?scope=repository%3APROJECT%2Fate-images%2F...
&scope=repository%3APROJECT%2Fgcr.io%2Fate-images%2F...: 403 Forbidden
```

The second `scope` with `gcr.io` in the *path* is the giveaway: `gcr.io` is
redirecting to Artifact Registry, so the node's `devstorage.read_only` scope no
longer covers the pull.

---

## 1. Branch and environment

Whatever is checked out is what gets deployed.

```bash
cd <repo root>
git checkout <branch>
source .ate-dev-env.sh
```

Note which side of the connection-reuse fix you are on, because it determines
what a healthy result looks like:

```bash
grep -q MaxRequestsPerConnection cmd/atenet/internal/router/xds.go \
  && echo "fix present — expect low failures" \
  || echo "fix absent — expect ~40% 503s"
```

---

## 2. Delete actors before templates

Actors must go first or they are orphaned: an ActorTemplate owns its golden
actor, and nothing owns the ones boomer created. Deletion requires
`STATUS_SUSPENDED`.

```bash
kubectl ate get actors -A -o json \
  | jq -r '.actors[] | select(.metadata.atespace != "ate-golden")
           | [.metadata.atespace, .metadata.name] | @tsv' \
  | while IFS=$'\t' read -r as name; do
      echo "deleting ${as}/${name}"
      kubectl ate delete actor "$name" -a "$as"
    done

kubectl ate get actors -A -o json | jq '.actors | length'   # expect golden only
```

`-A` is required. Listing has no default atespace, and the golden actors live in
the reserved `ate-golden` one. Leave those alone; they go away with their
templates in the next step.

Note the `.metadata.` prefix. The fields are nested, and `.atespace` /
`.actorId` at the top level are not errors — they evaluate to `null`, so the
`select` passes every actor and the `@tsv` yields two empty strings. The loop
then deletes nothing and reports no failure.

If an actor is not suspended, resume-then-suspend it until it is —
`prepare_actor_for_delete` in [hack/install-ate.sh](../hack/install-ate.sh) is
the reference loop.

---

## 3. Tear down the benchmark stack

```bash
./benchmarking/deploy_locust.sh --delete
kubectl ate delete atespace benchmark
kubectl get ns benchmarking benchmark-workloads   # expect NotFound
```

Boomer recreates the `benchmark` atespace at startup
([boomer-glutton/main.go](../cmd/benchmarking/boomer-glutton/main.go)), so there
is nothing to pre-create on the way back up.

---

## 4. Tear down the substrate

```bash
./hack/install-ate.sh --delete-all
kubectl get ns ate-system   # expect NotFound
```

`--delete-all` covers the demos and `ate-system` but **not** the benchmark
stack, which is why step 3 is separate and has to come first.

---

## 5. Clear old snapshots (optional)

Golden snapshots from deleted templates are dead weight, and stale ones are one
more thing a run could accidentally pick up.

```bash
gcloud storage rm -r "gs://${BUCKET_NAME}/benchmark-workloads/"
```

---

## 6. Reinstall the substrate

```bash
./hack/install-ate.sh --deploy-ate-system
kubectl get pods -n ate-system -w   # all Running before continuing
```

This recompiles the monorepo, so it is the slow step.

---

## 7. Redeploy the benchmark stack

`ko` authenticates through the Google keychain, but `docker push` reads
`~/.docker/config.json` and will push anonymously without this:

```bash
gcloud auth configure-docker us-docker.pkg.dev

./benchmarking/deploy_locust.sh --deploy --worker-count 50
```

```bash
kubectl get pods -n benchmark-workloads --no-headers | awk '{print $3}' | sort | uniq -c
kubectl get pods -n benchmarking
```

Want 50 Running with nothing in `ImagePullBackOff`, and locust at 3/3. Anything
stuck on an image pull means step 0 did not take.

---

## 8. Confirm which router you are running

Do not skip this. A rejected CDS push is silent — the router pod stays 2/2
Running and every actor request 503s, which looks like a load problem rather
than a config one.

```bash
kubectl port-forward -n ate-system deploy/atenet-router 19901:9901 &
sleep 2
curl -s localhost:19901/config_dump | \
  jq -r '.configs[] | select(.["@type"]|test("Clusters")) | .dynamic_active_clusters[]
         | select(.cluster.name=="dynamic_forward_proxy_cluster")
         | "has_http_opts=\(.cluster|has("typed_extension_protocol_options"))"'
```

| Output | Meaning |
|---|---|
| `has_http_opts=true` | Connection reuse is disabled — the fix is live |
| `has_http_opts=false` | Connections are pooled — reproducing the failure |
| *(no output at all)* | The cluster is missing entirely. Envoy NACKed the push; check the `atenet` container logs |

That third case is why the query prints a computed string rather than the field
itself. `jq` emitting `null` and `jq` emitting nothing look similar in a
terminal but mean opposite things.

---

## 9. Run

Reset the counters first so the numbers describe this run and not the last one.

### Headless

Use this for anything whose numbers you intend to quote. It writes stats CSVs,
JSONL, and `traces.txt` — a tab-separated file of per-request `time, name,
duration_ms, latency_source, trace_id, err`. That `latency_source` column
(`server` vs `client`) is the cleanest signal available for separating server
compute from network and queueing time, and the web UI does not produce it.

```bash
curl -s -XPOST localhost:19901/reset_counters

kubectl exec -n benchmarking deploy/locust -c locust-master -- \
  python /app/runner.py \
    -f /app/tests/glutton.py -t 2m -u 50 \
    --tag <branch>-<sha> --name glutton_50 --dest /tmp/results \
    --min-wait-time 1.0 --max-wait-time 1.0 --trace-probability 1.0

curl -s localhost:19901/stats | grep -E \
  'dynamic_forward_proxy_cluster\.(upstream_rq_total|upstream_cx_total|upstream_rq_503|upstream_cx_connect_fail|upstream_cx_destroy_remote_with_active_rq): '
```

Envoy renders stats as `name: value`, so anchor the grep on the colon. A `$`
anchor matches nothing and reads as "the counter is zero".

### From the web UI

Better when you are exploring — you can watch failures appear in real time and
stop early instead of waiting out a fixed duration.

```bash
kubectl port-forward svc/locust -n benchmarking 8089:8089
```

Then at <http://localhost:8089>:

1. **Class picker: select `GluttonUser` only.** Deselect `AteAPIUser`,
   `SleepUser`, and `CounterUser`. They measure different things and will
   pollute the stats table.
2. **Number of users** `50`, **Ramp up** `5`.
3. Expand **Advanced options**. Three custom fields appear there, registered by
   [common/wait_time.py](locust/common/wait_time.py) and
   [common/trace.py](locust/common/trace.py):

   | Field | Value |
   |---|---|
   | `min-wait-time` | `1.0` |
   | `max-wait-time` | `1.0` |
   | `trace-probability` | `1.0` |

4. Reset the Envoy counters **before** pressing Start, in the terminal holding
   the admin forward:

   ```bash
   curl -s -XPOST localhost:19901/reset_counters
   ```

5. Start. Watch the `GluttonPing` row's fail percentage; without the fix it
   climbs into the high thirties within the first thirty seconds.
6. Stop, then read the Envoy counters with the same `grep` as above. Those are
   what identify the mechanism — the UI only shows you that something failed.

Two things about this path that are easy to get wrong:

**The three fields are read by the Go worker, not by Python.** `boomer-glutton`
fetches them from `http://{master}:8089/boomer-config` on every spawn message
([locust.yaml](locust/manifests/locust.yaml), `--master-web-port=8089`). So
editing a field mid-run does nothing; you have to stop and start again for the
new value to be picked up.

**Nothing is archived.** The web UI writes no CSVs and no `traces.txt`, and the
numbers vanish when you start the next run. Screenshot it or re-run headless.

### Getting the result files out

`kubectl cp` does not work here. The locust image is
`gcr.io/distroless/python3-debian12`, which has no `tar` and no shell for
`kubectl cp` to drive. The base does have python, so stream the archive out with
the standard library instead:

```bash
kubectl exec -n benchmarking deploy/locust -c locust-master -- \
  python -c 'import sys,tarfile; t=tarfile.open(fileobj=sys.stdout.buffer,mode="w|"); t.add("/tmp/results",arcname="results"); t.close()' \
  | tar xf - -C ./benchmarking/results/
```

---

## Reading the result

The failure percentage alone does not identify the mechanism. The counter ratio
does.

| Signal | Connections reused | Reuse disabled |
|---|---|---|
| `upstream_rq_total` / `upstream_cx_total` | ≈ 1.9 | ≈ 1.0 |
| `upstream_cx_destroy_remote_with_active_rq` | a few hundred | 0 |
| `GluttonPing` failures | ~40% | low single digits |

If failures are high but `upstream_cx_destroy_remote_with_active_rq` is 0, this
is not the connection-reuse bug and the rest of [FINDINGS.md](FINDINGS.md) will
not explain it. Start from `upstream_cx_connect_fail` instead.

---

## A/B between two builds

Reinstall the substrate on each side, but reuse the workloads and the locust
image — only the router differs, and rebuilding the rest introduces variables
the comparison cannot separate.

```bash
git checkout wip/benchmarking-repro        # without the fix
./benchmarking/clean_redeploy.sh --yes

git checkout wip/benchmarking-all          # with the fix
./benchmarking/clean_redeploy.sh --yes --skip-iam --skip-teardown --skip-build
```

One caveat worth stating in any writeup: this is a single-cluster A/B, so
anything that drifted between the runs — node pressure, GCS latency, commits the
branch picked up from `main` — lands in the delta alongside the change you meant
to measure. The Envoy counters isolate cleanly. The locust percentage does not.
