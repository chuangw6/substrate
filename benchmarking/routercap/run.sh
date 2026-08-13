#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Measures one atenet-router configuration: one Envoy CPU limit, one ladder of
# offered load, one stats.jsonl out. Sweeping several CPU limits means several
# invocations, which is what lets each one be a separate CI entry. Unattended
# by design; everything that writes to the cluster lives here, and load
# generation (which does not) lives in the binary.
#
#   0  clean       1  failed       2  interrupted
#   3  rig-limited 4  preflight/provisioning
#
# Exit 3 still produced valid data up to the rung that tripped: the rig ran
# out, not the router.

# shellcheck source=benchmarking/routercap/common.sh
source "$(git rev-parse --show-toplevel)/benchmarking/routercap/common.sh"

CPU_LIMIT="${RC_CPU_LIMIT_DEFAULT}"
ACTORS=100
START_QPS=1000
STEP_QPS=1000
RUNGS=16
HOLD_S=45
WARMUP_S=10
# Not a knob: this is the denominator of the generator's own CPU guard, which
# trips at 80% of it. 80, not 88, because the loadgen node is one
# c3-standard-88 and a request for all of it would sit Pending; change only
# alongside the loadgen pool's machine type.
LOADGEN_CPU=80
LOADGEN_MEMORY=64Gi
SIDECAR_CORES="${RC_SIDECAR_CORES}"
OUTPUT_DIR=""
TAG=""
IMAGE="${ROUTERCAP_IMAGE:-}"
SMOKE=false

usage() {
  cat <<EOF
Usage: $0 [options]

  --cpu-limit N         Envoy CPU limit to measure, in cores (default: ${CPU_LIMIT}).
  --actors N            Actors to warm, one per worker pod (default: ${ACTORS}).
  --start-qps N         First rung (default: ${START_QPS}).
  --step-qps N          Added by each rung (default: ${STEP_QPS}).
  --rungs N             Rungs in the ladder (default: ${RUNGS}).
  --hold N              Seconds per rung (default: ${HOLD_S}).
  --warmup N            Leading seconds of each rung marked warmup (default: ${WARMUP_S}).
  --output-dir DIR      Where stats.jsonl lands (default: benchmarking/routercap/runs/<utc timestamp>).
  --tag T               Run tag; defaults to the short commit, with -dirty if the tree is.
  --image REF           Skip the ko build and use this image.
  --smoke               2 actors, 3 short rungs. Proves the rig, measures nothing.
  -h, --help            This.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cpu-limit) shift; CPU_LIMIT="$1" ;;
    --cpu-limit=*) CPU_LIMIT="${1#*=}" ;;
    --actors) shift; ACTORS="$1" ;;
    --actors=*) ACTORS="${1#*=}" ;;
    --start-qps) shift; START_QPS="$1" ;;
    --start-qps=*) START_QPS="${1#*=}" ;;
    --step-qps) shift; STEP_QPS="$1" ;;
    --step-qps=*) STEP_QPS="${1#*=}" ;;
    --rungs) shift; RUNGS="$1" ;;
    --rungs=*) RUNGS="${1#*=}" ;;
    --hold) shift; HOLD_S="$1" ;;
    --hold=*) HOLD_S="${1#*=}" ;;
    --warmup) shift; WARMUP_S="$1" ;;
    --warmup=*) WARMUP_S="${1#*=}" ;;
    --output-dir) shift; OUTPUT_DIR="$1" ;;
    --output-dir=*) OUTPUT_DIR="${1#*=}" ;;
    --tag) shift; TAG="$1" ;;
    --tag=*) TAG="${1#*=}" ;;
    --image) shift; IMAGE="$1" ;;
    --image=*) IMAGE="${1#*=}" ;;
    --smoke) SMOKE=true ;;
    -h|--help) usage; exit 0 ;;
    *) rc::die "unknown option: $1" ;;
  esac
  shift
done

if [[ "${SMOKE}" == "true" ]]; then
  # Proves the rig end to end; measures nothing. Three rungs is the fewest
  # that gives the ladder a slope.
  ACTORS=2
  RUNGS=3
  HOLD_S=30
  WARMUP_S=5
  START_QPS=200
  STEP_QPS=200
fi

if ! [[ "${CPU_LIMIT}" =~ ^[0-9]+$ ]] || (( CPU_LIMIT < 1 )); then
  rc::die "--cpu-limit must be a whole number of cores, got '${CPU_LIMIT}'"
fi
if (( CPU_LIMIT > RC_MAX_CPU_LIMIT )); then
  # The router pod would sit Pending and the run would measure the pod that is
  # still running, under the new limit's label.
  rc::die "--cpu-limit ${CPU_LIMIT} is above ROUTERCAP_MAX_CPU_LIMIT (${RC_MAX_CPU_LIMIT}), which is what provision.sh sized the router node against"
fi

rc::need kubectl git go python3
rc::env
rc::assert_cluster

if [[ -z "${TAG}" ]]; then
  TAG="$(git -C "${ROOT}" rev-parse --short HEAD)"
  if [[ -n "$(git -C "${ROOT}" status --porcelain)" ]]; then
    # A run tagged with a clean commit that was not the code that ran is worse
    # than no tag at all.
    TAG="${TAG}-dirty"
  fi
fi
if [[ -z "${OUTPUT_DIR}" ]]; then
  OUTPUT_DIR="${RC_DIR}/runs/$(date -u +%Y%m%dT%H%M%SZ)"
fi
mkdir -p "${OUTPUT_DIR}"

# --- preflight ---------------------------------------------------------------

rc::step "preflight"
worker_pods="$(rc::kubectl -n "${RC_WORKER_NS}" get pods -l "${RC_WORKER_SELECTOR}" \
  --field-selector=status.phase=Running -o name | wc -l | tr -d ' ')"
if (( worker_pods < ACTORS )); then
  rc::die "${worker_pods} worker pods are Running but ${ACTORS} actors were asked for; one actor per pod is what keeps the per-worker connection-rate limit from binding before the concurrency limit"
fi
rc::step "${worker_pods} worker pods running"

# A generator that does not fit its node sits Pending until the run times out.
# Checked here rather than in provision.sh because LOADGEN_CPU lives here and
# the loadgen pool can be resized between a provision and a run.
loadgen_alloc="$(rc::kubectl get node -l "${RC_ROLE_KEY}=${RC_POOL_LOADGEN}" \
  -o jsonpath='{.items[0].status.allocatable.cpu}' 2>/dev/null || true)"
if [[ -n "${loadgen_alloc}" ]]; then
  if [[ "${loadgen_alloc}" == *m ]]; then lg_m="${loadgen_alloc%m}"; else lg_m=$(( loadgen_alloc * 1000 )); fi
  # 3 cores for atelet and GKE's own DaemonSets, same allowance provision.sh uses.
  if (( (LOADGEN_CPU + 3) * 1000 > lg_m )); then
    rc::die "the generator's ${LOADGEN_CPU} cores do not fit the loadgen node (${lg_m}m allocatable, 3 cores reserved for DaemonSets); the Job would sit Pending. Lower LOADGEN_CPU in run.sh or grow the pool's machine type"
  fi
fi

rc::kubectl apply -f "${RC_DIR}/manifests/rbac.yaml" >/dev/null

if [[ -z "${IMAGE}" ]]; then
  rc::step "building the generator image"
  ldflags=()
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -n "${line}" ]] && ldflags+=("--ldflags=${line}")
  done < <(make -C "${ROOT}" ldflags)
  # In a subshell at the repo root: ko resolves ./cmd/... against its own
  # working directory.
  IMAGE="$(cd "${ROOT}" && "${ROOT}/hack/run-tool.sh" ko build --platform=linux/amd64 \
    "${ldflags[@]}" ./cmd/benchmarking/routercap | tail -1)"
fi
rc::step "generator image: ${IMAGE}"

# --- the router ---------------------------------------------------------------

# patch_router resizes the envoy container to the CPU limit under test. Envoy
# >= 1.37 sizes its worker threads from the cgroup limit when the manifest
# passes --cpuset-threads, so nothing sets --concurrency; the binary scrapes
# envoy_server_concurrency back and refuses to run if that did not take.
# Recreate strategy = a fresh pod, and the sidecar stays pinned so the two
# containers remain separable.
patch_router() {
  local cores="$1" patch=""
  patch="$(rc::kubectl -n "${RC_ROUTER_NS}" get deployment atenet-router -o json | python3 -c '
import json, sys

cores, sidecar = sys.argv[1], sys.argv[2]
spec = json.load(sys.stdin)["spec"]["template"]["spec"]
# Replaces the whole strategy object, so any rollingUpdate block goes with it;
# leaving one behind alongside type Recreate is rejected by the API server.
ops = [{"op": "replace", "path": "/spec/strategy", "value": {"type": "Recreate"}},
       {"op": "replace", "path": "/spec/replicas", "value": 1}]
seen = False
for i, c in enumerate(spec["containers"]):
    envoy = c["name"] == "envoy"
    seen = seen or envoy
    value = cores if envoy else sidecar
    # The whole resources object, carried over from the live spec and with only
    # cpu changed. A per-field "replace" on .../resources/requests/cpu is what
    # this used to do and the API server rejects it whenever the container
    # declares no resources at all — which the shipped manifest does not, so
    # nothing was ever resized. Rebuilding the object keeps memory and any
    # other resource exactly as the manifest set them.
    resources = dict(c.get("resources") or {})
    for field in ("requests", "limits"):
        resources[field] = dict(resources.get(field) or {}, cpu=value)
    ops.append({"op": "add",
                "path": "/spec/template/spec/containers/%d/resources" % i,
                "value": resources})
    if not envoy:
        continue
    # The whole experiment is that the thread count follows the CPU limit, so
    # check the two flags that decide it before spending a rollout and a ladder
    # on a router that would be labelled with a thread count it is not running.
    # --cpuset-threads is what reaches the cgroup detection in Envoy 1.37 at
    # all; without it the default is the node core count no matter what the
    # limit says. An explicit --concurrency then overrides even that.
    flags = (c.get("command") or []) + (c.get("args") or [])
    if "--concurrency" in flags:
        sys.exit("the envoy container passes --concurrency, which overrides the cgroup-aware thread default; remove it from the router manifest")
    if "--cpuset-threads" not in flags:
        sys.exit("the envoy container does not pass --cpuset-threads, so Envoy sizes its workers from the node core count and ignores the CPU limit; add it to the router manifest")
if not seen:
    sys.exit("no container named envoy in the atenet-router Deployment")
json.dump(ops, sys.stdout)
' "${cores}" "${SIDECAR_CORES}")" || return 4
  # Checked, not chained: a rejected patch leaves the previous pod running and
  # perfectly Ready, so letting rollout status supply this function's exit
  # status reports success for a router that was never resized.
  if ! rc::kubectl -n "${RC_ROUTER_NS}" patch deployment atenet-router --type=json -p "${patch}" >/dev/null; then
    rc::warn "the API server rejected the resize patch"
    return 4
  fi
  rc::kubectl -n "${RC_ROUTER_NS}" rollout status deployment/atenet-router --timeout=10m
}

# wait_pod_started blocks until the Job's pod is past Pending, so the log
# stream that follows starts at the first line rather than erroring out.
wait_pod_started() {
  local job="$1" deadline=$((SECONDS + 600))
  while (( SECONDS < deadline )); do
    local phase=""
    phase="$(rc::kubectl -n "${RC_JOB_NS}" get pod -l "job-name=${job}" \
      -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)"
    case "${phase}" in
      Running|Succeeded|Failed) return 0 ;;
    esac
    sleep 2
  done
  return 1
}

# job_exit_code reads the generator's own exit status. The binary distinguishes
# rig-limited from failed from interrupted, and collapsing that to "the Job
# failed" would throw away the only bit that says whether the number is usable.
job_exit_code() {
  local job="$1" deadline=$((SECONDS + 300))
  while (( SECONDS < deadline )); do
    local code=""
    code="$(rc::kubectl -n "${RC_JOB_NS}" get pod -l "job-name=${job}" \
      -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || true)"
    if [[ -n "${code}" ]]; then
      echo "${code}"
      return 0
    fi
    sleep 2
  done
  echo 1
}

run_ladder() {
  rc::step "patching the router to ${CPU_LIMIT} cores"
  # Checked explicitly: the run is driven with errexit off so a rig-limited
  # ladder does not unwind before its exit code is read.
  if ! patch_router "${CPU_LIMIT}"; then
    rc::warn "could not resize the router; refusing to measure the previous pod under a ${CPU_LIMIT}-core label"
    return 4
  fi

  local job
  job="routercap-${CPU_LIMIT}c-$(date -u +%H%M%S)"
  local deadline=$(( RUNGS * HOLD_S + 900 ))

  rc::step "launching ${job} (deadline ${deadline}s)"
  sed \
    -e "s|\${JOB_NAME}|${job}|g" \
    -e "s|\${IMAGE}|${IMAGE}|g" \
    -e "s|\${CPU_LIMIT}|${CPU_LIMIT}|g" \
    -e "s|\${ACTORS}|${ACTORS}|g" \
    -e "s|\${START_QPS}|${START_QPS}|g" \
    -e "s|\${STEP_QPS}|${STEP_QPS}|g" \
    -e "s|\${RUNGS}|${RUNGS}|g" \
    -e "s|\${HOLD}|${HOLD_S}s|g" \
    -e "s|\${WARMUP}|${WARMUP_S}s|g" \
    -e "s|\${NAME}|routercap|g" \
    -e "s|\${TAG}|${TAG}|g" \
    -e "s|\${LOADGEN_CPU}|${LOADGEN_CPU}|g" \
    -e "s|\${LOADGEN_MEMORY}|${LOADGEN_MEMORY}|g" \
    -e "s|\${DEADLINE}|${deadline}|g" \
    -e "s|\${ROLE_KEY}|${RC_ROLE_KEY}|g" \
    "${RC_DIR}/manifests/job.yaml.tmpl" > "${OUTPUT_DIR}/job.yaml"

  # A placeholder added to the template but not to the sed list above would
  # otherwise be applied verbatim — a run labelled with something it did not
  # do.
  local unrendered
  # shellcheck disable=SC2016  # literal ${VAR} placeholders are the search target
  unrendered="$(grep -o '\${[A-Z_]\+}' "${OUTPUT_DIR}/job.yaml" | sort -u | tr '\n' ' ')"
  if [[ -n "${unrendered}" ]]; then
    rc::warn "job.yaml still contains ${unrendered}— add it to the substitution list in run.sh"
    return 4
  fi

  rc::kubectl apply -f "${OUTPUT_DIR}/job.yaml" >/dev/null

  if ! wait_pod_started "${job}"; then
    rc::kubectl -n "${RC_JOB_NS}" describe job "${job}" >"${OUTPUT_DIR}/job.describe" 2>&1 || true
    rc::warn "pod never started; see ${OUTPUT_DIR}/job.describe"
    rc::kubectl -n "${RC_JOB_NS}" delete job "${job}" --wait=false >/dev/null 2>&1 || true
    return 4
  fi

  # Streamed, not collected at the end: nothing can read files back out of the
  # distroless container, and streaming keeps every line an interrupted run
  # already emitted.
  # errexit is saved and restored, never forced on: errexit would turn a
  # rig-limited run into a lost stats.jsonl.
  local errexit_was="off"
  [[ $- == *e* ]] && errexit_was="on"
  set +o errexit
  rc::kubectl -n "${RC_JOB_NS}" logs -f "job/${job}" --tail=-1 \
    | python3 "${RC_DIR}/demux.py" "${OUTPUT_DIR}"
  [[ "${errexit_was}" == "on" ]] && set -o errexit

  local code
  code="$(job_exit_code "${job}")"
  rc::kubectl -n "${RC_JOB_NS}" delete job "${job}" --cascade=foreground --wait=true >/dev/null 2>&1 || true

  # A clean run keeps only the data. Debugging material survives exactly when
  # the run did not exit clean, which is when someone will want it.
  if [[ "${code}" -eq 0 ]]; then
    rm -f "${OUTPUT_DIR}/job.yaml" "${OUTPUT_DIR}/job.log"
  fi
  return "${code}"
}

# --- the run -------------------------------------------------------------------

# Ctrl-C deletes the Job with a foreground cascade, which SIGTERMs the
# generator, which suspends and deletes every actor on the way out. Without
# this a hundred actors survive the run that created them.
# shellcheck disable=SC2317,SC2329  # invoked via the trap below, not by call
cleanup() {
  local jobs=""
  jobs="$(rc::kubectl -n "${RC_JOB_NS}" get jobs -l app=routercap -o name 2>/dev/null || true)"
  if [[ -n "${jobs}" ]]; then
    rc::warn "interrupted; deleting ${jobs}"
    # shellcheck disable=SC2086
    rc::kubectl -n "${RC_JOB_NS}" delete ${jobs} --cascade=foreground --wait=true >/dev/null 2>&1 || true
  fi
  exit 2
}
trap cleanup INT TERM

rc::step "run: ${CPU_LIMIT} cores · ${RUNGS} rungs · ${HOLD_S}s each · ${ACTORS} actors · tag ${TAG}"
rc::step "output: ${OUTPUT_DIR}"

set +o errexit
run_ladder
code=$?
set -o errexit

trap - INT TERM

case "${code}" in
  0) rc::step "complete" ;;
  2) rc::warn "interrupted" ;;
  3) rc::warn "RIG-LIMITED — the rig ran out before the router did; the windows up to the trip stand, the ladder stopped there" ;;
  4) rc::warn "preflight failed; nothing was measured" ;;
  *) rc::warn "failed (exit ${code})" ;;
esac

# summarize.py is a convenience for a human reading the run directory: it reads
# stats.jsonl and nothing downstream reads what it writes, so its failure never
# changes the run's exit code.
if [[ -s "${OUTPUT_DIR}/stats.jsonl" ]]; then
  set +o errexit
  python3 "${RC_DIR}/summarize.py" "${OUTPUT_DIR}/stats.jsonl" > "${OUTPUT_DIR}/summary.json"
  summary_code=$?
  set -o errexit
  if (( summary_code != 0 )); then
    rc::warn "summarize.py failed (exit ${summary_code}); stats.jsonl is intact, re-run: python3 ${RC_DIR}/summarize.py ${OUTPUT_DIR}/stats.jsonl"
    rm -f "${OUTPUT_DIR}/summary.json"
  fi
fi

rc::step "done: ${OUTPUT_DIR}"
exit "${code}"
