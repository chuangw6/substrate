#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The one command. Runs the Envoy capacity measurement end to end and leaves a
# self-contained run directory behind:
#
#   benchmarking/envoycap/runs/<timestamp>/
#     job.log       everything the Job printed
#     summary.json  the machine-readable step table (what the charts read)
#     nodecpu.json  node CPU over the run window, for the router and the workers
#     latency.svg   latency vs offered load, with the 500 ms budget line
#                   (one chart per pass, latency-pass<N>.svg, when --repeat > 1)
#     throughput.svg  achieved vs offered, with the y=x reference
#     report.html   every chart in one page, with hover tooltips
#
# Usage:
#   benchmarking/envoycap/run.sh [--actors N] [--start-qps N] [--max-qps N]
#                                [--steps N] [--step-duration D] [--repeat N]
#
# Defaults are the real experiment: 40 actors, 1000 -> 8000 QPS in 8 linear
# steps of 30 s, 2 passes. About 10 minutes.
#
# The ladder has been raised twice, each time because the previous top was not
# the knee. 2000 was inside the budget on 5 passes of 6; 3000 crossed it but on
# 8-vCPU nodes, where the crossing could not be separated from contention. The
# nodes are now 88 vCPU each and Envoy sizes its worker threads from the node,
# so the rate that was binding before is no longer expected to bind, and the
# ladder has to reach past it.
#
# 8000 is also close to where the generator itself stops being open-loop: its
# worker pool is capped at 20,000 in flight, which at 8000/s holds the loop open
# up to 2.5 s of latency and no further. Past that the dispatcher blocks, which
# the dispatch-lag guard reports rather than hides -- but the number would be a
# floor, not a measurement.
#
# Smoke test before a real run:
#   benchmarking/envoycap/run.sh --actors 2 --start-qps 10 --max-qps 20 \
#     --steps 2 --step-duration 10s --repeat 1

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"
# shellcheck source=benchmarking/envoycap/common.sh
source "${ROOT}/benchmarking/envoycap/common.sh"

# The six experiment knobs, passed straight through to the generator.
ACTORS=40
START_QPS=1000
MAX_QPS=8000
STEPS=8
STEP_DURATION=30s
REPEAT=2

ATESPACE="benchmark"
API_ENDPOINT="dns:///api.ate-system.svc.cluster.local:443"
ROUTER_URL="http://atenet-router.ate-system.svc.cluster.local"
JOB_NAMESPACE="benchmarking"

usage() {
  cat <<'EOF'
Usage: benchmarking/envoycap/run.sh [options]

  --actors N           Actors to create and round-robin over (default 40)
  --start-qps N        First rung of the ladder (default 1000)
  --max-qps N          Last rung of the ladder (default 8000)
  --steps N            Evenly spaced rungs from start to max (default 8)
  --step-duration D    How long to hold each rung (default 30s)
  --repeat N           Passes over the whole ladder (default 2)
  -h, --help           Show this help

Defaults are the real experiment: about 10 minutes. Smoke test first with
  --actors 2 --start-qps 10 --max-qps 20 --steps 2 --step-duration 10s --repeat 1
EOF
}

parse_args() {
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --actors) shift; ACTORS="$1" ;;
      --actors=*) ACTORS="${1#*=}" ;;
      --start-qps) shift; START_QPS="$1" ;;
      --start-qps=*) START_QPS="${1#*=}" ;;
      --max-qps) shift; MAX_QPS="$1" ;;
      --max-qps=*) MAX_QPS="${1#*=}" ;;
      --steps) shift; STEPS="$1" ;;
      --steps=*) STEPS="${1#*=}" ;;
      --step-duration) shift; STEP_DURATION="$1" ;;
      --step-duration=*) STEP_DURATION="${1#*=}" ;;
      --repeat) shift; REPEAT="$1" ;;
      --repeat=*) REPEAT="${1#*=}" ;;
      -h|--help) usage; exit 0 ;;
      *) echo "Error: unknown option: $1" >&2; usage; exit 1 ;;
    esac
    shift
  done
}

main() {
  parse_args "$@"

  envoycap::resolve_project
  envoycap::use_cluster --require-existing

  preflight
  local image
  image="$(build_image)"

  RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
  RUN_DIR="benchmarking/envoycap/runs/${RUN_ID}"
  mkdir -p "${RUN_DIR}"
  # A Job name is an RFC 1123 subdomain, so the ISO timestamp's "T" and "Z"
  # have to come down to lower case. The run directory keeps the readable form.
  JOB_NAME="envoycap-$(echo "${RUN_ID}" | tr '[:upper:]' '[:lower:]')"
  # ISO-8601 UTC, for the Cloud Monitoring window. Recorded before the Job
  # starts so setup time is inside the window rather than silently excluded.
  RUN_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  # From here on there is a Job in the cluster holding actors, so an interrupt
  # has to go through the cluster rather than just killing this shell.
  trap on_interrupt INT TERM

  launch_job "${image}"
  local exit_code=0
  follow_job || exit_code=$?

  trap - INT TERM

  RUN_END="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  extract_summary
  query_node_cpu
  render_charts
  report "${exit_code}"
  return "${exit_code}"
}

# on_interrupt hands a Ctrl-C on the driver through to the generator.
#
# Killing this shell on its own does nothing to the cluster: the Job keeps
# running with every actor resumed and holding a worker slot, which is exactly
# the leak the brief forbids. Deleting the Job sends SIGTERM to the generator,
# which suspends and deletes the pool on its way out
# (cmd/benchmarking/envoycap/main.go). Foreground cascade, because the default
# background delete returns as soon as the Job object is gone and would leave
# this shell exiting while teardown is still in flight; the timeout is the
# pod's 180 s grace period plus slack.
on_interrupt() {
  trap - INT TERM
  echo
  if [[ -n "${FOLLOW_PID:-}" ]]; then
    # disown as well as kill, or bash prints its own "Terminated" job-control
    # notice for the log follower right over the message that says what to do
    # next.
    kill "${FOLLOW_PID}" 2>/dev/null || true
    disown "${FOLLOW_PID}" 2>/dev/null || true
  fi
  envoycap::log "interrupted: deleting job ${JOB_NAME} so the generator tears its actors down"
  kubectl -n "${JOB_NAMESPACE}" delete job "${JOB_NAME}" \
    --cascade=foreground --wait --timeout=200s >/dev/null 2>&1 ||
    envoycap::log "WARNING: job delete did not complete; check for leftover actors"
  envoycap::log "verify with: go run ./cmd/kubectl-ate get actors -A"
  exit 130
}

preflight() {
  envoycap::log "preflight"

  # The corp environment reaps IAM bindings on the node service account, so
  # re-grant rather than assume they survived since the last run. Without
  # metricWriter the managed collection stops and the node-CPU evidence for
  # "it was not the rig" disappears; without artifactregistry.reader the Job's
  # image cannot be pulled.
  local sa="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
  for role in roles/monitoring.metricWriter roles/artifactregistry.reader; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
      --member "${sa}" --role "${role}" --condition None --quiet >/dev/null
  done
  envoycap::log "IAM bindings re-granted to ${sa}"

  # The measurement is only interpretable if the pieces are where provision.sh
  # put them. A router that drifted onto the load generator's node makes the
  # numbers a measurement of that node.
  local pinned
  pinned="$(kubectl -n ate-system get deployment atenet-router \
    -o jsonpath='{.spec.template.spec.nodeSelector.cloud\.google\.com/gke-nodepool}')"
  [[ "${pinned}" == "${ENVOYCAP_SYSTEM_POOL}" ]] ||
    envoycap::die "atenet-router is not pinned to ${ENVOYCAP_SYSTEM_POOL} (nodeSelector='${pinned}'). Re-run provision.sh."

  local replicas ready
  replicas="$(kubectl -n benchmark-workloads get workerpool benchmark-ateom -o jsonpath='{.spec.replicas}')"
  ready="$(kubectl -n benchmark-workloads get pods -l "${WORKER_POD_SELECTOR}" \
    -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' | grep -c True || true)"
  envoycap::log "WorkerPool benchmark-ateom: ${ready}/${replicas} pods ready"
  # One actor per worker pod is a hard cap. Asking for more actors than there
  # are free workers fails during setup, several minutes in.
  ((ready >= ACTORS)) ||
    envoycap::die "need ${ACTORS} ready worker pods, have ${ready}. Scale the WorkerPool or lower --actors."

  # A leftover pool from a killed run holds worker slots this run needs.
  #
  # Actors are not Kubernetes objects -- there is no actors.ate.dev CRD, they
  # live in the ateapi store -- so this has to ask ateapi. kubectl-ate
  # port-forwards to it on its own, which costs a few seconds and is the only
  # way to make this check mean anything.
  local leftover
  leftover="$(go run ./cmd/kubectl-ate get actors -A -o json 2>/dev/null |
    grep -c '"envoycap-' || true)"
  ((leftover == 0)) ||
    envoycap::die "${leftover} actors from a previous envoycap run are still present. Delete them before running: go run ./cmd/kubectl-ate get actors -A"
}

build_image() {
  envoycap::log "building and pushing the generator image"
  # ko prints the fully-qualified digest to stdout and its progress to stderr,
  # so the digest is captured while build failures stay visible. That digest
  # goes into both the pod spec and the run JSON, so the report names the exact
  # image that produced it.
  KO_DEFAULTPLATFORMS=linux/amd64 \
    envoycap::ko build ./cmd/benchmarking/envoycap | tail -1
}

launch_job() {
  local image="$1"

  local router_pod router_ip router_node
  router_pod="$(kubectl -n ate-system get pod -l app=atenet-router \
    -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "${router_pod}" ]] || envoycap::die "no atenet-router pod found in ate-system"
  router_ip="$(kubectl -n ate-system get pod "${router_pod}" -o jsonpath='{.status.podIP}')"
  router_node="$(kubectl -n ate-system get pod "${router_pod}" -o jsonpath='{.spec.nodeName}')"
  envoycap::log "router ${router_pod} at ${router_ip} on ${router_node}"
  echo "${router_node}" > "${RUN_DIR}/router-node.txt"

  local git_sha
  git_sha="$(git rev-parse --short HEAD)$(git diff --quiet || echo '-dirty')"

  sed -e "s|\${JOB_NAME}|${JOB_NAME}|g" \
      -e "s|\${IMAGE}|${image}|g" \
      -e "s|\${ACTORS}|${ACTORS}|g" \
      -e "s|\${START_QPS}|${START_QPS}|g" \
      -e "s|\${MAX_QPS}|${MAX_QPS}|g" \
      -e "s|\${STEPS}|${STEPS}|g" \
      -e "s|\${STEP_DURATION}|${STEP_DURATION}|g" \
      -e "s|\${REPEAT}|${REPEAT}|g" \
      -e "s|\${API_ENDPOINT}|${API_ENDPOINT}|g" \
      -e "s|\${ROUTER_URL}|${ROUTER_URL}|g" \
      -e "s|\${ATESPACE}|${ATESPACE}|g" \
      -e "s|\${ROUTER_POD_IP}|${router_ip}|g" \
      -e "s|\${ROUTER_NODE}|${router_node}|g" \
      -e "s|\${CLUSTER_NAME}|${ENVOYCAP_CLUSTER_NAME}|g" \
      -e "s|\${GIT_SHA}|${git_sha}|g" \
      benchmarking/envoycap/manifests/job.yaml.tmpl > "${RUN_DIR}/job.yaml"

  envoycap::log "launching job ${JOB_NAME}"
  kubectl apply -f "${RUN_DIR}/job.yaml"
}

follow_job() {
  envoycap::log "waiting for the generator pod to start"
  # Poll the phase rather than wait --for=condition=Ready: a pod that has
  # already finished never reports Ready, and a short smoke run can beat the
  # watch.
  local deadline=$((SECONDS + 300)) phase=""
  while ((SECONDS < deadline)); do
    phase="$(kubectl -n "${JOB_NAMESPACE}" get pod -l "job-name=${JOB_NAME}" \
      -o jsonpath='{.items[0].status.phase}' 2>/dev/null || true)"
    [[ "${phase}" == Running || "${phase}" == Succeeded || "${phase}" == Failed ]] && break
    sleep 3
  done
  if [[ -z "${phase}" || "${phase}" == Pending ]]; then
    # "Pending" on its own says nothing. The waiting reason is where the
    # answer is -- ImagePullBackOff, Unschedulable, and a slow node pull are
    # three different problems with the same phase.
    local why
    why="$(kubectl -n "${JOB_NAMESPACE}" get pod -l "job-name=${JOB_NAME}" \
      -o jsonpath='{range .items[*].status.containerStatuses[*]}{.state.waiting.reason}: {.state.waiting.message}{end}' 2>/dev/null || true)"
    envoycap::die "generator pod did not start (phase='${phase}'). ${why:-No container status yet.} See: kubectl -n ${JOB_NAMESPACE} describe job ${JOB_NAME}"
  fi

  # tee, not redirect: a run this long should be watchable.
  #
  # Backgrounded and waited on rather than run in the foreground, because bash
  # defers a trap until the running foreground command returns -- and this one
  # runs for the whole ladder. Ctrl-C would still work (the terminal signals the
  # whole process group), but a plain `kill` of the driver would hang until the
  # run finished, which is the opposite of what an interrupt is for.
  kubectl -n "${JOB_NAMESPACE}" logs -f "job/${JOB_NAME}" --all-containers \
    | tee "${RUN_DIR}/job.log" &
  FOLLOW_PID=$!
  wait "${FOLLOW_PID}" || true

  # Exit codes are meaningful: 3 is "the rig ran out", which must not be
  # reported the same way as a clean run.
  local code
  code="$(kubectl -n "${JOB_NAMESPACE}" get pod -l "job-name=${JOB_NAME}" \
    -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null)"
  return "${code:-1}"
}

extract_summary() {
  # The report is framed on stdout by sentinels so it can be sliced out of a
  # log that also carries slog lines. The chart script reads this file and
  # never parses the log.
  awk '/===ENVOYCAP-JSON-BEGIN===/{flag=1; next} /===ENVOYCAP-JSON-END===/{flag=0} flag' \
    "${RUN_DIR}/job.log" > "${RUN_DIR}/summary.json"

  if [[ ! -s "${RUN_DIR}/summary.json" ]]; then
    rm -f "${RUN_DIR}/summary.json"
    envoycap::log "WARNING: no framed JSON report in the log; the run did not get far enough to emit one"
    return
  fi
  envoycap::log "wrote ${RUN_DIR}/summary.json"
}

query_node_cpu() {
  local token
  # A plain user token is rejected with ACCESS_TOKEN_TYPE_UNSUPPORTED; the
  # Monitoring API wants an ADC token.
  token="$(gcloud auth application-default print-access-token 2>/dev/null)" || {
    envoycap::log "WARNING: no ADC token (gcloud auth application-default login); skipping node CPU"
    return
  }

  # The filter MUST pin cluster_name. This project's metrics scope contains
  # more than one cluster, and an unfiltered query silently sums across them
  # and returns a plausible wrong answer.
  local filter="metric.type=\"kubernetes.io/node/cpu/allocatable_utilization\" AND resource.labels.cluster_name=\"${ENVOYCAP_CLUSTER_NAME}\""

  envoycap::log "querying node CPU for ${RUN_START} .. ${RUN_END}"
  curl -sS -G "https://monitoring.googleapis.com/v3/projects/${PROJECT_ID}/timeSeries" \
    -H "Authorization: Bearer ${token}" \
    --data-urlencode "filter=${filter}" \
    --data-urlencode "interval.startTime=${RUN_START}" \
    --data-urlencode "interval.endTime=${RUN_END}" \
    --data-urlencode "aggregation.alignmentPeriod=30s" \
    --data-urlencode "aggregation.perSeriesAligner=ALIGN_MEAN" \
    --data-urlencode "aggregation.crossSeriesReducer=REDUCE_MEAN" \
    --data-urlencode "aggregation.groupByFields=resource.labels.node_name" \
    > "${RUN_DIR}/nodecpu.json" ||
    envoycap::log "WARNING: node CPU query failed; see ${RUN_DIR}/nodecpu.json"
}

render_charts() {
  [[ -s "${RUN_DIR}/summary.json" ]] || return 0
  envoycap::log "rendering charts"
  python3 benchmarking/envoycap/charts.py "${RUN_DIR}"
}

report() {
  local exit_code="$1"
  echo
  echo "Run directory: ${RUN_DIR}"
  echo
  case "${exit_code}" in
    0) ;;
    2) echo "The run was interrupted; the report covers the steps that completed." ;;
    3) echo "RIG LIMITED: the load generator, not the system under test, is what ran out." ;;
    *) echo "The run failed with exit code ${exit_code}; see ${RUN_DIR}/job.log." ;;
  esac
}

main "$@"
