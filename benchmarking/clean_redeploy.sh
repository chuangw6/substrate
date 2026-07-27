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

# clean_redeploy.sh tears the benchmark environment down to nothing, rebuilds it
# from the current checkout, and runs the glutton scale test.
#
# It exists because a partial redeploy is the usual way to waste an afternoon
# here. Two things make in-place updates unreliable:
#
#   1. ActorTemplate.spec is immutable. ko resolves images to digests, and any
#      dependency change moves the digest, so `apply` fails with "Spec is
#      immutable" even when nothing you wrote has changed.
#   2. A partial rollout leaves some pods on the old image and some on the new
#      one, which quietly turns a comparison run into a mixture of two builds.
#
# Whatever is checked out is what gets deployed. The script reads xds.go to work
# out whether the connection-reuse fix is present, then asserts the live Envoy
# config agrees — so it is equally usable for reproducing the failure (a branch
# without the fix) and for confirming the fix (a branch with it).
#
# Usage:
#   ./benchmarking/clean_redeploy.sh                     # full run, prompts once
#   ./benchmarking/clean_redeploy.sh --yes               # no prompt
#   ./benchmarking/clean_redeploy.sh --skip-teardown     # rebuild in place
#   ./benchmarking/clean_redeploy.sh --skip-run          # deploy only
#   ./benchmarking/clean_redeploy.sh --users 10 --worker-count 10

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------

WORKER_COUNT=50
USERS=50
DURATION="2m"
MIN_WAIT="1.0"
MAX_WAIT="1.0"
TRACE_PROBABILITY="1.0"
RUN_NAME=""
RUN_TAG=""
ENVOY_ADMIN_PORT=19901

SKIP_IAM=0
SKIP_TEARDOWN=0
SKIP_BUILD=0
SKIP_RUN=0
WIPE_SNAPSHOTS=0
ASSUME_YES=0

usage() {
  cat <<'EOF'
Usage: ./benchmarking/clean_redeploy.sh [options]

Scale:
  --worker-count N     WorkerPool replicas (default: 50)
  --users N            Concurrent locust users (default: 50)
  --duration D         Run duration, locust syntax (default: 2m)
  --min-wait S         Boomer min wait between iterations (default: 1.0)
  --max-wait S         Boomer max wait between iterations (default: 1.0)
  --trace-probability P  Span sampling rate (default: 1.0)

Labelling:
  --name NAME          Run name (default: glutton_<users>_<fix|nofix>)
  --tag TAG            Run tag (default: <branch>-<short sha>)

Phases (all run by default):
  --skip-iam           Do not reconcile GCP IAM bindings
  --skip-teardown      Do not delete actors, benchmark stack, or substrate
  --skip-build         Do not rebuild the locust image
  --skip-run           Deploy and verify, but do not run the load test
  --wipe-snapshots     Also delete GCS snapshots under benchmark-workloads/

Other:
  --admin-port P       Local port for the Envoy admin forward (default: 19901)
  -y, --yes            Do not prompt before destructive steps
  -h, --help           Show this message
EOF
}

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --worker-count) shift; WORKER_COUNT="$1" ;;
    --worker-count=*) WORKER_COUNT="${1#*=}" ;;
    --users) shift; USERS="$1" ;;
    --users=*) USERS="${1#*=}" ;;
    --duration) shift; DURATION="$1" ;;
    --duration=*) DURATION="${1#*=}" ;;
    --min-wait) shift; MIN_WAIT="$1" ;;
    --min-wait=*) MIN_WAIT="${1#*=}" ;;
    --max-wait) shift; MAX_WAIT="$1" ;;
    --max-wait=*) MAX_WAIT="${1#*=}" ;;
    --trace-probability) shift; TRACE_PROBABILITY="$1" ;;
    --trace-probability=*) TRACE_PROBABILITY="${1#*=}" ;;
    --name) shift; RUN_NAME="$1" ;;
    --name=*) RUN_NAME="${1#*=}" ;;
    --tag) shift; RUN_TAG="$1" ;;
    --tag=*) RUN_TAG="${1#*=}" ;;
    --admin-port) shift; ENVOY_ADMIN_PORT="$1" ;;
    --admin-port=*) ENVOY_ADMIN_PORT="${1#*=}" ;;
    --skip-iam) SKIP_IAM=1 ;;
    --skip-teardown) SKIP_TEARDOWN=1 ;;
    --skip-build) SKIP_BUILD=1 ;;
    --skip-run) SKIP_RUN=1 ;;
    --wipe-snapshots) WIPE_SNAPSHOTS=1 ;;
    -y|--yes) ASSUME_YES=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Error: unknown option: $1" >&2; usage; exit 1 ;;
  esac
  shift
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PHASE=0

log_phase() {
  PHASE=$((PHASE + 1))
  echo
  echo "=============================================================="
  echo "[${PHASE}] $*"
  echo "=============================================================="
}

log() { echo "    $*"; }

die() { echo "error: $*" >&2; exit 1; }

confirm() {
  ((ASSUME_YES)) && return 0
  local reply
  read -r -p "$1 [y/N] " reply
  [[ "${reply}" =~ ^[Yy]$ ]] || die "aborted"
}

require() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required but not on PATH${2:+ ($2)}"
}

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

[[ -f .ate-dev-env.sh ]] || die ".ate-dev-env.sh not found in ${ROOT}"
# shellcheck disable=SC1091
source .ate-dev-env.sh

: "${PROJECT_ID:?PROJECT_ID must be set by .ate-dev-env.sh}"
: "${PROJECT_NUMBER:?PROJECT_NUMBER must be set by .ate-dev-env.sh}"
: "${BUCKET_NAME:?BUCKET_NAME must be set by .ate-dev-env.sh}"

require kubectl
require jq
require gcloud
require go
require docker "needed to build the locust image; pass --skip-build to reuse the deployed one"
command -v kubectl-ate >/dev/null 2>&1 || {
  log "kubectl-ate not found, installing"
  go install ./cmd/kubectl-ate
  command -v kubectl-ate >/dev/null 2>&1 \
    || die "kubectl-ate still not on PATH; add \$(go env GOPATH)/bin"
}

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
SHA="$(git rev-parse --short HEAD)"

# The single source of truth for what this checkout does. Everything downstream
# (run name, post-deploy assertion, closing summary) is derived from it rather
# than from a flag, so the script cannot claim to have tested something it did
# not deploy.
if grep -q 'MaxRequestsPerConnection' cmd/atenet/internal/router/xds.go; then
  HAS_FIX=1
  FIX_LABEL="fix"
else
  HAS_FIX=0
  FIX_LABEL="nofix"
fi

[[ -n "${RUN_NAME}" ]] || RUN_NAME="glutton_${USERS}_${FIX_LABEL}"
[[ -n "${RUN_TAG}" ]] || RUN_TAG="${BRANCH//\//-}-${SHA}"

RESULTS_DIR="${ROOT}/benchmarking/results/${RUN_NAME}-${RUN_TAG}"

echo "=============================================================="
echo " Substrate benchmark clean redeploy"
echo "=============================================================="
echo "  project        ${PROJECT_ID} (${PROJECT_NUMBER})"
echo "  context        $(kubectl config current-context)"
echo "  branch         ${BRANCH} @ ${SHA}"
echo "  conn-reuse fix $( ((HAS_FIX)) && echo "PRESENT (expect low failures)" || echo "ABSENT (expect ~40% 503s)" )"
echo "  workers        ${WORKER_COUNT}"
echo "  users          ${USERS} for ${DURATION}"
echo "  run            ${RUN_NAME} / ${RUN_TAG}"
echo

if ! git diff --quiet || ! git diff --cached --quiet; then
  log "warning: working tree is dirty; the deployed build will not match ${SHA}"
fi

((SKIP_TEARDOWN)) || confirm "This DELETES all actors, the benchmark stack, and ate-system. Continue?"

# ---------------------------------------------------------------------------
# Envoy admin access
#
# There is no curl in the envoy sidecar, so the admin interface is only
# reachable through a port-forward from the workstation.
# ---------------------------------------------------------------------------

ENVOY_PF_PID=""

cleanup() {
  [[ -n "${ENVOY_PF_PID}" ]] && kill "${ENVOY_PF_PID}" 2>/dev/null || true
}
trap cleanup EXIT

start_envoy_forward() {
  [[ -n "${ENVOY_PF_PID}" ]] && return 0
  kubectl port-forward -n ate-system deploy/atenet-router \
    "${ENVOY_ADMIN_PORT}:9901" >/dev/null 2>&1 &
  ENVOY_PF_PID=$!
  local deadline=$((SECONDS + 60))
  while ((SECONDS < deadline)); do
    if curl -sf "localhost:${ENVOY_ADMIN_PORT}/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  die "envoy admin interface never became reachable on :${ENVOY_ADMIN_PORT}"
}

envoy_admin() { curl -s "localhost:${ENVOY_ADMIN_PORT}/$1"; }

# Envoy renders stats as "<name>: <value>", so the name must be anchored on the
# colon rather than end-of-line.
dfp_stats() {
  envoy_admin stats | grep -E \
    'dynamic_forward_proxy_cluster\.(upstream_rq_total|upstream_cx_total|upstream_rq_503|upstream_rq_pending_failure_eject|upstream_cx_connect_fail|upstream_cx_destroy_remote_with_active_rq): ' \
    || true
}

dfp_stat() {
  envoy_admin stats \
    | awk -F': ' -v k="dynamic_forward_proxy_cluster.$1" '$1 ~ (k "$") {print $2; exit}'
}

# ---------------------------------------------------------------------------
# Waiters
# ---------------------------------------------------------------------------

wait_for_workers() {
  local want="$1"
  local deadline=$((SECONDS + 900))
  local running stuck
  log "waiting for ${want} worker pods"
  while ((SECONDS < deadline)); do
    running=$(kubectl get pods -n benchmark-workloads --no-headers 2>/dev/null \
      | awk '$3 == "Running"' | wc -l | tr -d ' ')
    stuck=$(kubectl get pods -n benchmark-workloads --no-headers 2>/dev/null \
      | awk '$3 == "ImagePullBackOff" || $3 == "ErrImagePull"' | wc -l | tr -d ' ')

    if ((stuck > 0)); then
      kubectl get pods -n benchmark-workloads | grep -E 'ImagePullBackOff|ErrImagePull' | head -3
      die "${stuck} worker pods cannot pull their image. The node service account
       almost certainly lacks roles/artifactregistry.reader — re-run without
       --skip-iam, or run: go run ./tools/setup-gcp create iam"
    fi

    if ((running >= want)); then
      log "${running}/${want} worker pods Running"
      return 0
    fi
    sleep 5
  done
  die "timed out waiting for worker pods (${running}/${want} Running)"
}

# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

phase_iam() {
  log_phase "Reconcile GCP IAM bindings"
  # Grants the node service account artifactregistry.reader + storage.objectViewer
  # and atelet its bucket access. Idempotent: it logs and skips when the policy
  # already has them. Without this, freshly built images 403 on pull while
  # already-cached ones keep working, which looks like a flaky rollout.
  go run ./tools/setup-gcp create iam
}

phase_delete_actors() {
  log_phase "Delete actors"
  # Actors must go before their ActorTemplates or they are orphaned: the
  # template owns the golden actor, and nothing owns the ones boomer created.
  # Deletion requires STATUS_SUSPENDED, so resume-then-suspend anything running.
  local actors
  actors=$(list_deletable_actors) || true

  if [[ -z "${actors}" ]]; then
    log "no non-golden actors to delete"
    return 0
  fi

  while IFS=$'\t' read -r atespace actor_id; do
    [[ -z "${actor_id}" ]] && continue
    log "deleting ${atespace}/${actor_id}"
    prepare_actor_for_delete "${atespace}" "${actor_id}"
    kubectl ate delete actor "${actor_id}" -a "${atespace}" >/dev/null 2>&1 || true
  done <<<"${actors}"

  # Re-list rather than trusting the loop. Every delete above is best-effort,
  # and a wrong jq path here would otherwise read as "nothing to do" and let
  # the run continue onto templates, orphaning the actors it was meant to
  # remove.
  local leftover
  leftover=$(list_deletable_actors) || true
  [[ -z "${leftover}" ]] || die "actors still present after the delete pass:
$(printf '%s\n' "${leftover}" | head -5)"

  log "$(kubectl ate get actors -A -o json 2>/dev/null | jq '.actors | length') actors remain (golden only)"
}

# list_deletable_actors emits "<atespace>\t<name>" for every actor a teardown
# should remove. Golden actors are excluded: they are owned by their
# ActorTemplate and go away with it.
list_deletable_actors() {
  kubectl ate get actors -A -o json 2>/dev/null \
    | jq -r '.actors[]?
             | select(.metadata.atespace != "ate-golden")
             | [.metadata.atespace, .metadata.name]
             | @tsv'
}

prepare_actor_for_delete() {
  local atespace="$1" actor_id="$2"
  local deadline=$((SECONDS + 120))
  local status
  while ((SECONDS < deadline)); do
    status=$(kubectl ate get actor "${actor_id}" -a "${atespace}" -o json 2>/dev/null \
      | jq -r '.actors[0].status // empty') || return 0
    [[ -z "${status}" ]] && return 0
    case "${status}" in
      STATUS_SUSPENDED) return 0 ;;
      STATUS_PAUSED)  kubectl ate resume actor "${actor_id}" -a "${atespace}" -o json >/dev/null 2>&1 || true ;;
      STATUS_RUNNING) kubectl ate suspend actor "${actor_id}" -a "${atespace}" -o json >/dev/null 2>&1 || true ;;
      *) ;;  # a transition is in flight; wait it out
    esac
    sleep 2
  done
  log "warning: ${atespace}/${actor_id} never reached STATUS_SUSPENDED; deleting anyway"
}

phase_teardown() {
  log_phase "Tear down the benchmark stack and substrate"
  # --delete-all covers demos and ate-system but not the benchmark stack, so
  # that comes down separately and first.
  ./benchmarking/deploy_locust.sh --delete || log "warning: benchmark stack delete reported an error (already gone?)"
  kubectl ate delete atespace benchmark >/dev/null 2>&1 || true
  ./hack/install-ate.sh --delete-all || log "warning: --delete-all reported an error (already gone?)"

  local deadline=$((SECONDS + 300))
  while ((SECONDS < deadline)); do
    kubectl get ns ate-system >/dev/null 2>&1 || break
    sleep 5
  done
  log "namespaces: $(kubectl get ns ate-system benchmarking benchmark-workloads --no-headers 2>/dev/null | wc -l | tr -d ' ') of 3 still present"
}

phase_wipe_snapshots() {
  log_phase "Delete GCS snapshots"
  gcloud storage rm -r "gs://${BUCKET_NAME}/benchmark-workloads/" 2>/dev/null \
    || log "nothing to delete under gs://${BUCKET_NAME}/benchmark-workloads/"
}

phase_install_substrate() {
  log_phase "Install the substrate"
  ./hack/install-ate.sh --deploy-ate-system
  kubectl rollout status deploy/atenet-router -n ate-system --timeout=300s
  kubectl get pods -n ate-system
}

phase_deploy_benchmarks() {
  log_phase "Deploy the benchmark stack (worker_count=${WORKER_COUNT})"
  # docker push needs its own Artifact Registry credentials; ko authenticates
  # through the Google keychain and so never surfaces this.
  gcloud auth configure-docker us-docker.pkg.dev --quiet >/dev/null 2>&1 || true

  local args=(--deploy --worker-count "${WORKER_COUNT}")
  ((SKIP_BUILD)) && args+=(--skip-build)
  ./benchmarking/deploy_locust.sh "${args[@]}"

  wait_for_workers "${WORKER_COUNT}"
  kubectl rollout status deploy/locust -n benchmarking --timeout=300s
}

phase_verify() {
  log_phase "Verify the deployed Envoy config"
  start_envoy_forward

  local live
  live=$(envoy_admin config_dump | jq -r '
    .configs[] | select(.["@type"] | test("Clusters")) | .dynamic_active_clusters[]?
    | select(.cluster.name == "dynamic_forward_proxy_cluster")
    | (.cluster | has("typed_extension_protocol_options")) | tostring' | head -1)

  [[ -n "${live}" ]] || die "dynamic_forward_proxy_cluster is absent from Envoy's config.
       That is what a silent CDS NACK looks like — the pod stays 2/2 Running and
       every actor request 503s. Check the atenet container logs."

  log "source has fix: $( ((HAS_FIX)) && echo true || echo false ), live config has HttpProtocolOptions: ${live}"

  if ((HAS_FIX)) && [[ "${live}" != "true" ]]; then
    die "checkout contains the fix but Envoy is not running it — stale image?"
  fi
  if ((!HAS_FIX)) && [[ "${live}" != "false" ]]; then
    die "checkout omits the fix but Envoy is running it — stale image?"
  fi
  log "verified"
}

phase_run() {
  log_phase "Run the load test"
  start_envoy_forward

  log "resetting Envoy counters"
  curl -s -XPOST "localhost:${ENVOY_ADMIN_PORT}/reset_counters" >/dev/null

  mkdir -p "${RESULTS_DIR}"

  # Tee rather than redirect: the run takes minutes and the live locust output
  # is the only progress signal. Failure is not fatal here — the Envoy counters
  # collected next are the part that actually settles the question, and losing
  # them to a non-zero locust exit would be the wrong trade.
  kubectl exec -n benchmarking deploy/locust -c locust-master -- \
    python /app/runner.py \
      -f /app/tests/glutton.py \
      -t "${DURATION}" \
      -u "${USERS}" \
      --tag "${RUN_TAG}" \
      --name "${RUN_NAME}" \
      --dest /tmp/results \
      --min-wait-time "${MIN_WAIT}" \
      --max-wait-time "${MAX_WAIT}" \
      --trace-probability "${TRACE_PROBABILITY}" \
    2>&1 | tee "${RESULTS_DIR}/runner_output.txt" \
    || log "warning: runner exited non-zero; continuing to collect counters"
}

phase_collect() {
  log_phase "Collect results"
  start_envoy_forward

  echo
  echo "Envoy dynamic_forward_proxy_cluster counters:"
  dfp_stats | sed 's/^/    /'

  # Requests per connection is the counter that actually distinguishes the two
  # builds. ~1.9 means connections are being reused across actor lifetimes;
  # ~1.0 means max_requests_per_connection is in force.
  local rq cx
  rq=$(dfp_stat upstream_rq_total)
  cx=$(dfp_stat upstream_cx_total)
  if [[ -n "${rq:-}" && -n "${cx:-}" ]] && ((cx > 0)); then
    echo
    printf "    requests per connection: %.2f\n" "$(echo "${rq} / ${cx}" | bc -l)"
  fi

  mkdir -p "${RESULTS_DIR}"
  dfp_stats > "${RESULTS_DIR}/envoy_dfp_stats.txt" 2>/dev/null || true

  # `kubectl cp` cannot be used: the locust image is distroless, so there is no
  # tar (or shell) for it to drive. The base is distroless/python3 though, so
  # python's stdlib tarfile can stream the archive out over exec instead.
  if kubectl exec -n benchmarking deploy/locust -c locust-master -- \
       python -c 'import sys,tarfile; t=tarfile.open(fileobj=sys.stdout.buffer,mode="w|"); t.add("/tmp/results",arcname="results"); t.close()' \
       2>/dev/null | tar xf - -C "${RESULTS_DIR}" 2>/dev/null; then
    echo
    log "results copied to ${RESULTS_DIR}"
  else
    log "warning: could not extract /tmp/results; the run log is still in ${RESULTS_DIR}/runner_output.txt"
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

((SKIP_IAM))      || phase_iam
((SKIP_TEARDOWN)) || { phase_delete_actors; phase_teardown; }
((WIPE_SNAPSHOTS)) && phase_wipe_snapshots
phase_install_substrate
phase_deploy_benchmarks
phase_verify

if ((SKIP_RUN)); then
  log_phase "Done (--skip-run)"
  echo "    Environment is up and verified. To run the test:"
  echo "      ./benchmarking/clean_redeploy.sh --skip-iam --skip-teardown --skip-build"
  exit 0
fi

phase_run
phase_collect

log_phase "Done"
echo "    branch ${BRANCH} @ ${SHA}, connection-reuse fix $( ((HAS_FIX)) && echo present || echo absent )"
echo "    results: ${RESULTS_DIR}"
