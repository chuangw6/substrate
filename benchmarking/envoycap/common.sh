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

# Shared setup for the Envoy capacity measurement scripts. Sourced by
# provision.sh and run.sh; not executable on its own.
#
# The single most important thing this file does is keep the measurement off
# every other cluster. It points KUBECONFIG at a file that holds exactly one
# context, refuses to run against a name on the protected list, and never lets
# a repo-local .ate-dev-env.sh redirect the target — so a developer or another
# agent working on a different cluster in the same project is unaffected, and
# the shared ~/.kube/config is never rewritten.

# --- Identity of this benchmark's cluster ------------------------------------
# Overridable only through the ENVOYCAP_* env vars, so nothing that merely
# happens to be exported in the shell can retarget a run.
ENVOYCAP_CLUSTER_NAME="${ENVOYCAP_CLUSTER_NAME:-substrate-envoycap}"
ENVOYCAP_CLUSTER_LOCATION="${ENVOYCAP_CLUSTER_LOCATION:-us-central1-c}"

# One machine type for every pool, and one node per pool. The first run of this
# benchmark used 8-vCPU nodes and produced a latency curve that was not a
# function of offered rate; the leading suspect was contention, and no amount
# of re-running a contended cluster settles that. So the shape here is three
# identical, deliberately oversized nodes -- one per role, nothing shared, no
# node-type difference between roles to explain a result away with. Whatever
# the ladder finds now is a property of the software, not of who got a core.
#
# It is not subtle and it is not cheap. That is the point: CPU has to be off
# the table before "capacity" means anything.
ENVOYCAP_MACHINE_TYPE="${ENVOYCAP_MACHINE_TYPE:-c3-standard-88}"

# The three pools. substrate-node-pool is the one setup-gcp creates (at two
# nodes, which is why provision.sh has to revisit it); the other two this
# tooling creates itself. All three are managed identically by ensure_node_pool
# in provision.sh, which replaces a pool whose machine type has drifted --
# machine type is immutable once a pool exists, so changing it means delete and
# recreate.
#
# The loadgen taint is the whole reason this benchmark wants its own cluster:
# without it the scheduler drifts zero-request control-plane pods onto the
# idle loadgen node, where they then compete with the load generator.
ENVOYCAP_SYSTEM_POOL="substrate-node-pool"
ENVOYCAP_SYSTEM_NODES="${ENVOYCAP_SYSTEM_NODES:-1}"
ENVOYCAP_WORKER_POOL="workers"
ENVOYCAP_WORKER_NODES="${ENVOYCAP_WORKER_NODES:-1}"
ENVOYCAP_LOADGEN_POOL="loadgen"
ENVOYCAP_LOADGEN_NODES="${ENVOYCAP_LOADGEN_NODES:-1}"
ENVOYCAP_LOADGEN_TAINT="ate.dev/dedicated=loadgen:NoSchedule"

# One actor per worker pod is a hard cap: Worker.assignment is singular.
ENVOYCAP_WORKER_REPLICAS="${ENVOYCAP_WORKER_REPLICAS:-40}"

# The label the WorkerPool controller stamps on the pods it owns
# (cmd/atecontroller/internal/controllers/workerpool_apply.go). Defined once
# because provision.sh waits on it and run.sh preflights on it, and a selector
# that matches nothing reads exactly like a pool that never came up.
WORKER_POD_SELECTOR="ate.dev/worker-pool=benchmark-ateom"

# Exported because the callers that read them are separate files. The linter
# checks one file at a time and so cannot see the use.
export ENVOYCAP_CLUSTER_NAME ENVOYCAP_CLUSTER_LOCATION ENVOYCAP_MACHINE_TYPE
export ENVOYCAP_SYSTEM_POOL ENVOYCAP_SYSTEM_NODES
export ENVOYCAP_WORKER_POOL ENVOYCAP_WORKER_NODES
export ENVOYCAP_LOADGEN_POOL ENVOYCAP_LOADGEN_NODES ENVOYCAP_LOADGEN_TAINT
export ENVOYCAP_WORKER_REPLICAS WORKER_POD_SELECTOR

# Clusters this tooling must never touch. substrate-bench is the shared
# benchmarking cluster; substrate-poc is the default in the dev-env example, so
# a missing env var must not silently land there either.
ENVOYCAP_PROTECTED_CLUSTERS=("substrate-bench" "substrate-poc")

envoycap::die() {
  echo "Error: $*" >&2
  exit 1
}

envoycap::log() {
  echo "==> $*" >&2
}

# envoycap::resolve_project fills in PROJECT_ID, PROJECT_NUMBER and BUCKET_NAME
# from gcloud when they are not already set. The bucket is deliberately
# per-cluster: sharing a snapshot location with another cluster would let two
# clusters write golden snapshots for the same template into the same prefix.
envoycap::resolve_project() {
  PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
  [[ -n "${PROJECT_ID}" && "${PROJECT_ID}" != "(unset)" ]] ||
    envoycap::die "PROJECT_ID is not set and gcloud has no default project"

  if [[ -z "${PROJECT_NUMBER:-}" ]]; then
    PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
  fi
  [[ -n "${PROJECT_NUMBER}" ]] || envoycap::die "could not resolve PROJECT_NUMBER for ${PROJECT_ID}"

  BUCKET_NAME="${BUCKET_NAME:-snapshot-${ENVOYCAP_CLUSTER_NAME}-${PROJECT_ID}}"
  KO_DOCKER_REPO="gcr.io/${PROJECT_ID}/ate-images"

  export PROJECT_ID PROJECT_NUMBER BUCKET_NAME KO_DOCKER_REPO
}

# envoycap::assert_not_protected refuses to act on a cluster this tooling has
# no business creating, patching or loading.
envoycap::assert_not_protected() {
  local name="$1"
  for protected in "${ENVOYCAP_PROTECTED_CLUSTERS[@]}"; do
    if [[ "${name}" == "${protected}" ]]; then
      envoycap::die "refusing to operate on protected cluster '${name}'. This tooling provisions and loads its own cluster; point ENVOYCAP_CLUSTER_NAME somewhere else."
    fi
  done
}

# envoycap::use_cluster points this shell at the benchmark cluster and nothing
# else.
#
# KUBECONFIG is a dedicated file so `gcloud container clusters get-credentials`
# never rewrites ~/.kube/config or moves anyone else's current-context. The
# file ends up holding exactly one context, so even a command that ignores
# KUBECTL_CONTEXT cannot reach another cluster.
#
# Pass --require-existing to fail rather than fetch credentials, for callers
# that must not run before provisioning.
envoycap::use_cluster() {
  local require_existing=0
  [[ "${1:-}" == "--require-existing" ]] && require_existing=1

  envoycap::assert_not_protected "${ENVOYCAP_CLUSTER_NAME}"

  KUBECONFIG="${HOME}/.kube/${ENVOYCAP_CLUSTER_NAME}.config"
  KUBECTL_CONTEXT="gke_${PROJECT_ID}_${ENVOYCAP_CLUSTER_LOCATION}_${ENVOYCAP_CLUSTER_NAME}"
  mkdir -p "$(dirname "${KUBECONFIG}")"
  export KUBECONFIG KUBECTL_CONTEXT

  if ! kubectl config get-contexts "${KUBECTL_CONTEXT}" >/dev/null 2>&1; then
    if [[ "${require_existing}" -eq 1 ]]; then
      envoycap::die "no credentials for ${ENVOYCAP_CLUSTER_NAME} in ${KUBECONFIG}. Run benchmarking/envoycap/provision.sh first."
    fi
    envoycap::log "fetching credentials for ${ENVOYCAP_CLUSTER_NAME} into ${KUBECONFIG}"
    gcloud container clusters get-credentials "${ENVOYCAP_CLUSTER_NAME}" \
      --location "${ENVOYCAP_CLUSTER_LOCATION}" \
      --project "${PROJECT_ID}"
  fi

  kubectl config use-context "${KUBECTL_CONTEXT}" >/dev/null

  # Belt and braces: read the context back rather than trusting that the two
  # lines above did what they say.
  local actual
  actual="$(kubectl config current-context)"
  [[ "${actual}" == "${KUBECTL_CONTEXT}" ]] ||
    envoycap::die "kube context is '${actual}', expected '${KUBECTL_CONTEXT}'"

  local server
  server="$(kubectl config view --minify -o jsonpath='{.clusters[0].name}')"
  [[ "${server}" == *"${ENVOYCAP_CLUSTER_NAME}"* ]] ||
    envoycap::die "context '${actual}' points at cluster '${server}', which is not ${ENVOYCAP_CLUSTER_NAME}"

  envoycap::log "using cluster ${ENVOYCAP_CLUSTER_NAME} (KUBECONFIG=${KUBECONFIG})"
}

# envoycap::ko runs ko with the repo's ldflags, exactly as hack/install-ate.sh
# does, so images built here are stamped the same way as a normal install.
envoycap::ko() {
  local ldflags=()
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -n "${line}" ]] && ldflags+=("--ldflags=${line}")
  done < <(make ldflags)
  ./hack/run-tool.sh ko "$@" "${ldflags[@]}"
}
