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

# Creates the tainted node pools a routercap measurement needs, and nothing
# else. Idempotent: a pool that already exists has its taint reconciled rather
# than being recreated.
#
# Two callers, which is why this is a script and not a block inside
# provision.sh:
#
#   * provision.sh builds a dedicated cluster and wants all four pools.
#   * benchmarking/automation's orchestrator runs on a shared test cluster it
#     did not create, and wants only router and loadgen — the two a
#     measurement cannot be trusted without. Its other pods stay on whatever
#     pool the cluster already has.
#
# Pools are created in the order given. Put router first: it is the one pool
# that genuinely needs the large machine type, so a zone shortage surfaces
# before the others have been paid for.
#
# Creating a pool does NOT make it effective on its own — a tainted pool with
# nothing pinned to it is an empty node. placement.sh is the other half.

# shellcheck source=benchmarking/routercap/common.sh
source "$(git rev-parse --show-toplevel)/benchmarking/routercap/common.sh"

POOLS="${RC_POOL_ROUTER},${RC_POOL_SYSTEM},${RC_POOL_WORKERS},${RC_POOL_LOADGEN}"
WORKER_NODES="${RC_WORKER_NODES}"

usage() {
  cat <<EOF
Usage: $0 [options]

  --pools LIST      Comma-separated pools to ensure, in creation order
                    (default: ${POOLS}). Valid: ${RC_POOL_ROUTER}, ${RC_POOL_SYSTEM}, ${RC_POOL_WORKERS}, ${RC_POOL_LOADGEN}.
  --worker-nodes N  Nodes in the ${RC_POOL_WORKERS} pool (default: ${WORKER_NODES}).
  -h, --help        This.

The cluster comes from ROUTERCAP_CLUSTER / ROUTERCAP_CLUSTER_LOCATION and the
project from PROJECT_ID, so a caller driving a cluster this harness did not
create sets those first.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pools) shift; POOLS="$1" ;;
    --pools=*) POOLS="${1#*=}" ;;
    --worker-nodes) shift; WORKER_NODES="$1" ;;
    --worker-nodes=*) WORKER_NODES="${1#*=}" ;;
    -h|--help) usage; exit 0 ;;
    *) rc::die "unknown option: $1" ;;
  esac
  shift
done

rc::need gcloud
rc::env

# Machine type, node count and taint strength for each pool.
#
# The system pool's taint is soft; the other three are hard. A hard taint on
# system deadlocks a fresh install: ate-system pods carry no toleration until
# placement.sh's pinning patch lands, so they need somewhere to schedule first.
# router, workers and loadgen stay NoSchedule — an uninvited pod on those nodes
# is exactly the contamination this harness exists to exclude.
pool_spec() {
  case "$1" in
    "${RC_POOL_ROUTER}")  echo "${RC_MACHINE_TYPE} 1 ${RC_TAINT_HARD}" ;;
    "${RC_POOL_SYSTEM}")  echo "${RC_SYSTEM_MACHINE_TYPE} 1 ${RC_TAINT_SYSTEM}" ;;
    "${RC_POOL_WORKERS}") echo "${RC_MACHINE_TYPE} ${WORKER_NODES} ${RC_TAINT_HARD}" ;;
    "${RC_POOL_LOADGEN}") echo "${RC_MACHINE_TYPE} 1 ${RC_TAINT_HARD}" ;;
    *) rc::die "unknown pool '$1'; valid: ${RC_POOL_ROUTER}, ${RC_POOL_SYSTEM}, ${RC_POOL_WORKERS}, ${RC_POOL_LOADGEN}" ;;
  esac
}

# gcloud reports taint effects in the API's enum spelling, not the one you
# create them with, so a comparison has to translate.
taint_enum() {
  case "$1" in
    NoSchedule)       echo NO_SCHEDULE ;;
    PreferNoSchedule) echo PREFER_NO_SCHEDULE ;;
    NoExecute)        echo NO_EXECUTE ;;
    *)                rc::die "unknown taint effect: $1" ;;
  esac
}

ensure_pool() {
  local name="$1" machine="$2" nodes="$3" role="$4" effect="$5"
  if gcloud container node-pools describe "${name}" \
      --cluster="${RC_CLUSTER}" --location="${RC_LOCATION}" \
      --project="${PROJECT_ID}" >/dev/null 2>&1; then
    # Reconcile the taint rather than just reporting the pool present: a pool
    # created by an older revision of this script carries that revision's
    # taint, and the system pool's effect is the difference between an install
    # that converges and one that deadlocks.
    local have want
    have="$(gcloud container node-pools describe "${name}" \
      --cluster="${RC_CLUSTER}" --location="${RC_LOCATION}" --project="${PROJECT_ID}" \
      --format="value(config.taints[0].effect)" 2>/dev/null || true)"
    want="$(taint_enum "${effect}")"
    if [[ "${have}" != "${want}" ]]; then
      rc::step "node pool ${name} exists with taint effect ${have:-<none>}; updating to ${want}"
      # --quiet because the update prompts to confirm replacing the pool's
      # taints, and a provision run has to be unattended. Safe: the taints
      # being replaced are the ones this same function wrote.
      gcloud container node-pools update "${name}" --quiet \
        --cluster="${RC_CLUSTER}" --location="${RC_LOCATION}" --project="${PROJECT_ID}" \
        --node-taints="${RC_ROLE_KEY}=${role}:${effect}"
    else
      rc::step "node pool ${name} already exists"
    fi
    return
  fi
  rc::step "creating node pool ${name} (${nodes} x ${machine})"
  gcloud container node-pools create "${name}" \
    --cluster="${RC_CLUSTER}" \
    --location="${RC_LOCATION}" \
    --project="${PROJECT_ID}" \
    --machine-type="${machine}" \
    --num-nodes="${nodes}" \
    --node-labels="${RC_ROLE_KEY}=${role}" \
    --node-taints="${RC_ROLE_KEY}=${role}:${effect}"
}

IFS=',' read -r -a pool_list <<< "${POOLS}"
for pool in "${pool_list[@]}"; do
  [[ -n "${pool}" ]] || continue
  # The role is the pool name: the label attracts the pods that belong there,
  # the taint repels everything else, and both carry this same value.
  read -r machine nodes effect <<< "$(pool_spec "${pool}")"
  ensure_pool "${pool}" "${machine}" "${nodes}" "${pool}" "${effect}"
done
