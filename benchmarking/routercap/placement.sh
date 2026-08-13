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

# The other half of pools.sh: puts ate-system on the pools that exist, so a
# tainted node pool is something the measurement uses rather than something it
# pays for. Idempotent — every step is a patch, and re-applying the same patch
# is a no-op.
#
# Node pinning is what isolates the measurement. A CPU limit is CFS quota, not
# core pinning, and does not partition run-queue delay, L3, memory bandwidth,
# the NIC or conntrack — all per node. Giving the router a node to itself
# partitions all of them at once.
#
# Run this AFTER substrate is installed: it patches workloads that
# hack/install-ate.sh creates. Pools named here that do not exist are not an
# error, they simply pin nothing — the orchestrator creates two of the four.
#
# The benchmark WorkerPool is deliberately not handled here. Its pinning patch
# also carries the replica count, so it belongs with whoever deployed the
# workloads and knows what that count is.

# shellcheck source=benchmarking/routercap/common.sh
source "$(git rev-parse --show-toplevel)/benchmarking/routercap/common.sh"

POOLS="${RC_POOL_ROUTER},${RC_POOL_SYSTEM},${RC_POOL_WORKERS},${RC_POOL_LOADGEN}"

usage() {
  cat <<EOF
Usage: $0 [options]

  --pools LIST  Comma-separated pools whose placement to apply
                (default: ${POOLS}). Valid: ${RC_POOL_ROUTER}, ${RC_POOL_SYSTEM}, ${RC_POOL_WORKERS}, ${RC_POOL_LOADGEN}.
  -h, --help    This.

Only the ${RC_POOL_ROUTER} and ${RC_POOL_SYSTEM} pools have anything pinned to them. Naming
${RC_POOL_WORKERS} or ${RC_POOL_LOADGEN} adds nothing but atelet's toleration for their taint,
which is the reason to name them anyway.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pools) shift; POOLS="$1" ;;
    --pools=*) POOLS="${1#*=}" ;;
    -h|--help) usage; exit 0 ;;
    *) rc::die "unknown option: $1" ;;
  esac
  shift
done

rc::need kubectl
rc::env
rc::assert_cluster

IFS=',' read -r -a pool_list <<< "${POOLS}"
for pool in "${pool_list[@]}"; do
  case "${pool}" in
    ""|"${RC_POOL_ROUTER}"|"${RC_POOL_SYSTEM}"|"${RC_POOL_WORKERS}"|"${RC_POOL_LOADGEN}") ;;
    *) rc::die "unknown pool '${pool}'; valid: ${RC_POOL_ROUTER}, ${RC_POOL_SYSTEM}, ${RC_POOL_WORKERS}, ${RC_POOL_LOADGEN}" ;;
  esac
done

in_pools() {
  local want="$1" p
  for p in "${pool_list[@]}"; do
    [[ "${p}" == "${want}" ]] && return 0
  done
  return 1
}

# --- atelet ------------------------------------------------------------------
#
# atelet is a DaemonSet and nothing under manifests/ate-install declares any
# tolerations, so without this it gets no tainted node and no actor scheduled
# there ever starts. It must run everywhere, so it tolerates every pool asked
# for.
rc::step "letting atelet onto the tainted pools: ${POOLS}"
tolerations=""
for pool in "${pool_list[@]}"; do
  [[ -n "${pool}" ]] || continue
  # No "effect" field, deliberately: an empty effect matches every effect, so
  # one toleration covers both the hard-tainted pools and the soft-tainted
  # system pool. Naming NoSchedule here would silently fail to tolerate
  # system's PreferNoSchedule.
  tolerations+="{\"key\":\"${RC_ROLE_KEY}\",\"operator\":\"Equal\",\"value\":\"${pool}\"},"
done
rc::kubectl -n "${RC_ROUTER_NS}" patch daemonset atelet --type=strategic \
  -p "{\"spec\":{\"template\":{\"spec\":{\"tolerations\":[${tolerations%,}]}}}}" >/dev/null

# --- ate-system --------------------------------------------------------------
#
# Applied as patches because manifests/ate-install is the product's and this
# placement is the experiment's.

if in_pools "${RC_POOL_ROUTER}"; then
  rc::step "pinning atenet-router to the ${RC_POOL_ROUTER} pool"
  rc::pin_workload deployment "${RC_ROUTER_NS}" atenet-router "${RC_POOL_ROUTER}"
fi

if in_pools "${RC_POOL_SYSTEM}"; then
  rc::step "pinning the rest of ate-system to the ${RC_POOL_SYSTEM} pool"
  rc::pin_workload deployment "${RC_ROUTER_NS}" ate-api-server "${RC_POOL_SYSTEM}"
  rc::pin_workload deployment "${RC_ROUTER_NS}" ate-controller "${RC_POOL_SYSTEM}"
  rc::pin_workload deployment "${RC_ROUTER_NS}" dns "${RC_POOL_SYSTEM}"
  rc::pin_workload statefulset "${RC_ROUTER_NS}" valkey-cluster "${RC_POOL_SYSTEM}"
  rc::pin_workload deployment podcertificate-controller-system podcertificate-controller "${RC_POOL_SYSTEM}"
fi
