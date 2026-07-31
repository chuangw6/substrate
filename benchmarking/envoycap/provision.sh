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

# Provisions the dedicated cluster the Envoy capacity measurement runs on, and
# nothing else. Idempotent: safe to re-run, and re-running is how you repair a
# half-provisioned cluster.
#
# Three node pools, one node each, all the same machine type, because where
# things land is part of the measurement:
#
#   substrate-node-pool  1 node   ate-system control plane, pinned here
#   workers              1 node   the WorkerPool's ateom pods
#   loadgen              1 node   tainted; only the generator tolerates it
#
# Same type and size for all three on purpose. The first run of this benchmark
# put ate-system on shared 8-vCPU nodes and produced a latency curve that was
# not a function of offered rate. Contention was the leading suspect and could
# not be ruled out from the data. Three identical oversized nodes remove both
# CPU pressure and any node-shape difference between roles, so a result cannot
# be explained away as "that role happened to be on a smaller box".
#
# The loadgen taint is the reason this benchmark does not reuse an existing
# cluster. Without it the scheduler puts zero-request BestEffort control-plane
# pods -- ate-api-server, dns, valkey -- on whichever node is emptiest, which
# is the idle load-generator node, and the generator then competes for CPU with
# components sitting in the per-request hot path.
#
# Nothing under manifests/ate-install is edited. The router is measured as
# shipped; its placement is set here with a patch at provision time, which is
# recorded in the run JSON.
#
# Usage:
#   benchmarking/envoycap/provision.sh
#
# Environment (all optional, all defaulted in common.sh):
#   PROJECT_ID, PROJECT_NUMBER, BUCKET_NAME
#   ENVOYCAP_CLUSTER_NAME, ENVOYCAP_CLUSTER_LOCATION, ENVOYCAP_MACHINE_TYPE
#   ENVOYCAP_SYSTEM_NODES, ENVOYCAP_WORKER_NODES, ENVOYCAP_WORKER_REPLICAS

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"
# shellcheck source=benchmarking/envoycap/common.sh
source "${ROOT}/benchmarking/envoycap/common.sh"

# ate-system components that sit in the per-request hot path. They are pinned
# to substrate-node-pool so they neither drift onto the load generator's node
# nor onto a worker node whose CPU we are trying to attribute to the workload.
# atelet is deliberately absent: it is a DaemonSet and must run everywhere.
SYSTEM_DEPLOYMENTS=(ate-api-server ate-controller dns atenet-router)
SYSTEM_STATEFULSETS=(valkey-cluster)

main() {
  envoycap::resolve_project
  envoycap::assert_not_protected "${ENVOYCAP_CLUSTER_NAME}"

  preflight
  enable_extra_apis
  bootstrap_cluster
  # Deliberately no --require-existing: bootstrap_cluster has just created the
  # cluster, so this is the call that fetches credentials for it.
  # shellcheck disable=SC2119
  envoycap::use_cluster
  # Pools are reconciled before ate-system is installed, and a pool being
  # replaced is deleted before its replacement is created. Both matter: the
  # peak vCPU this script holds is then the final shape rather than the old
  # shape plus the new one, which is the difference between fitting in quota
  # and failing a node-pool create halfway through provisioning.
  ensure_node_pool "${ENVOYCAP_SYSTEM_POOL}" "${ENVOYCAP_SYSTEM_NODES}" ""
  ensure_node_pool "${ENVOYCAP_WORKER_POOL}" "${ENVOYCAP_WORKER_NODES}" ""
  ensure_node_pool "${ENVOYCAP_LOADGEN_POOL}" "${ENVOYCAP_LOADGEN_NODES}" "${ENVOYCAP_LOADGEN_TAINT}"
  reset_valkey
  install_ate_system
  pin_control_plane
  deploy_workloads
  wait_for_workers
  wait_for_templates
  summary
}

preflight() {
  envoycap::log "preflight"

  command -v gcloud >/dev/null || envoycap::die "gcloud is not on PATH"
  command -v kubectl >/dev/null || envoycap::die "kubectl is not on PATH"

  # CLUSTER_VERSION in hack/ate-dev-env.sh.example pins a version that is no
  # longer offered in every zone, and an unofferable version fails the create
  # ~15 minutes in. Leave it unset and take the zone's default.
  if [[ -n "${CLUSTER_VERSION:-}" ]]; then
    if ! gcloud container get-server-config \
      --location "${ENVOYCAP_CLUSTER_LOCATION}" --project "${PROJECT_ID}" \
      --format='value(validMasterVersions)' | tr ';' '\n' | grep -qx "${CLUSTER_VERSION}"; then
      envoycap::die "CLUSTER_VERSION=${CLUSTER_VERSION} is not offered in ${ENVOYCAP_CLUSTER_LOCATION}. Unset it to take the zone default."
    fi
  fi

  # Three c3-standard-88 nodes is 264 vCPU on top of whatever else the project
  # is running, and quota is per region, not per cluster. Failing here beats
  # failing halfway through a node-pool create -- especially now that pools are
  # deleted before their replacements are created, where a quota failure would
  # leave the cluster with fewer pools than it started with.
  #
  # The per-node vCPU count is read from the machine type rather than assumed:
  # this script used to hard-code 8, which silently under-counted by 10x the
  # moment the machine type changed.
  local region="${ENVOYCAP_CLUSTER_LOCATION%-*}"
  local vcpu
  vcpu="$(gcloud compute machine-types describe "${ENVOYCAP_MACHINE_TYPE}" \
    --zone "${ENVOYCAP_CLUSTER_LOCATION}" --project "${PROJECT_ID}" \
    --format='value(guestCpus)' 2>/dev/null)" ||
    envoycap::die "machine type ${ENVOYCAP_MACHINE_TYPE} is not offered in ${ENVOYCAP_CLUSTER_LOCATION}"
  [[ -n "${vcpu}" ]] || envoycap::die "could not read guestCpus for ${ENVOYCAP_MACHINE_TYPE}"

  local want=$(((ENVOYCAP_SYSTEM_NODES + ENVOYCAP_WORKER_NODES + ENVOYCAP_LOADGEN_NODES) * vcpu))
  local usage limit
  # gcloud's .filter() projection transform is rejected by some gcloud versions,
  # and the failure is silent here -- it would read back as "no quota data" and
  # skip the check. Flatten the quota list and pick the row with awk instead,
  # which every version understands. awk prints a zero row when the metric is
  # absent so the read below always has something to consume.
  read -r usage limit < <(
    gcloud compute regions describe "${region}" --project "${PROJECT_ID}" \
      --flatten='quotas[]' \
      --format='value[separator=" "](quotas.metric,quotas.usage,quotas.limit)' 2>/dev/null |
      awk '$1 == "C3_CPUS" { print $2, $3; found = 1 } END { if (!found) print "0 0" }'
  )
  if [[ -n "${limit}" && "${limit}" != "0" ]]; then
    # Reported usage already includes whatever this cluster is running now, and
    # every one of those nodes is released before its replacement is created.
    # Without adding them back, re-running this script against an
    # already-correct cluster would count its own nodes twice and abort.
    local reclaim
    reclaim="$(cluster_vcpu)"
    # Bash cannot compare the floats gcloud emits; strip the fraction.
    local have=$((${limit%%.*} - ${usage%%.*} + reclaim))
    envoycap::log "C3_CPUS in ${region}: ${usage%%.*} used of ${limit%%.*} (${reclaim} of it this cluster's, released and rebuilt), this cluster wants ${want}"
    ((have >= want)) || envoycap::die "not enough C3_CPUS quota in ${region}: need ${want}, have ${have}. Raise the C3_CPUS quota for ${region} to at least $((${usage%%.*} - reclaim + want))."
  fi
}

# cluster_vcpu totals the vCPU this cluster's node pools currently hold, or 0
# when the cluster does not exist yet.
cluster_vcpu() {
  local pools total=0 name count type vcpu
  pools="$(gcloud container node-pools list --cluster "${ENVOYCAP_CLUSTER_NAME}" \
    --location "${ENVOYCAP_CLUSTER_LOCATION}" --project "${PROJECT_ID}" \
    --format='value(name,initialNodeCount,config.machineType)' 2>/dev/null)" || true
  while read -r name count type; do
    [[ -n "${name}" ]] || continue
    vcpu="$(gcloud compute machine-types describe "${type}" \
      --zone "${ENVOYCAP_CLUSTER_LOCATION}" --project "${PROJECT_ID}" \
      --format='value(guestCpus)' 2>/dev/null)" || continue
    total=$((total + count * vcpu))
  done <<<"${pools}"
  echo "${total}"
}

enable_extra_apis() {
  # setup-gcp enables container/storage/monitoring/etc. but not the registry
  # APIs, and KO_DOCKER_REPO is gcr.io/... which is served by Artifact Registry.
  envoycap::log "enabling registry APIs"
  gcloud services enable artifactregistry.googleapis.com containerregistry.googleapis.com \
    --project "${PROJECT_ID}"
}

bootstrap_cluster() {
  envoycap::log "bootstrapping cluster ${ENVOYCAP_CLUSTER_NAME} (this takes ~15 min on first run)"
  # Every value is passed explicitly. setup-gcp reads CLUSTER_NAME from the
  # environment when the flag is absent, and it deletes and recreates a cluster
  # whose network attributes do not match -- so the name it sees must never be
  # inherited from an ambient env var.
  go run ./tools/setup-gcp bootstrap \
    --project-id "${PROJECT_ID}" \
    --project-number "${PROJECT_NUMBER}" \
    --cluster-name "${ENVOYCAP_CLUSTER_NAME}" \
    --cluster-location "${ENVOYCAP_CLUSTER_LOCATION}" \
    --cluster-version "" \
    --machine-type "${ENVOYCAP_MACHINE_TYPE}" \
    --bucket-name "${BUCKET_NAME}"
}

# ensure_node_pool NAME NODES [TAINT] brings one pool to the wanted machine
# type and size, and is the reason this script is safe to re-run.
#
# A pool's machine type is immutable once it exists, so changing it means
# delete and recreate -- there is no resize that will do it. The delete happens
# first so the region never holds the old shape and the new one at the same
# time, which is what keeps a re-provision inside the C3 quota checked in
# preflight. Node count alone is a plain resize.
#
# setup-gcp always creates substrate-node-pool at 2 nodes and never revisits
# it, so on a fresh cluster this is also what shrinks the system pool to one.
ensure_node_pool() {
  local name="$1" nodes="$2" taint="$3"
  local current_nodes current_type

  read -r current_nodes current_type < <(
    gcloud container node-pools describe "${name}" \
      --cluster "${ENVOYCAP_CLUSTER_NAME}" --location "${ENVOYCAP_CLUSTER_LOCATION}" \
      --project "${PROJECT_ID}" \
      --format='value(initialNodeCount,config.machineType)' 2>/dev/null || echo ""
  )

  if [[ -n "${current_type}" ]]; then
    if [[ "${current_type}" == "${ENVOYCAP_MACHINE_TYPE}" && "${current_nodes}" == "${nodes}" ]]; then
      envoycap::log "node pool ${name} is already ${nodes} x ${ENVOYCAP_MACHINE_TYPE}"
      return
    fi
    if [[ "${current_type}" == "${ENVOYCAP_MACHINE_TYPE}" ]]; then
      envoycap::log "resizing ${name} from ${current_nodes} to ${nodes} nodes"
      gcloud container clusters resize "${ENVOYCAP_CLUSTER_NAME}" \
        --node-pool "${name}" --num-nodes "${nodes}" \
        --location "${ENVOYCAP_CLUSTER_LOCATION}" --project "${PROJECT_ID}" --quiet
      return
    fi
    envoycap::log "replacing ${name}: ${current_nodes} x ${current_type} -> ${nodes} x ${ENVOYCAP_MACHINE_TYPE}"
    gcloud container node-pools delete "${name}" \
      --cluster "${ENVOYCAP_CLUSTER_NAME}" --location "${ENVOYCAP_CLUSTER_LOCATION}" \
      --project "${PROJECT_ID}" --quiet
  fi

  envoycap::log "creating node pool ${name} (${nodes} x ${ENVOYCAP_MACHINE_TYPE}${taint:+, tainted ${taint}})"
  # No taint on the worker pool: atelet is a DaemonSet with no tolerations in
  # the shipped manifest, and it has to run on every node that hosts workers.
  gcloud container node-pools create "${name}" \
    --cluster "${ENVOYCAP_CLUSTER_NAME}" \
    --location "${ENVOYCAP_CLUSTER_LOCATION}" \
    --project "${PROJECT_ID}" \
    --machine-type "${ENVOYCAP_MACHINE_TYPE}" \
    --num-nodes "${nodes}" \
    ${taint:+--node-taints "${taint}"}
}

# reset_valkey throws away the Valkey cluster's persisted membership so
# install_ate_system rebuilds it from scratch.
#
# Valkey records its peers in nodes.conf by pod IP, on the PVC. Replacing the
# system node pool moves all six pods at once, so every node comes back with a
# nodes.conf full of addresses that no longer exist and no surviving peer to
# gossip the new ones from. The cluster reports cluster_state:fail with all
# 16384 slots pfail, ate-api-server crash-loops on "CLUSTERDOWN", and nothing
# times out its way back to health. Only a wipe fixes it.
#
# Done unconditionally rather than only when the state looks broken: this is a
# benchmark cluster whose Valkey holds nothing worth keeping, and a conditional
# repair path is a path that only ever runs when something has already gone
# wrong -- which is to say, untested. On a fresh cluster there is nothing to
# delete and this is a no-op.
reset_valkey() {
  if ! kubectl get namespace ate-system >/dev/null 2>&1; then
    return
  fi
  if ! kubectl -n ate-system get statefulset valkey-cluster >/dev/null 2>&1; then
    return
  fi

  envoycap::log "resetting Valkey cluster state (pod IPs changed with the node pools)"
  # The init job is Complete and immutable; without deleting it, re-applying the
  # manifest leaves the old one in place and the fresh cluster is never formed.
  kubectl -n ate-system delete job valkey-cluster-init --ignore-not-found --wait=true
  kubectl -n ate-system delete statefulset valkey-cluster --ignore-not-found --wait=true
  # The PVCs are what actually hold the stale nodes.conf. They outlive the
  # StatefulSet by design, so deleting the StatefulSet alone changes nothing.
  kubectl -n ate-system delete pvc -l app=valkey-cluster --ignore-not-found --wait=true
}

install_ate_system() {
  envoycap::log "installing ate-system"
  # NO_DEV_ENV keeps install-ate.sh from sourcing a repo-local .ate-dev-env.sh,
  # which would otherwise be free to override CLUSTER_NAME and point this
  # install at somebody else's cluster.
  NO_DEV_ENV=1 \
  PROJECT_ID="${PROJECT_ID}" \
  PROJECT_NUMBER="${PROJECT_NUMBER}" \
  BUCKET_NAME="${BUCKET_NAME}" \
  KO_DOCKER_REPO="${KO_DOCKER_REPO}" \
  KO_DEFAULTPLATFORMS="${KO_DEFAULTPLATFORMS:-linux/amd64}" \
  CLUSTER_NAME="${ENVOYCAP_CLUSTER_NAME}" \
  CLUSTER_LOCATION="${ENVOYCAP_CLUSTER_LOCATION}" \
  KUBECTL_CONTEXT="${KUBECTL_CONTEXT}" \
  KUBECONFIG="${KUBECONFIG}" \
    ./hack/install-ate.sh --deploy-ate-system
}

pin_control_plane() {
  envoycap::log "pinning ate-system control plane to ${ENVOYCAP_SYSTEM_POOL}"
  local patch
  patch="$(printf '{"spec":{"template":{"spec":{"nodeSelector":{"cloud.google.com/gke-nodepool":"%s"}}}}}' "${ENVOYCAP_SYSTEM_POOL}")"

  for name in "${SYSTEM_DEPLOYMENTS[@]}"; do
    kubectl -n ate-system patch deployment "${name}" --type=merge -p "${patch}"
  done
  for name in "${SYSTEM_STATEFULSETS[@]}"; do
    kubectl -n ate-system patch statefulset "${name}" --type=merge -p "${patch}"
  done

  for name in "${SYSTEM_DEPLOYMENTS[@]}"; do
    kubectl -n ate-system rollout status "deployment/${name}" --timeout=5m
  done
  for name in "${SYSTEM_STATEFULSETS[@]}"; do
    kubectl -n ate-system rollout status "statefulset/${name}" --timeout=10m
  done
}

deploy_workloads() {
  envoycap::log "deploying benchmark workloads"

  # ActorTemplate.spec is immutable, and every provisioning run rebuilds the
  # glutton image, so on a re-run the apply below would be rejected with
  # "Spec is immutable" the moment the digest differs. Delete first: the
  # template exists to be rebuilt from the manifest, and its golden snapshot is
  # re-taken from whatever image the run just pushed.
  kubectl -n benchmark-workloads delete actortemplate --all --ignore-not-found --wait=true

  # Apply the shared workloads template with the pool at zero, then patch the
  # scheduling settings in before scaling up. Applying 40 replicas first would
  # let the scheduler place worker pods -- which carry no resource requests
  # until the patch lands -- onto the control-plane and load-generator nodes.
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${WORKER_COUNT}|0|g" \
      benchmarking/workloads/manifests/workloads.yaml.tmpl |
    envoycap::ko apply -f -

  # nodeSelector and resources are first-class WorkerPool fields, not a
  # manifest hack. 40 pods x 1 CPU on one c3-standard-88 (~85 allocatable vCPU)
  # is under half the node, so a worker never waits on a core and worker-side
  # CPU cannot be confused for router capacity. The request is kept rather than
  # dropped because it is what makes that guarantee schedulable instead of
  # merely likely; the components under test keep their shipped (absent)
  # requests.
  kubectl -n benchmark-workloads patch workerpool benchmark-ateom --type=merge -p "$(cat <<EOF
{
  "spec": {
    "replicas": ${ENVOYCAP_WORKER_REPLICAS},
    "template": {
      "nodeSelector": {"cloud.google.com/gke-nodepool": "${ENVOYCAP_WORKER_POOL}"},
      "resources": {"requests": {"cpu": "1", "memory": "2Gi"}}
    }
  }
}
EOF
)"
}

wait_for_workers() {
  envoycap::log "waiting for ${ENVOYCAP_WORKER_REPLICAS} worker pods to be ready"
  local deadline=$((SECONDS + 900))
  while ((SECONDS < deadline)); do
    local ready
    ready="$(kubectl -n benchmark-workloads get pods -l "${WORKER_POD_SELECTOR}" \
      -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null |
      grep -c True || true)"
    if [[ "${ready}" -ge "${ENVOYCAP_WORKER_REPLICAS}" ]]; then
      envoycap::log "${ready} worker pods ready"
      return
    fi
    echo "    ${ready}/${ENVOYCAP_WORKER_REPLICAS} ready..." >&2
    sleep 15
  done
  envoycap::die "timed out waiting for worker pods; kubectl -n benchmark-workloads get pods"
}

# wait_for_templates blocks until every ActorTemplate reports phase Ready.
#
# Worker pods being Ready is not the same thing as the system being able to
# place an actor on one. ateapi learns about workers from a pod informer and
# writes them into Valkey; until that lands, ResumeActor answers "no free
# workers available" while kubectl happily shows 40 running pods. Rolling the
# pool (which deploy_workloads does on every re-provision, since ko stamps a new
# image digest) drops all 40 registrations and re-adds them, so the window is
# real and it is minutes wide.
#
# ActorTemplate Ready is the honest gate: reaching it requires resuming a golden
# actor, which requires a free registered worker. Waiting on the thing that
# consumes the dependency beats waiting on a proxy for it.
wait_for_templates() {
  envoycap::log "waiting for ActorTemplates to be ready"
  local deadline=$((SECONDS + 900))
  while ((SECONDS < deadline)); do
    local phases
    phases="$(kubectl -n benchmark-workloads get actortemplate \
      -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null)"
    if [[ -n "${phases}" ]] && ! grep -qv '^Ready$' <<<"${phases}"; then
      envoycap::log "$(grep -c . <<<"${phases}") ActorTemplates ready"
      return
    fi
    echo "    templates: $(tr '\n' ' ' <<<"${phases}")" >&2
    sleep 15
  done
  envoycap::die "timed out waiting for ActorTemplates; kubectl -n benchmark-workloads get actortemplate"
}

summary() {
  echo
  echo "Cluster ${ENVOYCAP_CLUSTER_NAME} is ready."
  echo
  echo "  KUBECONFIG=${KUBECONFIG}"
  echo "  context=${KUBECTL_CONTEXT}"
  echo
  kubectl get nodes -L cloud.google.com/gke-nodepool
  echo
  kubectl -n ate-system get pods -o wide
  echo
  echo "Next: benchmarking/envoycap/run.sh"
}

main "$@"
