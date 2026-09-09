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

# Deploys the OpenClaw density workload (WorkerPool + ActorTemplate, see
# manifests in benchmarking/workloads/manifests/openclaw-density.yaml.tmpl
# and the README next to this script). Accepts the same flag surface as
# benchmarking/workloads/deploy.sh so the automation orchestrator can call
# either script interchangeably (--sandbox-class is validated: only gvisor).

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

if [[ -f .ate-dev-env.sh ]]; then
  source .ate-dev-env.sh
fi

if [[ -z "${BUCKET_NAME:-}" ]]; then
  echo "Error: BUCKET_NAME environment variable is not set." >&2
  exit 1
fi

MANIFEST_TEMPLATE="benchmarking/workloads/manifests/openclaw-density.yaml.tmpl"

if [[ ! -f "${MANIFEST_TEMPLATE}" ]]; then
  echo "Error: ${MANIFEST_TEMPLATE} not found in $(pwd)" >&2
  exit 1
fi

WORKER_COUNT=1
SANDBOX_CLASS="gvisor"
# Actor cgroup limit. A fresh-boot openclaw gateway idles at ~765Mi RSS
# (measured on 2026.5.7 locally: node + 6 auto-enabled plugins), so 1Gi
# would leave too little headroom.
ACTOR_MEMORY="1536Mi"
# Per-worker scheduling requests, aligned with the capacity math model
# (0.25 vCPU / 2 GiB per pod). 2Gi is validated by measurement: per-worker
# working-set peaks were ~0.8-1.3Gi under full-herd load. X (workers)
# ~= allocatable memory / WORKER_MEMORY_REQUEST; memory binds first.
WORKER_CPU_REQUEST="250m"
WORKER_MEMORY_REQUEST="2Gi"
# The OpenClaw image. The ActorTemplate CRD rejects tag-only references
# (changing the image invalidates snapshots), so this must carry @sha256.
# Default: the multi-arch index digest of 2026.5.7.
OPENCLAW_IMAGE="${OPENCLAW_IMAGE:-ghcr.io/openclaw/openclaw@sha256:1af3f457a2d5a1d210f4d95634fa5da6e23f9c0ac7b52ef4bc38e2ecf09704fd}"
# What the shell wrapper execs after writing the config. The image's own
# CMD (docker inspect): node openclaw.mjs gateway, workdir /app. The
# --allow-unconfigured flag is unnecessary — we write a config first.
OPENCLAW_ENTRYPOINT="node /app/openclaw.mjs gateway"
# readyz path on the gateway's port 80; answers 200 unauthenticated under
# token auth (verified locally on 2026.5.7).
OPENCLAW_READYZ_PATH="/healthz"
WAIT_TIMEOUT="300s"
# --discover-workers: deploy more replicas than the node fits and report how
# many become Ready — the measured worker count X.
DISCOVER_WORKERS=false

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Options:"
  echo "  --deploy                    Substitute and apply the workload"
  echo "  --delete                    Substitute and delete the workload"
  echo "  --worker-count N            WorkerPool replicas (default: 1)"
  echo "  --sandbox-class CLASS       Must be gvisor (accepted for orchestrator compatibility)"
  echo "  --actor-memory SIZE         ActorTemplate memory limit (default: 1536Mi)"
  echo "  --worker-cpu-request CPU    Per-worker CPU request (default: 250m, the math model's)"
  echo "  --worker-memory-request MEM Per-worker memory request (default: 2Gi, the math model's)"
  echo "  --openclaw-image REF        OpenClaw image, digest-pinned (@sha256:...)."
  echo "                              Also read from \$OPENCLAW_IMAGE"
  echo "  --openclaw-entrypoint CMD   Gateway exec line (default: 'node /app/openclaw.mjs gateway')"
  echo "  --readyz-path PATH          Unauthenticated 200 path on port 80 (default: /healthz)"
  echo "  --discover-workers            With --deploy: report Ready workers on the"
  echo "                              density node instead of requiring full rollout."
  echo "                              Use --worker-count above the expected cap."
  echo "  --wait-timeout DURATION     Worker readiness wait (default: 300s)"
  echo "  -h, --help                  Show this help message"
}

substitute() {
  sed -e "s|\${BUCKET_NAME}|${BUCKET_NAME}|g" \
      -e "s|\${WORKER_COUNT}|${WORKER_COUNT}|g" \
      -e "s|\${ACTOR_MEMORY}|${ACTOR_MEMORY}|g" \
      -e "s|\${WORKER_CPU_REQUEST}|${WORKER_CPU_REQUEST}|g" \
      -e "s|\${WORKER_MEMORY_REQUEST}|${WORKER_MEMORY_REQUEST}|g" \
      -e "s|\${OPENCLAW_IMAGE}|${OPENCLAW_IMAGE}|g" \
      -e "s|\${OPENCLAW_ENTRYPOINT}|${OPENCLAW_ENTRYPOINT}|g" \
      -e "s|\${OPENCLAW_READYZ_PATH}|${OPENCLAW_READYZ_PATH}|g" \
      "${MANIFEST_TEMPLATE}"
}

resolve_image() {
  if [[ -z "${OPENCLAW_IMAGE}" ]]; then
    echo "Error: --openclaw-image (or \$OPENCLAW_IMAGE) is required, e.g." >&2
    echo "  ghcr.io/openclaw/openclaw:2026.5.7" >&2
    exit 1
  fi
  if [[ "${OPENCLAW_IMAGE}" == *"@sha256:"* ]]; then
    return 0
  fi
  # The CRD requires a pinned image. Resolve the tag once, here, so every
  # actor and snapshot of a run refers to the same bytes.
  local digest=""
  if command -v docker >/dev/null 2>&1; then
    digest="$(docker buildx imagetools inspect "${OPENCLAW_IMAGE}" \
      --format '{{json .Manifest.Digest}}' 2>/dev/null | tr -d '"' || true)"
  fi
  if [[ -z "${digest}" ]]; then
    echo "Error: ${OPENCLAW_IMAGE} is not digest-pinned and the digest could" >&2
    echo "not be resolved locally. Pass --openclaw-image <ref>@sha256:<digest>." >&2
    exit 1
  fi
  OPENCLAW_IMAGE="${OPENCLAW_IMAGE%%@*}"
  OPENCLAW_IMAGE="${OPENCLAW_IMAGE%:*}@${digest}"
  echo "Pinned OpenClaw image: ${OPENCLAW_IMAGE}"
}

count_ready_workers() {
  kubectl get pods --namespace=benchmark-openclaw \
    --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name} {.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
    | grep -c ' True$' || true
}

deploy() {
  resolve_image
  echo "Deploying openclaw-density (worker_count=${WORKER_COUNT}, worker_request=${WORKER_CPU_REQUEST}/${WORKER_MEMORY_REQUEST}, actor_memory=${ACTOR_MEMORY})..."
  # ActorTemplate spec is immutable (self == oldSelf); remove the old
  # template so an image/config change applies cleanly. Safe: runs delete
  # their actors, and a template delete only blocks new actor creation.
  kubectl delete actortemplate --namespace=benchmark-openclaw \
    --all --ignore-not-found
  substitute | hack/run-tool.sh ko apply -f -

  if [[ "${DISCOVER_WORKERS}" == true ]]; then
    # Over-provisioned on purpose: rollout will not finish. Poll until the
    # Ready count is stable for three samples, then report it as X.
    echo "Discovering workers (waiting up to ${WAIT_TIMEOUT} for the Ready count to settle)..."
    local timeout_s
    timeout_s="$(kubectl_wait_seconds "${WAIT_TIMEOUT}")"
    local deadline=$((SECONDS + timeout_s))
    local last=-1 stable=0 ready=0
    while (( SECONDS < deadline )); do
      ready="$(count_ready_workers)"
      if [[ "${ready}" == "${last}" && "${ready}" -gt 0 ]]; then
        stable=$((stable + 1))
        if (( stable >= 3 )); then
          break
        fi
      else
        stable=0
      fi
      last="${ready}"
      sleep 10
    done
    echo "WORKERS=${ready}"
    echo "Pass this as --workers to the locust test (and use points 2x,3x,4x)."
    return 0
  fi

  echo "Waiting for the worker pool to be ready (timeout: ${WAIT_TIMEOUT})..."
  kubectl wait --for=create deployment/openclaw-density \
    --namespace=benchmark-openclaw --timeout="${WAIT_TIMEOUT}"
  kubectl rollout status deployment/openclaw-density \
    --namespace=benchmark-openclaw --timeout="${WAIT_TIMEOUT}"
  echo "Ready workers: $(count_ready_workers)"

  # Gate on the golden-snapshot path actually working: a (re)deployed
  # template triggers a fresh golden build, the template's Ready condition
  # does NOT wait for it, and a benchmark started mid-build races it into
  # spurious resume failures. One probe round trip proves the path.
  if kubectl ate --help >/dev/null 2>&1; then
    echo "Waiting for the golden snapshot (probe actor resume)..."
    kubectl ate create atespace openclaw-density >/dev/null 2>&1 || true
    kubectl ate create actor deploy-probe -a openclaw-density \
      --template=benchmark-openclaw/openclaw >/dev/null 2>&1 || true
    local probe_deadline=$((SECONDS + $(kubectl_wait_seconds "${WAIT_TIMEOUT}")))
    until kubectl ate resume actor deploy-probe -a openclaw-density >/dev/null 2>&1; do
      if (( SECONDS >= probe_deadline )); then
        echo "Error: probe actor did not resume within ${WAIT_TIMEOUT}; golden snapshot not ready" >&2
        exit 1
      fi
      sleep 10
    done
    kubectl ate suspend actor deploy-probe -a openclaw-density >/dev/null 2>&1 || true
    kubectl ate delete actor deploy-probe -a openclaw-density >/dev/null 2>&1 || true
    echo "Golden snapshot ready."
  else
    echo "WARNING: kubectl-ate not found; cannot verify the golden snapshot is built."
    echo "Resume one probe actor manually before starting the benchmark."
  fi
}

# Go-duration to seconds, for the discovery poll loop. Supports the same
# h/m/s forms the --wait-timeout validation admits.
kubectl_wait_seconds() {
  local d="$1" total=0 num unit
  while [[ -n "${d}" ]]; do
    num="${d%%[hms]*}"
    unit="${d:${#num}:1}"
    d="${d:$(( ${#num} + 1 ))}"
    case "${unit}" in
      h) total=$((total + num * 3600)) ;;
      m) total=$((total + num * 60)) ;;
      s) total=$((total + num)) ;;
    esac
  done
  echo "${total}"
}

delete() {
  echo "Deleting openclaw-density workload..."
  # ko:// worker image reference needs ko to resolve before kubectl.
  # OPENCLAW_IMAGE may be unset on delete; substitute a syntactically valid
  # placeholder so the manifest parses.
  OPENCLAW_IMAGE="${OPENCLAW_IMAGE:-deleted@sha256:0000000000000000000000000000000000000000000000000000000000000000}"
  substitute | hack/run-tool.sh ko delete --ignore-not-found -f -
}

if [[ "$#" -eq 0 ]]; then
  usage
  exit 1
fi

action=""
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --deploy) action="deploy" ;;
    --delete) action="delete" ;;
    --discover-workers) DISCOVER_WORKERS=true ;;
    --worker-count) shift; WORKER_COUNT="$1" ;;
    --worker-count=*) WORKER_COUNT="${1#*=}" ;;
    --sandbox-class) shift; SANDBOX_CLASS="$1" ;;
    --sandbox-class=*) SANDBOX_CLASS="${1#*=}" ;;
    --actor-memory) shift; ACTOR_MEMORY="$1" ;;
    --actor-memory=*) ACTOR_MEMORY="${1#*=}" ;;
    --worker-cpu-request) shift; WORKER_CPU_REQUEST="$1" ;;
    --worker-cpu-request=*) WORKER_CPU_REQUEST="${1#*=}" ;;
    --worker-memory-request) shift; WORKER_MEMORY_REQUEST="$1" ;;
    --worker-memory-request=*) WORKER_MEMORY_REQUEST="${1#*=}" ;;
    --openclaw-image) shift; OPENCLAW_IMAGE="$1" ;;
    --openclaw-image=*) OPENCLAW_IMAGE="${1#*=}" ;;
    --openclaw-entrypoint) shift; OPENCLAW_ENTRYPOINT="$1" ;;
    --openclaw-entrypoint=*) OPENCLAW_ENTRYPOINT="${1#*=}" ;;
    --readyz-path) shift; OPENCLAW_READYZ_PATH="$1" ;;
    --readyz-path=*) OPENCLAW_READYZ_PATH="${1#*=}" ;;
    --wait-timeout) shift; WAIT_TIMEOUT="$1" ;;
    --wait-timeout=*) WAIT_TIMEOUT="${1#*=}" ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Error: Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [[ "${SANDBOX_CLASS}" != "gvisor" ]]; then
  echo "Error: openclaw-density supports only --sandbox-class gvisor, got '${SANDBOX_CLASS}'" >&2
  exit 1
fi

if ! [[ "${WAIT_TIMEOUT}" =~ ^([0-9]+(h|m|s))+$ ]]; then
  echo "Error: --wait-timeout must be a Go duration like 300s, 10m, or 1h30m, got '${WAIT_TIMEOUT}'" >&2
  exit 1
fi

if [[ "${action}" == "deploy" ]]; then
  deploy
elif [[ "${action}" == "delete" ]]; then
  delete
fi
