#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  Vanguard Bot — Kubernetes deploy script
#
#  Использование:
#    ./scripts/k8s-deploy.sh [OPTIONS]
#
#  Опции:
#    -t, --tag TAG        Docker image tag  (default: git-sha short, e.g. a1b2c3d)
#    -r, --registry REG   Image registry    (default: ghcr.io/$GITHUB_REPOSITORY_OWNER)
#    -e, --env ENV        Deployment env    (default: prod)
#    --dry-run            Только kubectl --dry-run=client
#    --skip-build         Не собирать образ (взять уже готовый из registry)
#    --skip-push          Не пушить образ   (полезно для локального minikube)
#    --no-rollback        Не откатываться при ошибке
#    -h, --help           Показать справку
#
#  Требования:
#    docker, kubectl (с настроенным kubeconfig), kustomize (или kubectl >= 1.21)
#
#  Примеры:
#    # Стандартный деплой (build → push → apply → verify)
#    ./scripts/k8s-deploy.sh
#
#    # Конкретный тег
#    ./scripts/k8s-deploy.sh --tag v1.4.0 --registry ghcr.io/myorg
#
#    # Только dry-run (проверить что применится)
#    ./scripts/k8s-deploy.sh --dry-run
#
#    # Деплой в minikube (без push)
#    ./scripts/k8s-deploy.sh --skip-push --registry localhost:5000
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
IFS=$'\n\t'

# ── Цвета ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
BOLD='\033[1m'; RESET='\033[0m'
log_info()    { echo -e "${CYAN}[INFO]${RESET}  $*"; }
log_ok()      { echo -e "${GREEN}[OK]${RESET}    $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${RESET}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; }
log_section() { echo -e "\n${BOLD}━━━ $* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"; }

# ── Defaults ────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
K8S_DIR="${REPO_ROOT}/k8s"
NAMESPACE="vanguard"

GIT_SHA="$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo "local")"
TAG="${GIT_SHA}"
REGISTRY="${REGISTRY:-ghcr.io/${GITHUB_REPOSITORY_OWNER:-OWNER}}"
IMAGE="${REGISTRY}/vanguard_bot"
ENV="prod"
DRY_RUN=false
SKIP_BUILD=false
SKIP_PUSH=false
NO_ROLLBACK=false

ROLLOUT_TIMEOUT="300s"
HEALTH_RETRIES=12
HEALTH_WAIT=10

# ── Аргументы ───────────────────────────────────────────────────────────────
usage() {
  grep '^#' "${BASH_SOURCE[0]}" | grep -v '#!/' | sed 's/^# \{0,2\}//'
  exit 0
}

while [[ $# -gt 0 ]]; do
  case $1 in
    -t|--tag)       TAG="$2";      shift 2 ;;
    -r|--registry)  REGISTRY="$2"; IMAGE="${REGISTRY}/vanguard_bot"; shift 2 ;;
    -e|--env)       ENV="$2";      shift 2 ;;
    --dry-run)      DRY_RUN=true;  shift ;;
    --skip-build)   SKIP_BUILD=true; shift ;;
    --skip-push)    SKIP_PUSH=true;  shift ;;
    --no-rollback)  NO_ROLLBACK=true; shift ;;
    -h|--help)      usage ;;
    *) log_error "Unknown argument: $1"; exit 1 ;;
  esac
done

FULL_IMAGE="${IMAGE}:${TAG}"

# ── Проверка зависимостей ────────────────────────────────────────────────────
log_section "Pre-flight checks"
for cmd in docker kubectl; do
  if command -v "$cmd" &>/dev/null; then
    log_ok "$cmd found: $(command -v "$cmd")"
  else
    log_error "$cmd not found in PATH"
    exit 1
  fi
done

# kustomize встроен в kubectl >= 1.21, но может быть и отдельно
if command -v kustomize &>/dev/null; then
  KUSTOMIZE_CMD="kustomize build"
  log_ok "standalone kustomize found"
else
  KUSTOMIZE_CMD="kubectl kustomize"
  log_ok "using kubectl --kustomize"
fi

# Проверяем доступность кластера
if ! kubectl cluster-info &>/dev/null; then
  log_error "Cannot reach Kubernetes cluster. Check your kubeconfig."
  exit 1
fi
log_ok "Kubernetes cluster reachable"

# ── 1. Build ─────────────────────────────────────────────────────────────────
log_section "1 / 4  Build Docker image"
if [[ "${SKIP_BUILD}" == true ]]; then
  log_warn "Skipping build (--skip-build)"
else
  log_info "Building ${FULL_IMAGE} …"
  docker build \
    --file "${REPO_ROOT}/Dockerfile" \
    --tag  "${FULL_IMAGE}" \
    --label "org.opencontainers.image.revision=${GIT_SHA}" \
    --label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "${REPO_ROOT}"
  # Таggируем latest тоже
  docker tag "${FULL_IMAGE}" "${IMAGE}:latest"
  log_ok "Build complete: ${FULL_IMAGE}"
fi

# ── 2. Push ──────────────────────────────────────────────────────────────────
log_section "2 / 4  Push to registry"
if [[ "${SKIP_PUSH}" == true ]]; then
  log_warn "Skipping push (--skip-push)"
elif [[ "${DRY_RUN}" == true ]]; then
  log_warn "Dry-run: would push ${FULL_IMAGE}"
else
  log_info "Pushing ${FULL_IMAGE} …"
  docker push "${FULL_IMAGE}"
  docker push "${IMAGE}:latest"
  log_ok "Push complete"
fi

# ── 3. Применяем манифесты ───────────────────────────────────────────────────
log_section "3 / 4  Apply Kubernetes manifests"

# Патчим версию образа в kustomization.yaml
TMP_KUSTOMIZE="$(mktemp -d)"
cp -r "${K8S_DIR}/." "${TMP_KUSTOMIZE}/"

# Сменить newTag в kustomization.yaml
sed -i "s/newTag: .*/newTag: \"${TAG}\"/" "${TMP_KUSTOMIZE}/kustomization.yaml"
log_info "Image tag set to: ${TAG}"

if [[ "${DRY_RUN}" == true ]]; then
  log_info "Dry-run: rendering manifests to stdout"
  ${KUSTOMIZE_CMD} "${TMP_KUSTOMIZE}"
  rm -rf "${TMP_KUSTOMIZE}"
  log_ok "Dry-run complete — no changes applied"
  exit 0
fi

log_info "Applying manifests from ${K8S_DIR} …"
${KUSTOMIZE_CMD} "${TMP_KUSTOMIZE}" | kubectl apply -f -
rm -rf "${TMP_KUSTOMIZE}"
log_ok "Manifests applied"

# ── 4. Rollout & Health check ────────────────────────────────────────────────
log_section "4 / 4  Rollout status"

DEPLOYMENTS=("vanguard-bot" "vanguard-api")
FAILED_DEPLOY=()

for deploy in "${DEPLOYMENTS[@]}"; do
  log_info "Waiting for Deployment/${deploy} (timeout=${ROLLOUT_TIMEOUT}) …"
  if kubectl rollout status deployment/"${deploy}" \
       -n "${NAMESPACE}" \
       --timeout="${ROLLOUT_TIMEOUT}"; then
    log_ok "Deployment/${deploy} rolled out successfully"
  else
    log_error "Deployment/${deploy} failed to roll out"
    FAILED_DEPLOY+=("${deploy}")
  fi
done

# ── Rollback при ошибке ──────────────────────────────────────────────────────
if [[ ${#FAILED_DEPLOY[@]} -gt 0 ]]; then
  if [[ "${NO_ROLLBACK}" == true ]]; then
    log_warn "--no-rollback: skipping rollback"
  else
    log_warn "Rolling back failed deployments: ${FAILED_DEPLOY[*]}"
    for deploy in "${FAILED_DEPLOY[@]}"; do
      kubectl rollout undo deployment/"${deploy}" -n "${NAMESPACE}" || true
      log_warn "Rollback triggered for ${deploy}"
    done
  fi
  log_error "Deploy FAILED. Check: kubectl get events -n ${NAMESPACE} --sort-by=.lastTimestamp"
  exit 1
fi

# ── Health check API ─────────────────────────────────────────────────────────
API_SVC_PORT=$(kubectl get svc vanguard-api -n "${NAMESPACE}" \
  -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "")

if [[ -n "${API_SVC_PORT}" ]]; then
  log_info "Checking API health (port ${API_SVC_PORT}, ${HEALTH_RETRIES} retries) …"
  NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || echo "127.0.0.1")
  HEALTH_URL="http://${NODE_IP}:${API_SVC_PORT}/health"

  for i in $(seq 1 "${HEALTH_RETRIES}"); do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${HEALTH_URL}" 2>/dev/null || echo "000")
    if [[ "${HTTP_CODE}" == "200" ]]; then
      log_ok "API health check passed (HTTP 200)"
      break
    fi
    log_warn "Attempt ${i}/${HEALTH_RETRIES}: HTTP ${HTTP_CODE}, retrying in ${HEALTH_WAIT}s …"
    sleep "${HEALTH_WAIT}"
    if [[ ${i} -eq "${HEALTH_RETRIES}" ]]; then
      log_error "API health check failed after ${HEALTH_RETRIES} attempts"
      exit 1
    fi
  done
else
  log_warn "NodePort not found, skipping HTTP health check"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}✅  Deploy complete!${RESET}"
echo -e "   Image  : ${FULL_IMAGE}"
echo -e "   Cluster: $(kubectl config current-context)"
echo -e "   NS     : ${NAMESPACE}"
echo ""
echo "  Полезные команды:"
echo "    kubectl get pods -n ${NAMESPACE}"
echo "    kubectl logs -n ${NAMESPACE} -l app=vanguard-api --tail=50"
echo "    kubectl logs -n ${NAMESPACE} -l app=vanguard-bot --tail=50"
echo "    kubectl get hpa -n ${NAMESPACE}"
