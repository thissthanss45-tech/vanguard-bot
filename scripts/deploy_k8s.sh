#!/usr/bin/env bash
# deploy_k8s.sh — деплой vanguard-bot в Kubernetes
# Использование: ./scripts/deploy_k8s.sh [VERSION] [CONTEXT]
#   VERSION — тег Docker-образа, по умолчанию берётся из git describe
#   CONTEXT — kubeconfig context, по умолчанию текущий
#
# Пример:
#   ./scripts/deploy_k8s.sh 1.4.0 my-cluster

set -euo pipefail

REPO="ghcr.io/OWNER/vanguard_bot"       # ← заменить на свой registry
VERSION="${1:-$(git describe --tags --abbrev=0 2>/dev/null || echo "latest")}"
K8S_CONTEXT="${2:-}"
NAMESPACE="vanguard"
K8S_DIR="$(cd "$(dirname "$0")/../k8s" && pwd)"

# ─── Цвет вывода ──────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()   { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ─── Зависимости ──────────────────────────────────────────────────────────────
command -v kubectl  &>/dev/null || die "kubectl не найден. Установи: https://kubernetes.io/docs/tasks/tools/"
command -v docker   &>/dev/null || die "docker не найден."

# ─── Context ──────────────────────────────────────────────────────────────────
if [[ -n "$K8S_CONTEXT" ]]; then
    kubectl config use-context "$K8S_CONTEXT"
fi
CURRENT_CTX="$(kubectl config current-context)"
info "Kubernetes context: $CURRENT_CTX"

# ─── Проверка секрета ─────────────────────────────────────────────────────────
if ! kubectl get secret vanguard-secret -n "$NAMESPACE" &>/dev/null; then
    warn "Secret 'vanguard-secret' не найден в namespace '$NAMESPACE'."
    warn "Создай его перед деплоем:"
    warn "  cp k8s/bot/secret.yaml.example /tmp/vs.yaml && vim /tmp/vs.yaml && kubectl apply -f /tmp/vs.yaml"
    read -rp "Продолжить без секрета? (y/N): " CONFIRM
    [[ "${CONFIRM,,}" == "y" ]] || exit 1
fi

# ─── Сборка и пуш образа ──────────────────────────────────────────────────────
info "Собираем образ ${REPO}:${VERSION} ..."
docker build -t "${REPO}:${VERSION}" -t "${REPO}:latest" .

info "Пушим образ ..."
docker push "${REPO}:${VERSION}"
docker push "${REPO}:latest"

# ─── Обновляем kustomization ──────────────────────────────────────────────────
info "Обновляем тег образа → ${VERSION} ..."
cd "$K8S_DIR"
# Если установлен kustomize — используем его, иначе sed
if command -v kustomize &>/dev/null; then
    kustomize edit set image "${REPO}=${REPO}:${VERSION}"
else
    warn "kustomize не найден, обновляем newTag через sed"
    sed -i "s/newTag: .*/newTag: \"${VERSION}\"/" kustomization.yaml
fi

# ─── Apply ────────────────────────────────────────────────────────────────────
info "Применяем манифесты (namespace: $NAMESPACE) ..."
kubectl apply -k .

# ─── Rollout wait ─────────────────────────────────────────────────────────────
info "Ждём rollout bot ..."
kubectl rollout status deployment/vanguard-bot -n "$NAMESPACE" --timeout=120s

info "Ждём rollout api ..."
kubectl rollout status deployment/vanguard-api -n "$NAMESPACE" --timeout=120s

# ─── Финал ────────────────────────────────────────────────────────────────────
echo ""
info "✅ Деплой v${VERSION} завершён!"
echo ""
kubectl get pods -n "$NAMESPACE" -o wide
echo ""
kubectl get svc -n "$NAMESPACE"
