#!/usr/bin/env bash
# =============================================================================
# OpenMetadata à la demande (serveur + Elasticsearch ≈ 3 Go) : la mémoire du nœud va d'abord aux pipelines.
#   ./scripts/k8s/governance.sh up        démarre OpenMetadata et active le rafraîchissement quotidien
#   ./scripts/k8s/governance.sh catalog   lance le catalogue as code maintenant (découverte, docs, PII, lineage)
#   ./scripts/k8s/governance.sh down      arrête OpenMetadata (données conservées sur les volumes)
# NB : deploy.sh / helmfile sync remettent OpenMetadata à l'arrêt (replicas 0 dans le chart).
# =============================================================================
set -euo pipefail
NS=governance
case "${1:-}" in
  up)
    kubectl -n $NS scale statefulset openmetadata-postgres openmetadata-search --replicas=1
    kubectl -n $NS rollout status statefulset openmetadata-postgres --timeout=300s
    kubectl -n $NS rollout status statefulset openmetadata-search --timeout=600s
    kubectl -n $NS scale deployment openmetadata --replicas=1
    echo "démarrage du serveur (migration du schéma au premier lancement : 3 à 8 min)…"
    kubectl -n $NS rollout status deployment openmetadata --timeout=1200s
    kubectl -n $NS patch cronjob openmetadata-catalog -p '{"spec":{"suspend":false}}' >/dev/null
    echo "✔ OpenMetadata : http://openmetadata.waba.local (admin@open-metadata.org / OM_ADMIN_PASSWORD de .env)" ;;
  catalog)
    ready=$(kubectl -n $NS get deployment openmetadata -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
    [[ "${ready:-0}" -ge 1 ]] || { echo "✘ OpenMetadata n'est pas démarré : lancer d'abord $0 up" >&2; exit 1; }
    job="catalog-$(date +%Y%m%d%H%M%S)"
    kubectl -n $NS create job "$job" --from=cronjob/openmetadata-catalog
    kubectl -n $NS wait --for=condition=ready pod -l job-name="$job" --timeout=300s >/dev/null || true
    kubectl -n $NS logs -f "job/$job" | grep -vE "^\s*$|WARNING|warnings.warn" || true
    kubectl -n $NS wait --for=condition=complete "job/$job" --timeout=60s ;;
  down)
    kubectl -n $NS patch cronjob openmetadata-catalog -p '{"spec":{"suspend":true}}' >/dev/null || true
    kubectl -n $NS scale deployment openmetadata --replicas=0
    kubectl -n $NS scale statefulset openmetadata-search openmetadata-postgres --replicas=0
    echo "✔ OpenMetadata arrêté (volumes conservés)" ;;
  *) sed -n '3,7p' "$0"; exit 2 ;;
esac
