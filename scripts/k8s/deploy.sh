#!/usr/bin/env bash
# Déploiement complet du Level 4, en une commande : ./scripts/k8s/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
./scripts/k8s/cluster-up.sh
./scripts/k8s/bootstrap.sh
for c in k8s/charts/*/; do helm lint --quiet "$c"; done      # charts maison validés avant déploiement
helmfile -f k8s/helmfile.yaml sync
./scripts/k8s/status.sh
