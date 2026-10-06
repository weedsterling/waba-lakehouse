#!/usr/bin/env bash
# Déploiement complet du Level 4, en une commande : ./scripts/k8s/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
# Images à copier dans le cluster depuis le Docker de la VM (si présentes) : images maison + images
# publiques déjà téléchargées par la stack Compose. Évite les limites de Docker Hub et les longs
# téléchargements à chaque recréation du cluster (déploiement reproductible, même hors réseau).
export WABA_IMAGES="${WABA_IMAGES:-waba/generator:1.0 trinodb/trino:467 apache/nifi:1.28.1 python:3.12-slim \
  tabulario/iceberg-rest:1.6.0 ghcr.io/coollabsio/minio:RELEASE.2025-10-15T17-29-55Z \
  bitnamilegacy/minio-client:2025.7.21-debian-12-r3 busybox:1.36}"
./scripts/k8s/cluster-up.sh
./scripts/k8s/bootstrap.sh
for c in k8s/charts/*/; do helm lint --quiet "$c"; done      # charts maison validés avant déploiement
helmfile -f k8s/helmfile.yaml sync
./scripts/k8s/status.sh
