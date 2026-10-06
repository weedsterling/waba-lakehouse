#!/usr/bin/env bash
# Kubernetes : client Trino. Usage : ./scripts/k8s/trino.sh < fichier.sql   ou   ./scripts/k8s/trino.sh "SELECT 1"
set -euo pipefail
if [[ $# -gt 0 ]]; then
  kubectl -n serving exec deploy/trino -- trino --catalog lakehouse --output-format ALIGNED --execute "$*"
else
  kubectl -n serving exec -i deploy/trino -- trino --catalog lakehouse --output-format ALIGNED
fi
