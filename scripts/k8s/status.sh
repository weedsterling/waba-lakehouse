#!/usr/bin/env bash
# État de la plateforme : pods par domaine, volumes, Ingress.
set -uo pipefail
for ns in ingestion processing serving governance monitoring; do
  echo "== $ns =="; kubectl -n "$ns" get pods -o wide 2>/dev/null | awk '{print $1, $2, $3, $4, $5}' | column -t
done
echo "== volumes =="; kubectl get pvc -A --no-headers | awk '{print $1, $2, $3, $5}' | column -t
echo "== ingress =="; kubectl get ingress -A --no-headers | awk '{print $1, $2, $4}' | column -t
