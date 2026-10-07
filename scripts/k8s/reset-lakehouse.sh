#!/usr/bin/env bash
# =============================================================================
# Remet à zéro le LAKEHOUSE Kubernetes (tables Iceberg + checkpoints des flux), sans toucher aux sources :
#   * conservé : raw-landing, archive (fichiers CSV), topics Kafka, configuration ;
#   * supprimé : bucket lakehouse (données Iceberg + checkpoints), base du catalogue.
# Usage : après un changement de catalogue, puis rejouer les données depuis l'archive :
#   ./scripts/k8s/reset-lakehouse.sh
#   ./scripts/k8s/spark-run.sh ingest-bronze ingest_raw.py --namespace bronze --source archive --no-archive
#   ./scripts/k8s/spark-run.sh bronze-silver bronze_to_silver.py
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
set -a; source .env; set +a
read -r -p "Supprimer toutes les tables Iceberg et les checkpoints du cluster Kubernetes ? [oui/N] " ok
[[ "$ok" == "oui" ]] || { echo "annulé"; exit 1; }

kubectl -n processing delete sparkapplication --all --wait=true
kubectl -n ingestion run lakehouse-reset --rm -i --restart=Never --quiet \
  --image=bitnamilegacy/minio-client:2025.7.21-debian-12-r3 --image-pull-policy=IfNotPresent \
  --env="HOME=/tmp" --env="U=$MINIO_ROOT_USER" --env="P=$MINIO_ROOT_PASSWORD" --command -- sh -c \
  'mc alias set l http://minio:9000 "$U" "$P" >/dev/null && mc rm -r --force l/lakehouse/ >/dev/null 2>&1; mc ls l/lakehouse/ | wc -l'
# Base du catalogue : volume supprimé puis recréé vide par le StatefulSet
kubectl -n processing delete statefulset iceberg-catalog-db --ignore-not-found --wait=true
kubectl -n processing delete pvc data-iceberg-catalog-db-0 --ignore-not-found --wait=true
kubectl -n processing delete pvc iceberg-catalog --ignore-not-found      # ancien volume SQLite
echo "Lakehouse remis à zéro : relancer ./scripts/k8s/deploy.sh puis rejouer l'archive (cf. en-tête)"
