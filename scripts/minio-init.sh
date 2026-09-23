#!/bin/sh
# Initialisation idempotente de MinIO : buckets + compte de service applicatif.
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

for bucket in raw-landing lakehouse archive; do
  mc mb --ignore-existing "local/$bucket"
done

# Compte de service (utilisé par Streamlit, Spark, Iceberg REST et Trino)
if ! mc admin user info local "$LAKEHOUSE_ACCESS_KEY" >/dev/null 2>&1; then
  mc admin user add local "$LAKEHOUSE_ACCESS_KEY" "$LAKEHOUSE_SECRET_KEY"
fi
mc admin policy attach local readwrite --user "$LAKEHOUSE_ACCESS_KEY" 2>/dev/null || true

# Versioning sur l'archive : protège contre toute suppression accidentelle
mc version enable local/archive

echo "MinIO initialisé : buckets raw-landing, lakehouse, archive"
