#!/usr/bin/env bash
# =============================================================================
# Namespaces (étiquetés par domaine), Secrets et ConfigMaps créés depuis .env et le dépôt.
# Idempotent (kubectl apply) ; AUCUN secret n'est écrit dans un manifeste versionné.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
[[ -f .env ]] || { echo ".env absent : lancer ./scripts/gen-secrets.sh" >&2; exit 1; }
set -a; source .env; set +a

apply() { kubectl apply -f - >/dev/null; }

for ns in ingestion processing serving governance monitoring; do
  kubectl create namespace "$ns" --dry-run=client -o yaml \
    | kubectl label --local -f - app.kubernetes.io/part-of=waba waba.io/domain="$ns" -o yaml | apply
done

secret() {  # namespace nom CLE=valeur...
  local ns=$1 name=$2 kv args=(); shift 2
  for kv in "$@"; do args+=("--from-literal=$kv"); done
  kubectl -n "$ns" create secret generic "$name" "${args[@]}" --dry-run=client -o yaml | apply
}
S3=("LAKEHOUSE_ACCESS_KEY=$LAKEHOUSE_ACCESS_KEY" "LAKEHOUSE_SECRET_KEY=$LAKEHOUSE_SECRET_KEY" "AWS_REGION=$AWS_REGION")
secret ingestion waba-minio-root "MINIO_ROOT_USER=$MINIO_ROOT_USER" "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD"
for ns in ingestion processing serving; do secret "$ns" waba-s3 "${S3[@]}"; done
secret processing waba-pii "PII_HASH_SALT=$PII_HASH_SALT"
[[ -n "${ICEBERG_DB_PASSWORD:-}" && "$ICEBERG_DB_PASSWORD" != change-me* ]] \
  || { echo "ICEBERG_DB_PASSWORD manquant : lancer ./scripts/gen-secrets.sh" >&2; exit 1; }
secret processing waba-catalog-db "ICEBERG_DB_PASSWORD=$ICEBERG_DB_PASSWORD"
secret ingestion waba-nifi "NIFI_ADMIN_USER=${NIFI_ADMIN_USER:-admin}" "NIFI_ADMIN_PASSWORD=$NIFI_ADMIN_PASSWORD" \
  "NIFI_SENSITIVE_PROPS_KEY=$NIFI_SENSITIVE_PROPS_KEY"

# Scripts et schémas versionnés dans le dépôt -> ConfigMaps (une seule source pour Compose et Kubernetes)
configmap() {  # namespace nom --from-file...
  local ns=$1 name=$2; shift 2
  kubectl -n "$ns" create configmap "$name" "$@" --dry-run=client -o yaml | apply
}
configmap ingestion minio-init-script --from-file=minio-init.sh=scripts/minio-init.sh
configmap ingestion nifi-provision-script --from-file=provision_flow.py=nifi/provision_flow.py
configmap serving trino-kafka-tables --from-file=trino/kafka/
# Code Spark monté dans les pods (même code que Compose, mis à jour sans reconstruire l'image)
configmap processing waba-spark-jobs --from-file=spark/jobs/
configmap processing waba-spark-lib --from-file=spark/waba_spark/

# Noms d'hôtes des interfaces (Ingress) -> IP du cluster, dans /etc/hosts de la VM
ip=$(minikube ip)
hosts="minio.waba.local generator.waba.local nifi.waba.local airflow.waba.local superset.waba.local keycloak.waba.local openmetadata.waba.local grafana.waba.local"
if ! grep -q "^$ip $hosts\$" /etc/hosts; then
  sudo sed -i '/waba\.local/d' /etc/hosts
  echo "$ip $hosts" | sudo tee -a /etc/hosts >/dev/null
fi
echo "Namespaces, secrets et /etc/hosts prêts (cluster $ip)"
