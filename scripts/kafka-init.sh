#!/usr/bin/env bash
# =============================================================================
# Crée les topics Kafka du Level 3 (idempotent : --if-not-exists).
#   8 partitions (8 pays), clé de message = country_code -> ordre garanti par pays.
#   Rétention : raw/silver 7 jours (rejouables par Spark), gold et DLQ 30 jours (audit).
# =============================================================================
set -euo pipefail
BS="${KAFKA_BOOTSTRAP:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh
DAY=86400000

for i in $(seq 1 60); do
  $KT --bootstrap-server "$BS" --list >/dev/null 2>&1 && break
  echo "attente de Kafka ($i/60)…"; sleep 2
done

create() {  # topic retention_ms
  $KT --bootstrap-server "$BS" --create --if-not-exists --topic "$1" \
      --partitions 8 --replication-factor 1 \
      --config retention.ms="$2" --config cleanup.policy=delete --config compression.type=producer
}

for t in raw-bank-transactions raw-insurance-operations raw-mobile-money-payments raw-loan-repayments \
         silver-bank-transactions silver-insurance-operations silver-mobile-money; do
  create "$t" $((7 * DAY))
done
for t in gold-fraud-alerts gold-aml-events gold-liquidity-alerts dlq-financial-events; do
  create "$t" $((30 * DAY))
done
$KT --bootstrap-server "$BS" --describe | grep -E "^Topic:" | awk '{print $2, "partitions=" $6}'
echo "Topics Kafka prêts"
