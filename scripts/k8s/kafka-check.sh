#!/usr/bin/env bash
# Kubernetes : nombre de messages par topic + un exemple (équivalent de scripts/kafka-check.sh)
set -euo pipefail
K="kubectl -n ingestion exec waba-dual-role-0 -c kafka -- /opt/kafka/bin"
echo "== messages par topic =="
$K/kafka-get-offsets.sh --bootstrap-server localhost:9092 --time -1 \
  | awk -F: '$1 !~ /^__/ {n[$1]+=$3} END {for (t in n) printf "%-30s %8d\n", t, n[t]}' | sort
TOPIC="${1:-raw-bank-transactions}"
echo "== exemple ($TOPIC) : clé | valeur =="
$K/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$TOPIC" --from-beginning \
  --max-messages 1 --timeout-ms 10000 --property print.key=true --property key.separator=" | " 2>/dev/null || true
