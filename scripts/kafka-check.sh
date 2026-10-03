#!/usr/bin/env bash
# Contrôle du speed layer : nombre de messages par topic + un exemple de message.
# Usage : ./scripts/kafka-check.sh [topic-exemple]
set -euo pipefail
cd "$(dirname "$0")/.."
K="docker compose exec -T kafka /opt/kafka/bin"
echo "== messages par topic (somme des offsets de fin) =="
$K/kafka-get-offsets.sh --bootstrap-server kafka:9092 --time -1 \
  | awk -F: '{n[$1]+=$3} END {for (t in n) printf "%-30s %8d\n", t, n[t]}' | sort
TOPIC="${1:-raw-bank-transactions}"
echo "== exemple de message ($TOPIC) : clé | valeur =="
$K/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic "$TOPIC" --from-beginning \
  --max-messages 1 --timeout-ms 10000 --property print.key=true --property key.separator=" | " 2>/dev/null || true
