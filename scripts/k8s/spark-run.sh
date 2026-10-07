#!/usr/bin/env bash
# =============================================================================
# Lance un job Spark batch sur Kubernetes (SparkApplication) et attend son résultat.
# Même modèle que les flux permanents (k8s/charts/spark-jobs) : une seule définition de la config Spark.
#   ./scripts/k8s/spark-run.sh <nom> <script.py> [arguments...]
#   ex. ./scripts/k8s/spark-run.sh ingest-bronze ingest_raw.py --namespace bronze
# Code retour : 0 si COMPLETED, 1 sinon (log du driver affiché).
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
[[ $# -ge 2 ]] || { sed -n '3,7p' "$0"; exit 2; }
name=$1 file=$2; shift 2
NS=processing
values=$(mktemp); trap 'rm -f "$values"' EXIT
python3 - "$name" "$file" "$@" > "$values" <<'PY'
import json, sys
name, file, *args = sys.argv[1:]
print(json.dumps({"apps": [{"name": name, "kind": "batch", "file": file, "args": args, "restart": "Never",
                            "ttl": 3600, "driverMemory": "1g", "executorMemory": "3g",
                            "executorCores": 2, "executorInstances": 1,
                            "conf": {"spark.sql.shuffle.partitions": "16"}}]}))
PY
kubectl -n "$NS" delete sparkapplication "$name" --ignore-not-found --wait=true >/dev/null
helm template spark-jobs k8s/charts/spark-jobs -f "$values" | kubectl -n "$NS" apply -f -
echo "job $name soumis ; suivi : kubectl -n $NS get sparkapplication $name -w"

for _ in $(seq 1 360); do                 # 1 h maximum
  state=$(kubectl -n "$NS" get sparkapplication "$name" -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
  case "$state" in
    COMPLETED) echo "✔ $name terminé"; exit 0 ;;
    FAILED|SUBMISSION_FAILED|FAILING)
      echo "✘ $name : $state" >&2
      kubectl -n "$NS" logs "$name-driver" --tail=60 2>/dev/null | grep -vE "^\s+at " >&2 || true
      exit 1 ;;
  esac
  sleep 10
done
echo "✘ $name : délai dépassé" >&2; exit 1
