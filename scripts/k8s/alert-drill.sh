#!/usr/bin/env bash
# =============================================================================
# Exercices de déclenchement des 3 alertes du challenge (démonstration et recette de la supervision).
#
#   ./scripts/k8s/alert-drill.sh status            état des 3 alertes (Grafana) + mesures sous-jacentes
#   ./scripts/k8s/alert-drill.sh incident-start    met le job 2 (stream-silver-gold) en échec
#        -> alerte « job Spark fraude en erreur > 5 min » après 5 minutes
#        -> alerte « lag consumer AML > 5000 » dès que 5 000 messages silver-* attendent : laisser tourner le
#           générateur en mode continu (http://generator.waba.local) pendant l'incident
#   ./scripts/k8s/alert-drill.sh incident-stop     rétablit le job 2 depuis Git (reprise exacte par le checkpoint)
#   ./scripts/k8s/alert-drill.sh regulatory-fail   exécution de dag_regulatory_report marquée en échec
#        -> alerte « dag_regulatory_report en échec à 06h00 UTC » (active à partir de 06h00 UTC)
#   ./scripts/k8s/alert-drill.sh regulatory-run    relance du rapport -> l'alerte se résout au succès
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/../.."
APP=stream-silver-gold
DAG=dag_regulatory_report

airflow_api() {  # $1 = action (fail|run) : API REST Airflow 3 depuis le pod api-server (aucun secret exposé)
  kubectl -n processing exec deploy/airflow-api-server -c api-server -- python - "$1" "$DAG" <<'PY'
import json, os, sys, urllib.request
action, dag = sys.argv[1], sys.argv[2]
base = "http://localhost:8080"
pw = json.load(open(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"]))["admin"]

def call(method, path, body, token=None):
    req = urllib.request.Request(base + path, data=json.dumps(body).encode(), method=method,
                                 headers={"Content-Type": "application/json",
                                          **({"Authorization": f"Bearer {token}"} if token else {})})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)

token = call("POST", "/auth/token", {"username": "admin", "password": pw})["access_token"]
run = call("POST", f"/api/v2/dags/{dag}/dagRuns",
           {"logical_date": None, "note": "Exercice d'alerte 9.7" if action == "fail" else "Relance après incident"},
           token)
if action == "fail":
    call("PATCH", f"/api/v2/dags/{dag}/dagRuns/{run['dag_run_id']}", {"state": "failed"}, token)
    print(f"exécution {run['dag_run_id']} marquée en échec")
else:
    call("PATCH", f"/api/v2/dags/{dag}", {"is_paused": False}, token)
    print(f"exécution {run['dag_run_id']} déclenchée (DAG activé)")
PY
}

status() {
  echo "== job 2 : $(kubectl -n processing get sparkapplication $APP -o jsonpath='{.status.applicationState.state}' \
    2>/dev/null || echo ABSENT)"
  echo "== lag du groupe waba-spark-rules (consumer AML) : $(kubectl -n ingestion exec waba-dual-role-0 -c kafka -- \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group waba-spark-rules \
    2>/dev/null | awk '$6 ~ /^[0-9]+$/ {s += $6} END {print s + 0}') messages"
  set -a; source .env; set +a
  echo "== alertes Grafana (http://grafana.waba.local, dossier WABA) =="
  curl -fsS -u "admin:$GRAFANA_ADMIN_PASSWORD" http://grafana.waba.local/api/prometheus/grafana/api/v1/rules \
    | python3 -c "$(cat <<'PY'
import json, sys
for group in json.load(sys.stdin)["data"]["groups"]:
    if group["file"] == "WABA":
        for rule in group["rules"]:
            print("  {:9} {:7} {}".format(rule["state"].upper(), rule["health"], rule["name"]))
PY
)"
}

case "${1:-}" in
  status) status ;;
  incident-start)
    kubectl -n processing patch sparkapplication $APP --type merge -p \
      '{"spec":{"mainApplicationFile":"local:///opt/waba/jobs/exercice_alerte_inexistant.py","restartPolicy":{"type":"Never"}}}'
    echo "✔ $APP en échec : alerte fraude attendue dans ~6 min ; lancer le générateur en continu pour le lag AML" ;;
  incident-stop)
    kubectl -n processing delete sparkapplication $APP --ignore-not-found --wait=true
    helmfile -f k8s/helmfile.yaml -l name=spark-jobs sync >/dev/null
    echo "✔ $APP redéployé depuis Git : reprise depuis le checkpoint, le lag se résorbe" ;;
  regulatory-fail) airflow_api fail ;;
  regulatory-run) airflow_api run ;;
  *) sed -n '3,14p' "$0"; exit 2 ;;
esac
