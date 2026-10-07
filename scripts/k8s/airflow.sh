#!/usr/bin/env bash
# CLI Airflow dans le cluster (pod du scheduler) : ./scripts/k8s/airflow.sh <commande airflow...>
#   ex. ./scripts/k8s/airflow.sh dags list
#       ./scripts/k8s/airflow.sh dags unpause dag_ingest_raw
#       ./scripts/k8s/airflow.sh dags trigger dag_ingest_raw
set -euo pipefail
[[ $# -ge 1 ]] || { sed -n '2,6p' "$0"; exit 2; }
exec kubectl -n processing exec airflow-scheduler-0 -c scheduler -- airflow "$@"
