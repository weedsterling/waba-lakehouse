"""dag_regulatory_report — reporting réglementaire quotidien BCEAO / CIMA (00h30 UTC).

  1. spark_regulatory_report : photos datées reporting.bceao_prudential / reporting.cima_technical
     + exports CSV par régulateur + résumé JSON des dépassements (MinIO) ;
  2. notify_breaches : lit le résumé (Connection minio_s3) et émet une alerte structurée par
     dépassement (NPL > 5 %, loss ratio > 70 %).

Un dépassement est une alerte MÉTIER, pas un échec technique : le DAG reste vert et l'alerte
part vers le canal de supervision (logs JSON -> Loki/Grafana au Level 4).

Rapport d'une date passée : déclencher avec le paramètre report_date (AAAA-MM-JJ).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, task
from waba.common import COUNTRIES_PARAM, DEFAULT_ARGS, spark_job

log = logging.getLogger("waba.alerts")

EXPORT_BUCKET = "lakehouse"
BREACHES_KEY = "exports/regulatory/breaches/report_date={}.json"   # écrit par regulatory_report.py
# Date du rapport : paramètre explicite (backfill) sinon jour du déclenchement
# (trim : le formulaire de déclenchement peut transmettre une chaîne blanche au lieu d'une valeur vide)
REPORT_DATE = "{{ ((params.report_date or '') | trim) or (dag_run.run_after | ds) }}"

with DAG(
    dag_id="dag_regulatory_report",
    description="Reporting réglementaire quotidien : NPL (BCEAO) et loss ratio / sinistres (CIMA)",
    schedule="30 0 * * *",                  # 00h30 UTC (heure d'Abidjan = UTC)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    params={
        "countries": COUNTRIES_PARAM,
        "report_date": Param(None, type=["null", "string"], format="date", title="Date du rapport",
                             description="Vide = date du jour d'exécution (données arrêtées à J-1)"),
    },
    tags=["waba", "gold", "regulatory"],
) as dag:

    report = spark_job(
        task_id="spark_regulatory_report",
        script="regulatory_report.py",
        args=["--report-date", REPORT_DATE, "--countries", "{{ params.countries | join(',') }}"],
        execution_timeout=timedelta(minutes=30),
    )

    @task(task_id="notify_breaches")
    def notify_breaches(report_date: str) -> int:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        summary = json.loads(S3Hook(aws_conn_id="minio_s3").read_key(
            BREACHES_KEY.format(report_date), bucket_name=EXPORT_BUCKET))
        for b in summary["breaches"]:
            log.warning(json.dumps({"event": "regulatory_breach", "report_date": report_date, **b},
                                   ensure_ascii=False, default=str))
        log.info("rapport %s : %s dépassement(s) réglementaire(s)", report_date, summary["breach_count"])
        return summary["breach_count"]

    report >> notify_breaches(REPORT_DATE)
