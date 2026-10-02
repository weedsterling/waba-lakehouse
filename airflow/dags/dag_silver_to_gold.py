"""dag_silver_to_gold — couche Silver -> couche Gold (7 KPIs métier).

Déclenchement : data-aware, dès que dag_bronze_to_silver publie l'asset SILVER
(également déclenchable à la main pour un backfill par pays).
Publie l'asset GOLD, consommé par les tableaux de bord et le reporting réglementaire.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG
from waba.common import COUNTRIES_PARAM, DEFAULT_ARGS, GOLD, SILVER, spark_job

with DAG(
    dag_id="dag_silver_to_gold",
    description="KPIs Gold : volumes, NPL (BCEAO), ARPU, loss ratio (CIMA), sinistres, mobile money, corridors",
    schedule=[SILVER],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    params={"countries": COUNTRIES_PARAM},
    tags=["waba", "gold"],
) as dag:

    spark_job(
        task_id="spark_silver_to_gold",
        script="silver_to_gold.py",
        args=["--countries", "{{ params.countries | join(',') }}"],
        outlets=[GOLD],
        execution_timeout=timedelta(minutes=30),
    )
