"""dag_bronze_to_silver — couche Bronze -> couche Silver (nettoyage, EUR, enrichissement).

Déclenchement : data-aware, dès que dag_ingest_raw publie l'asset BRONZE
(également déclenchable à la main pour un backfill par pays).
Publie l'asset SILVER, qui déclenche dag_silver_to_gold.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG
from waba.common import BRONZE, COUNTRIES_PARAM, DEFAULT_ARGS, SILVER, spark_job

with DAG(
    dag_id="dag_bronze_to_silver",
    description="Transformations Silver multi-pays : dédoublonnage, conversion EUR, jointures référentiels",
    schedule=[BRONZE],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    params={"countries": COUNTRIES_PARAM},
    tags=["waba", "silver"],
) as dag:

    spark_job(
        task_id="spark_bronze_to_silver",
        script="bronze_to_silver.py",
        args=["--countries", "{{ params.countries | join(',') }}"],
        outlets=[SILVER],
        execution_timeout=timedelta(minutes=45),
    )
