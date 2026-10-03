"""dag_ingest_raw — raw-landing (MinIO) -> couche Bronze Iceberg (bronze.*).

Déclenchement : toutes les 15 minutes ; un capteur vérifie d'abord la présence de
nouveaux fichiers dans s3://raw-landing (sinon le run est « skipped », sans erreur).
Publie l'asset BRONZE, qui déclenche automatiquement dag_bronze_to_silver.

Paramètres (UI « Trigger DAG w/ config ») :
  countries : sous-ensemble de pays (backfill sélectif)
  source    : landing (nominal) | archive (retraitement / backfill depuis les fichiers archivés)
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import DAG, Param
from waba.common import BRONZE, COUNTRIES_PARAM, DEFAULT_ARGS, spark_job

# Bucket surveillé : archive en mode retraitement, sinon raw-landing (surchargeable par Variable)
SOURCE_BUCKET = (
    "{{ 'archive' if params.source == 'archive' "
    "else var.value.get('waba_landing_bucket', 'raw-landing') }}"
)

with DAG(
    dag_id="dag_ingest_raw",
    description="Ingestion des CSV bruts MinIO vers la couche Bronze (Iceberg)",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,                      # jamais deux ingestions concurrentes
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    params={
        "countries": COUNTRIES_PARAM,
        "source": Param("landing", enum=["landing", "archive"], title="Source des fichiers"),
    },
    tags=["waba", "bronze", "ingestion"],
) as dag:

    wait_for_files = S3KeySensor(
        task_id="wait_for_new_files",
        aws_conn_id="minio_s3",
        bucket_name=SOURCE_BUCKET,
        bucket_key="*.csv",
        wildcard_match=True,
        mode="reschedule",                  # libère le slot d'exécution entre deux vérifications
        poke_interval=60,
        timeout=10 * 60,
        soft_fail=True,                     # aucun fichier -> run « skipped », pas en échec
    )

    ingest_bronze = spark_job(
        task_id="spark_ingest_bronze",
        script="ingest_raw.py",
        args=[
            "--namespace", "bronze",
            "--countries", "{{ params.countries | join(',') }}",
            "--source", "{{ params.source }}",
            # Lambda : les fichiers de moins de N minutes restent dans raw-landing pour NiFi (speed layer)
            "--min-age-minutes", "{{ var.value.get('waba_batch_min_age_minutes', '5') }}",
        ],
        outlets=[BRONZE],
        execution_timeout=timedelta(minutes=45),
    )

    wait_for_files >> ingest_bronze
