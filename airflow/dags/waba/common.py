"""Briques communes aux DAGs WABA : paramètres, politique de reprise, alertes, soumission Spark.

Aucun secret ici : les identifiants viennent des Connections Airflow
(`spark_default`, `minio_s3`) et des variables d'environnement injectées par Docker Compose.
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Asset, Param

log = logging.getLogger("waba.alerts")

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
JOBS_DIR = "/opt/waba/jobs"
# Pool Airflow (1 emplacement, créé par airflow-init) : un seul job Spark à la fois sur le
# cluster (6 Go / 4 cœurs). Les runs déclenchés en rafale par les assets font la queue
# au lieu de se disputer la mémoire des exécuteurs.
SPARK_POOL = "spark"

# Assets (data-aware scheduling) : chaque couche publiée déclenche la suivante
BRONZE = Asset("iceberg://lakehouse/bronze")
SILVER = Asset("iceberg://lakehouse/silver")
GOLD = Asset("iceberg://lakehouse/gold")

# Paramètre commun : backfill sélectif par pays (exigence Level 2)
COUNTRIES_PARAM = Param(
    COUNTRIES, type="array", items={"type": "string", "enum": COUNTRIES},
    title="Pays à traiter", description="Sous-ensemble de pays (backfill sélectif)",
)


def on_failure_alert(context) -> None:
    """Alerte d'échec : log JSON structuré (collecté par Loki au Level 4 et
    exploité par une règle d'alerte Grafana). Brancher ici Slack/Teams/e-mail si besoin."""
    ti = context.get("task_instance")
    log.error(json.dumps({
        "event": "task_failed",
        "dag_id": ti.dag_id if ti else None,
        "task_id": ti.task_id if ti else None,
        "run_id": context.get("run_id"),
        "try_number": ti.try_number if ti else None,
        "exception": str(context.get("exception"))[:500],
    }, ensure_ascii=False))


DEFAULT_ARGS = {
    "owner": "waba-data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "on_failure_callback": on_failure_alert,
}

# Driver Spark exécuté dans le conteneur airflow-scheduler (LocalExecutor, mode client) :
# les executors du spark-worker doivent pouvoir le joindre par son nom d'hôte.
SPARK_CONF = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.memory": "1g",
    "spark.executor.memory": "3g",          # 2 exécuteurs x 3g = SPARK_WORKER_MEMORY (6g)
    "spark.cores.max": "4",
}


def spark_job(task_id: str, script: str, args: list[str], **kwargs) -> SparkSubmitOperator:
    """Soumet un job PySpark du dépôt (spark/jobs/<script>) au cluster Spark."""
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",
        application=f"{JOBS_DIR}/{script}",
        application_args=args,
        name=f"waba-{task_id}",
        conf=SPARK_CONF,
        env_vars={"PYTHONPATH": "/opt/waba"},
        verbose=False,
        pool=SPARK_POOL,
        **kwargs,
    )
