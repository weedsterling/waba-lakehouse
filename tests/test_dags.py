"""Tests d'intégrité des DAGs : import sans erreur, structure attendue, bonnes pratiques.

Nécessite apache-airflow (ignoré sinon) :
  AIRFLOW_HOME=/tmp/af PYTHONPATH=airflow/dags pytest -q tests/test_dags.py
"""
import os
import sys

import pytest

pytest.importorskip("airflow.sdk")  # le dossier airflow/ du dépôt ne suffit pas
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "airflow", "dags")
sys.path.insert(0, DAGS_DIR)

try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # Airflow 2
    from airflow.models.dagbag import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(DAGS_DIR)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_ingest_dag_structure(dagbag):
    dag = dagbag.dags["dag_ingest_raw"]
    assert {t.task_id for t in dag.tasks} == {"wait_for_new_files", "spark_ingest_bronze"}
    assert "countries" in dag.params and "source" in dag.params
    assert dag.max_active_runs == 1


def test_best_practices_on_every_dag(dagbag):
    for dag in dagbag.dags.values():
        assert dag.catchup is False, dag.dag_id
        for t in dag.tasks:
            assert t.retries >= 1, f"{dag.dag_id}.{t.task_id} sans retries"
            assert t.on_failure_callback, f"{dag.dag_id}.{t.task_id} sans alerte d'échec"


def test_silver_dag_triggered_by_bronze_asset(dagbag):
    dag = dagbag.dags["dag_bronze_to_silver"]
    assert "countries" in dag.params
    assert "bronze" in str(dag.timetable).lower() or "asset" in type(dag.timetable).__name__.lower()
    task = dag.get_task("spark_bronze_to_silver")
    assert any("silver" in str(o) for o in task.outlets)


def test_gold_dag_triggered_by_silver_asset(dagbag):
    dag = dagbag.dags["dag_silver_to_gold"]
    assert "countries" in dag.params
    task = dag.get_task("spark_silver_to_gold")
    assert any("gold" in str(o) for o in task.outlets)


def test_spark_jobs_serialized_by_pool(dagbag):
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    spark_tasks = [t for d in dagbag.dags.values() for t in d.tasks if isinstance(t, SparkSubmitOperator)]
    assert len(spark_tasks) >= 3
    assert all(t.pool == "spark" for t in spark_tasks)


def test_regulatory_dag(dagbag):
    dag = dagbag.dags["dag_regulatory_report"]
    assert dag.timetable.expression == "30 0 * * *"
    assert {"countries", "report_date"} <= set(dag.params)
    assert dag.get_task("notify_breaches").upstream_task_ids == {"spark_regulatory_report"}


@pytest.mark.parametrize("value,expected", [(None, "2026-10-03"), ("", "2026-10-03"), (" ", "2026-10-03"),
                                            ("2026-09-30", "2026-09-30")])
def test_regulatory_report_date_template(value, expected):
    """Régression : une valeur blanche transmise par le formulaire ne doit pas atteindre spark-submit."""
    from datetime import datetime

    from airflow.sdk.definitions._internal.templater import FILTERS
    from dag_regulatory_report import REPORT_DATE
    from jinja2 import Environment

    env = Environment()
    env.filters.update(FILTERS)
    run = type("Run", (), {"run_after": datetime(2026, 10, 3, 0, 30)})()
    assert env.from_string(REPORT_DATE).render(params={"report_date": value}, dag_run=run) == expected
