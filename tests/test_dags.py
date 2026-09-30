"""Tests d'intégrité des DAGs : import sans erreur, structure attendue, bonnes pratiques.

Nécessite apache-airflow (ignoré sinon) :
  AIRFLOW_HOME=/tmp/af PYTHONPATH=airflow/dags pytest -q tests/test_dags.py
"""
import os
import sys

import pytest

pytest.importorskip("airflow")
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
