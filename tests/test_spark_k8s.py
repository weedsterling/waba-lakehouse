"""Level 4 : soumission des jobs Spark par Airflow sur Kubernetes (waba/spark_k8s.py), sans cluster.

  AIRFLOW_HOME=/tmp/af PYTHONPATH=airflow/dags pytest -q tests/test_spark_k8s.py
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow.sdk")
ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "airflow" / "dags"
sys.path.insert(0, str(DAGS_DIR))

from waba import spark_k8s as K  # noqa: E402

TEMPLATE = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2", "kind": "SparkApplication",
    "metadata": {"name": "__NAME__", "labels": {"waba.io/kind": "batch"}},
    "spec": {"mainApplicationFile": "local:///opt/waba/jobs/__FILE__", "restartPolicy": {"type": "Never"},
             "driver": {"labels": {"waba.io/app": "__NAME__"}}, "executor": {"labels": {"waba.io/app": "__NAME__"}}},
}


def test_app_name_is_dns_safe_deterministic_and_per_try():
    n1 = K.app_name("dag_ingest_raw", "spark_ingest_bronze", "scheduled__2026-10-07T10:15:00+00:00", 1)
    assert n1 == K.app_name("dag_ingest_raw", "spark_ingest_bronze", "scheduled__2026-10-07T10:15:00+00:00", 1)
    assert n1 != K.app_name("dag_ingest_raw", "spark_ingest_bronze", "scheduled__2026-10-07T10:15:00+00:00", 2)
    assert n1 != K.app_name("dag_ingest_raw", "spark_ingest_bronze", "manual__2026-10-07T10:20:00+00:00", 1)
    assert n1.startswith("ingest-bronze-")
    long = K.app_name("d", "spark_" + "x" * 200, "r", 3)
    for n in (n1, long):
        assert re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", n) and len(n) <= 52, n


def test_build_manifest_substitutes_placeholders_and_arguments():
    m = K.build_manifest(TEMPLATE, "silver-abc-1", "bronze_to_silver.py", ["--countries", "CI,SN", 5],
                         labels={"waba.io/dag-id": "dag_bronze_to_silver"}, annotations={"run": "x"})
    assert "__NAME__" not in json.dumps(m) and "__FILE__" not in json.dumps(m)
    assert m["metadata"]["name"] == "silver-abc-1"
    assert m["spec"]["mainApplicationFile"] == "local:///opt/waba/jobs/bronze_to_silver.py"
    assert m["spec"]["arguments"] == ["--countries", "CI,SN", "5"]
    assert m["spec"]["driver"]["labels"]["waba.io/app"] == "silver-abc-1"
    assert m["metadata"]["labels"] == {"waba.io/kind": "batch", "waba.io/dag-id": "dag_bronze_to_silver"}
    assert TEMPLATE["metadata"]["name"] == "__NAME__"          # modèle non modifié


@pytest.mark.parametrize("script", ["../etc/passwd", "job.sh", "a b.py", ""])
def test_build_manifest_rejects_unsafe_script(script):
    with pytest.raises(ValueError):
        K.build_manifest(TEMPLATE, "x-1", script, [])


def test_chart_batch_template_uses_the_placeholders():
    tpl = (ROOT / "k8s/charts/spark-jobs/templates/batch-template.yaml").read_text()
    assert '"name" "__NAME__"' in tpl and '"file" "__FILE__"' in tpl and "spark-batch-template" in tpl


class FakeKube:
    """API Kubernetes simulée : la SparkApplication suit la séquence d'états fournie."""

    def __init__(self, states, driver_log=""):
        self.states, self.driver_log, self.calls, self.apps = list(states), driver_log, [], {}

    def request(self, method, path, body=None, raw=False):
        self.calls.append((method, path))
        name = path.rsplit("/", 1)[-1]
        if path.endswith("/log?tailLines=200") or "/log?" in path:
            return self.driver_log
        if method == "POST":
            self.apps[body["metadata"]["name"]] = body
            return body
        if method == "DELETE":
            return self.apps.pop(name, None)
        if method == "GET":
            if name not in self.apps:
                return None
            state = self.states.pop(0) if len(self.states) > 1 else self.states[0]
            return {"status": {"applicationState": {"state": state, "errorMessage": "driver exit 1"}}}
        raise AssertionError(method)


def _run(monkeypatch, states, driver_log=""):
    fake = FakeKube(states, driver_log)
    monkeypatch.setattr(K, "KubeClient", lambda: fake)
    monkeypatch.setattr(K, "load_template", lambda: TEMPLATE)
    op = K.SparkApplicationOperator(task_id="spark_bronze_to_silver", script="bronze_to_silver.py",
                                    arguments=["--countries", "CI"], poll_interval=0)
    ti = SimpleNamespace(dag_id="dag_bronze_to_silver", task_id="spark_bronze_to_silver", try_number=1)
    ctx = {"ti": ti, "run_id": "manual__2026-10-07T10:00:00+00:00"}
    return op, fake, ctx


def test_operator_success(monkeypatch):
    log = '{"ts": "t", "level": "INFO", "logger": "waba.bronze_to_silver", "msg": "ok"}\nWARN spark noise'
    op, fake, ctx = _run(monkeypatch, ["SUBMITTED", "RUNNING", "COMPLETED"], log)
    out = op.execute(ctx)
    assert out["state"] == "COMPLETED" and out["spark_application"].startswith("bronze-to-silver-")
    methods = [m for m, _ in fake.calls]
    assert methods[0] == "DELETE" and "POST" in methods       # nettoyage d'un éventuel run précédent
    assert all("/namespaces/processing/" in p for _, p in fake.calls)
    posted = next(iter(fake.apps.values()))
    assert posted["spec"]["arguments"] == ["--countries", "CI"]
    assert posted["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "airflow"


def test_operator_failure_raises_for_airflow_retries(monkeypatch):
    op, fake, ctx = _run(monkeypatch, ["RUNNING", "FAILED"], "Traceback\n    at org.apache.X\nValueError: boom")
    with pytest.raises(RuntimeError, match="FAILED.*driver exit 1"):
        op.execute(ctx)
    assert any("/pods/" in p and p.split("/pods/")[1].startswith("bronze-to-silver-") for _, p in fake.calls)


def test_operator_timeout_deletes_the_application(monkeypatch):
    op, fake, ctx = _run(monkeypatch, ["RUNNING"])

    def boom(name):
        raise TimeoutError("execution_timeout")

    monkeypatch.setattr(op, "_wait", boom)
    with pytest.raises(TimeoutError):
        op.execute(ctx)
    assert fake.apps == {} and fake.calls[-1][0] == "DELETE"


def test_dags_in_kubernetes_mode_use_spark_applications():
    """Mêmes DAGs qu'en Docker Compose : en mode kubernetes, chaque job Spark devient une SparkApplication
    (sous-processus : le mode est lu à l'import de waba.common)."""
    code = f"""
import json, sys
sys.path.insert(0, {str(DAGS_DIR)!r})
from airflow.dag_processing.dagbag import DagBag
from waba.spark_k8s import SparkApplicationOperator
bag = DagBag({str(DAGS_DIR)!r})
tasks = [t for d in bag.dags.values() for t in d.tasks if isinstance(t, SparkApplicationOperator)]
print(json.dumps({{"errors": list(bag.import_errors), "n": len(tasks),
                  "pools": sorted({{t.pool for t in tasks}}),
                  "scripts": sorted(t.script for t in tasks)}}))
"""
    env = {**os.environ, "WABA_SPARK_MODE": "kubernetes", "AIRFLOW__CORE__LOAD_EXAMPLES": "false"}
    out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=300)
    assert out.returncode == 0, out.stderr[-2000:]
    res = json.loads(out.stdout.strip().splitlines()[-1])
    assert res["errors"] == []
    assert res["pools"] == ["spark"]
    assert {"ingest_raw.py", "bronze_to_silver.py", "silver_to_gold.py", "regulatory_report.py"} <= set(res["scripts"])
