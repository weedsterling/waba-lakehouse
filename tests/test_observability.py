"""Level 4 (9.7) : cohérence de la supervision as code — alertes Grafana, métriques, collecte, moindre privilège.

Les 3 alertes du challenge relient des sources différentes (kube-state-metrics, Kafka Exporter, base Airflow) :
ces tests empêchent qu'un renommage (job Spark, requête, DAG, datasource) ne rende une alerte silencieuse.
"""
import json
import re
import sys
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")
ROOT = Path(__file__).resolve().parents[1]
K8S = ROOT / "k8s"
ALERTS = yaml.safe_load((K8S / "charts/observability/files/alerts/waba-alerts.yaml").read_text())
RULES = {r["uid"]: r for g in ALERTS["groups"] for r in g["rules"]}
KPS = yaml.safe_load((K8S / "monitoring/kube-prometheus-stack.yaml").read_text())
DATASOURCES = {"prometheus"} | {d["uid"] for d in KPS["grafana"]["additionalDataSources"]}


def query(rule: dict) -> dict:
    return next(d["model"] for d in rule["data"] if d["refId"] == "A")


def threshold(rule: dict) -> dict:
    cond = next(d["model"] for d in rule["data"] if d["refId"] == rule["condition"])
    return cond["conditions"][0]["evaluator"]


def test_the_three_challenge_alerts_are_provisioned():
    assert set(RULES) == {"waba-fraud-job-error", "waba-aml-consumer-lag", "waba-regulatory-report-failed"}
    for r in RULES.values():
        refs = {d["refId"] for d in r["data"]}
        assert r["condition"] in refs and not r["isPaused"]
        assert {d["datasourceUid"] for d in r["data"]} <= DATASOURCES | {"__expr__"}, r["uid"]


def test_fraud_alert_watches_the_detection_job_for_five_minutes():
    r = RULES["waba-fraud-job-error"]
    apps = {a["name"] for a in yaml.safe_load((K8S / "charts/spark-jobs/values.yaml").read_text())["apps"]}
    assert 'name="stream-silver-gold"' in query(r)["expr"] and "stream-silver-gold" in apps
    assert r["for"] == "5m" and threshold(r) == {"type": "lt", "params": [1]}
    assert r["noDataState"] == "Alerting"                     # application supprimée = job arrêté
    # métrique produite par kube-state-metrics (customResourceState sur les SparkApplication)
    crs = KPS["kube-state-metrics"]["customResourceState"]["config"]["spec"]["resources"][0]
    metric = f'{crs["metricNamePrefix"]}_{crs["metrics"][0]["name"]}'
    assert query(r)["expr"].strip().startswith(f"max({metric}{{")
    assert "RUNNING" in crs["metrics"][0]["each"]["stateSet"]["list"]
    assert {"apiGroups": ["sparkoperator.k8s.io"], "resources": ["sparkapplications"],
            "verbs": ["list", "watch"]} in KPS["kube-state-metrics"]["rbac"]["extraRules"]


def test_aml_alert_targets_the_group_published_by_the_rules_query():
    pytest.importorskip("pyspark")
    sys.path.insert(0, str(ROOT / "spark"))
    from waba_spark import monitoring

    r = RULES["waba-aml-consumer-lag"]
    group = monitoring.group_id("rules")
    assert f'consumergroup="{group}"' in query(r)["expr"]
    assert threshold(r) == {"type": "gt", "params": [5000]}
    job = (ROOT / "spark/jobs/stream_silver_to_gold.py").read_text()
    assert '"rules", on_rules' in job and "monitoring.install(spark, BOOTSTRAP)" in job
    assert "gold-aml-events" in job                            # la requête « rules » produit les événements AML
    exporter = yaml.safe_load((K8S / "charts/kafka/values.yaml").read_text())["exporter"]
    assert re.fullmatch(exporter["groupRegex"], group)


def test_regulatory_alert_checks_todays_report_at_six_utc():
    r = RULES["waba-regulatory-report-failed"]
    sql = query(r)["rawSql"]
    assert "dag_id = 'dag_regulatory_report'" in sql and ">= 6" in sql and "'UTC'" in sql
    assert "state = 'success'" in sql and "= 'failed'" in sql
    assert query(r)["datasource"]["uid"] == "airflow-db" and threshold(r) == {"type": "gt", "params": [0]}
    dag = (ROOT / "airflow/dags/dag_regulatory_report.py").read_text()
    assert 'dag_id="dag_regulatory_report"' in dag and 'schedule="30 0 * * *"' in dag   # échéance après le run


def test_grafana_reads_airflow_with_a_select_only_account():
    ds = next(d for d in KPS["grafana"]["additionalDataSources"] if d["uid"] == "airflow-db")
    assert ds["user"] == "grafana_ro" and ds["secureJsonData"]["password"] == "${AIRFLOW_GRAFANA_DB_PASSWORD}"
    job = (K8S / "charts/airflow/templates/grafana-reader-job.yaml").read_text()
    grants = re.findall(r"GRANT (\w+) ON (.+?) TO grafana_ro", job)
    assert ("SELECT", "public.dag_run") in grants and all(p != "SELECT" or t == "public.dag_run" for p, t in grants)
    assert "default_transaction_read_only = on" in job


def test_no_secret_in_monitoring_values_and_least_privilege_collector():
    kps = (K8S / "monitoring/kube-prometheus-stack.yaml").read_text()
    assert "adminPassword" not in kps and KPS["grafana"]["admin"]["existingSecret"] == "waba-grafana"
    alloy = yaml.safe_load((K8S / "monitoring/alloy.yaml").read_text())
    resources = {res for rule in alloy["rbac"]["rules"] for res in rule["resources"]}
    assert resources == {"pods", "pods/log"}                      # ni secrets ni configmaps
    assert "clusterRules" not in alloy["rbac"] and alloy["rbac"]["namespaces"]   # Roles, pas de ClusterRole
    assert "loki.monitoring.svc.cluster.local:3100" in alloy["alloy"]["configMap"]["content"]


def test_remote_monitoring_charts_are_pinned():
    hf = yaml.safe_load((K8S / "helmfile.yaml").read_text())
    for rel in hf["releases"]:
        if not rel["chart"].startswith("./"):
            assert re.fullmatch(r"\d+\.\d+\.\d+", rel.get("version", "")), rel["name"]
    names = {r["name"] for r in hf["releases"] if r["namespace"] == "monitoring"}
    assert names == {"kube-prometheus-stack", "loki", "alloy", "observability"}


def test_dashboard_is_generated_from_code_and_uses_known_datasources():
    sys.path.insert(0, str(ROOT / "observability"))
    import build_dashboard as B

    committed = json.loads(B.OUT.read_text())
    assert committed == json.loads(json.dumps(B.DASHBOARD, ensure_ascii=False))   # JSON à jour du code
    uids = {p["datasource"]["uid"] for p in committed["panels"] if p.get("datasource")}
    assert uids <= DATASOURCES and committed["timezone"] == "utc"


def test_offset_committer_reads_kafka_sources_only():
    pytest.importorskip("pyspark")
    sys.path.insert(0, str(ROOT / "spark"))
    from waba_spark.monitoring import kafka_offsets

    class Src:
        def __init__(self, description, end):
            self.description, self.endOffset = description, end

    sources = [Src("KafkaV2[Subscribe[silver-bank-transactions, silver-mobile-money]]",
                   '{"silver-bank-transactions":{"0":120,"1":80},"silver-mobile-money":{"0":7}}'),
               Src("RateStreamV2[rowsPerSecond=5]", "3"), Src("KafkaV2[Subscribe[x]]", None)]
    assert kafka_offsets(sources) == {"silver-bank-transactions": {0: 120, 1: 80}, "silver-mobile-money": {0: 7}}
