"""Génère le tableau de bord Grafana « WABA — Supervision plateforme » (JSON versionné dans le chart observability).

Source unique en Python (lisible, relue en revue de code) ; le JSON produit est déterministe :
  python observability/build_dashboard.py   -> k8s/charts/observability/files/dashboards/waba-supervision.json
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "k8s/charts/observability/files/dashboards/waba-supervision.json"
PROM = {"type": "prometheus", "uid": "prometheus"}
AIRFLOW = {"type": "grafana-postgresql-datasource", "uid": "airflow-db"}
LOKI = {"type": "loki", "uid": "loki"}
NAMESPACES = "ingestion|processing|serving|governance|monitoring"


def prom(expr: str, legend: str = "", instant: bool = False) -> dict:
    return {"refId": "A", "datasource": PROM, "expr": expr, "legendFormat": legend, "instant": instant,
            "range": not instant}


def sql(query: str, fmt: str = "table") -> dict:
    return {"refId": "A", "datasource": AIRFLOW, "rawQuery": True, "editorMode": "code", "format": fmt,
            "rawSql": query}


def steps(*pairs) -> dict:
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in pairs]}


def panel(pid: int, kind: str, title: str, grid: tuple, targets: list, datasource: dict, **extra) -> dict:
    x, y, w, h = grid
    p = {"id": pid, "type": kind, "title": title, "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "datasource": datasource, "targets": targets}
    p.update(extra)
    return p


def spark_state(pid: int, x: int, app: str, title: str) -> dict:
    return panel(pid, "stat", title, (x, 0, 6, 5), [prom(
        f'max(waba_sparkapp_state{{namespace="processing", name="{app}", state="RUNNING"}}) or on() vector(0)',
        instant=True)], PROM,
        description=f"SparkApplication {app} (kube-state-metrics). Alerte si hors RUNNING > 5 min (job fraude).",
        options={"colorMode": "background", "graphMode": "none", "reduceOptions": {"calcs": ["lastNotNull"]}},
        fieldConfig={"defaults": {"mappings": [{"type": "value", "options": {
            "1": {"text": "EN COURS", "color": "green"}, "0": {"text": "ARRÊTÉ / EN ERREUR", "color": "red"}}}],
            "thresholds": steps((None, "red"), (1, "green"))}, "overrides": []})


PANELS = [
    spark_state(1, 0, "stream-silver-gold", "Job fraude / AML (stream-silver-gold)"),
    spark_state(2, 6, "stream-raw-silver", "Job ingestion temps réel (stream-raw-silver)"),
    panel(3, "stat", "Lag consumer AML (messages)", (12, 0, 6, 5),
          [prom('sum(kafka_consumergroup_lag{consumergroup="waba-spark-rules"})', instant=True)], PROM,
          description="Messages silver-* non traités par la requête « rules » (groupe waba-spark-rules). Seuil 5 000.",
          options={"colorMode": "background", "graphMode": "area", "reduceOptions": {"calcs": ["lastNotNull"]}},
          fieldConfig={"defaults": {"unit": "short", "thresholds": steps((None, "green"), (2500, "orange"),
                                                                         (5000, "red"))}, "overrides": []}),
    panel(4, "stat", "Rapport réglementaire — dernière exécution", (18, 0, 6, 5),
          [sql("SELECT COALESCE((SELECT state FROM dag_run WHERE dag_id = 'dag_regulatory_report' "
               "ORDER BY id DESC LIMIT 1), 'aucune') AS etat")], AIRFLOW,
          description="dag_regulatory_report (00h30 UTC). Alerte si pas de succès du jour à 06h00 UTC.",
          options={"colorMode": "background", "graphMode": "none", "textMode": "value",
                   "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/^etat$/"}},
          fieldConfig={"defaults": {"mappings": [{"type": "value", "options": {
              "success": {"text": "SUCCÈS", "color": "green"}, "failed": {"text": "ÉCHEC", "color": "red"},
              "running": {"text": "EN COURS", "color": "blue"}, "queued": {"text": "EN ATTENTE", "color": "blue"},
              "aucune": {"text": "AUCUNE", "color": "orange"}}}]}, "overrides": []}),
    panel(5, "alertlist", "Alertes WABA", (0, 5, 8, 9), [], None,
          options={"viewMode": "list", "groupMode": "default", "maxItems": 10, "sortOrder": 1,
                   "stateFilter": {"firing": True, "pending": True, "noData": True, "error": True, "normal": True},
                   "alertInstanceLabelFilter": '{domaine=~"fraude|aml|reglementaire"}', "showInstances": False,
                   "folder": None}),
    panel(6, "timeseries", "Lag par groupe de consommateurs Spark", (8, 5, 16, 9),
          [prom('sum by (consumergroup) (kafka_consumergroup_lag{consumergroup=~"waba-spark-.*"})',
                "{{consumergroup}}")], PROM,
          fieldConfig={"defaults": {"unit": "short", "custom": {"thresholdsStyle": {"mode": "dashed"}},
                                    "thresholds": steps((None, "transparent"), (5000, "red"))}, "overrides": []}),
    panel(7, "timeseries", "Débit des topics Kafka (messages/s)", (0, 14, 12, 8),
          [prom('sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"(raw|silver|gold|dlq)-.*"}[5m]))',
                "{{topic}}")], PROM, fieldConfig={"defaults": {"unit": "short"}, "overrides": []}),
    panel(8, "timeseries", "Mémoire par domaine (namespace)", (12, 14, 12, 8),
          # Série au niveau pod (container="") : toujours présente dans cAdvisor, y compris sur Minikube (driver
          # Docker) où les séries par conteneur sont absentes ; pas de double comptage pod + conteneurs.
          [prom(f'sum by (namespace) (container_memory_working_set_bytes{{job="kubelet", namespace=~"{NAMESPACES}", '
                'container="", pod!=""})',
                "{{namespace}}")], PROM,
          fieldConfig={"defaults": {"unit": "bytes", "custom": {"stacking": {"mode": "normal"}, "fillOpacity": 30}},
                       "overrides": []}),
    panel(9, "table", "Dernières exécutions Airflow", (0, 22, 12, 9),
          [sql("SELECT dag_id, state, start_date, end_date, run_id FROM dag_run ORDER BY id DESC LIMIT 20")], AIRFLOW,
          fieldConfig={"defaults": {}, "overrides": [{"matcher": {"id": "byName", "options": "state"}, "properties": [
              {"id": "custom.cellOptions", "value": {"type": "color-background"}},
              {"id": "mappings", "value": [{"type": "value", "options": {
                  "success": {"color": "green"}, "failed": {"color": "red"}, "running": {"color": "blue"}}}]}]}]}),
    panel(10, "logs", "Journaux en erreur / avertissement (Loki)", (12, 22, 12, 9),
          # Filtre sur le texte (et pas sur le label level) : couvre aussi les journaux non JSON (log4j Spark,
          # Java, Python) dont le niveau n'est pas extrait par Alloy.
          [{"refId": "A", "datasource": LOKI,
            "expr": f'{{namespace=~"{NAMESPACES}"}} |~ `(?i)(\\berror\\b|\\bwarn(ing)?\\b|exception|traceback)`'}],
          LOKI, options={"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending",
                         "enableLogDetails": True}),
]

DASHBOARD = {
    "uid": "waba-supervision",
    "title": "WABA — Supervision plateforme",
    "description": "Flux temps réel, lag Kafka, reporting réglementaire, ressources et journaux (9.7)",
    "tags": ["waba", "observabilite"],
    "timezone": "utc",
    "refresh": "30s",
    "time": {"from": "now-6h", "to": "now"},
    "schemaVersion": 39,
    "version": 1,
    "editable": True,
    "panels": [{k: v for k, v in p.items() if v is not None} for p in PANELS],
}


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(DASHBOARD, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"{OUT.name} : {len(PANELS)} panneaux")


if __name__ == "__main__":
    main()
