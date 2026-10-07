"""Soumission des jobs Spark batch à Kubernetes (Level 4) : une tâche Airflow = une SparkApplication.

Le modèle de SparkApplication n'est PAS défini ici : il est rendu par le chart Helm `spark-jobs`
(ConfigMap `spark-batch-template`, monté dans les pods Airflow), qui porte aussi les flux temps réel.
Airflow n'y substitue que le nom, le script et les arguments -> une seule source de vérité pour la
configuration Spark (catalogue Iceberg, S3, secrets, ressources).

Cycle d'une tâche :
  1. nom déterministe par (DAG, tâche, run, tentative) ; une SparkApplication du même nom est supprimée ;
  2. création via l'API Kubernetes (compte de service `waba-airflow`, RBAC limité au namespace processing) ;
  3. suivi de l'état jusqu'à COMPLETED (succès) ou FAILED / SUBMISSION_FAILED (échec -> retries Airflow) ;
  4. journal du driver recopié dans le log Airflow (lignes JSON `waba.*` ; fin de trace en cas d'échec) ;
  5. arrêt de la tâche (timeout, « mark failed ») -> la SparkApplication est supprimée (pas de job orphelin).

Stdlib + PyYAML (fourni par Airflow) : aucune dépendance supplémentaire dans l'image.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from airflow.sdk import BaseOperator

API_VERSION = "sparkoperator.k8s.io/v1beta2"
TEMPLATE_PATH = os.environ.get("WABA_SPARK_TEMPLATE", "/opt/waba/k8s/sparkapplication.yaml")
NAMESPACE = os.environ.get("WABA_SPARK_NAMESPACE", "processing")
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
SUCCESS = {"COMPLETED"}
FAILURE = {"FAILED", "SUBMISSION_FAILED"}


def app_name(dag_id: str, task_id: str, run_id: str, try_number: int) -> str:
    """Nom DNS-1123 court et stable : <tâche>-<hash(dag, run)>-<tentative>.
    Une nouvelle tentative crée une nouvelle application ; la précédente reste consultable (TTL)."""
    base = re.sub(r"[^a-z0-9-]+", "-", task_id.lower()).removeprefix("spark-").strip("-")[:36].strip("-")
    digest = hashlib.sha1(f"{dag_id}/{run_id}".encode()).hexdigest()[:8]
    return f"{base or 'job'}-{digest}-{try_number}"


def build_manifest(template: dict, name: str, script: str, arguments: list[Any],
                   labels: dict[str, str] | None = None, annotations: dict[str, str] | None = None) -> dict:
    """Modèle du chart -> SparkApplication concrète (fonction pure, testée sans cluster)."""
    if not re.fullmatch(r"[A-Za-z0-9_]+\.py", script):
        raise ValueError(f"script invalide : {script!r}")
    text = json.dumps(template).replace("__NAME__", name).replace("__FILE__", script)
    m = json.loads(text)
    if m.get("kind") != "SparkApplication" or m.get("apiVersion") != API_VERSION:
        raise ValueError("le modèle n'est pas une SparkApplication sparkoperator.k8s.io/v1beta2")
    m["spec"]["arguments"] = [str(a) for a in arguments]
    m["metadata"].setdefault("labels", {}).update(labels or {})
    m["metadata"].setdefault("annotations", {}).update(annotations or {})
    return m


def load_template(path: str = TEMPLATE_PATH) -> dict:
    import yaml  # PyYAML, dépendance d'Airflow

    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def label_value(value: str) -> str:
    """Valeur d'étiquette Kubernetes valide (63 caractères, alphanumérique aux extrémités)."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value)[:63].strip("-_.")


class KubeClient:
    """Client minimal de l'API Kubernetes depuis un pod (jeton du compte de service, relu à chaque
    appel car il est renouvelé par le kubelet)."""

    def __init__(self) -> None:
        host, port = os.environ["KUBERNETES_SERVICE_HOST"], os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        self.base = f"https://{host}:{port}"
        self.ctx = ssl.create_default_context(cafile=f"{SA_DIR}/ca.crt")

    def request(self, method: str, path: str, body: dict | None = None, raw: bool = False):
        with open(f"{SA_DIR}/token", encoding="utf-8") as f:
            headers = {"Authorization": f"Bearer {f.read().strip()}", "Accept": "application/json"}
        data = None
        if body is not None:
            data, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=30) as r:
                payload = r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise RuntimeError(f"API Kubernetes {method} {path} -> HTTP {e.code} : "
                               f"{e.read().decode('utf-8', 'replace')[:1000]}") from e
        return payload if raw else json.loads(payload)


class SparkApplicationOperator(BaseOperator):
    """Exécute spark/jobs/<script> sur Kubernetes via le Spark Operator et attend son résultat."""

    template_fields = ("arguments",)
    ui_color = "#fdd9a6"

    def __init__(self, *, script: str, arguments: list[str] | None = None, namespace: str = NAMESPACE,
                 poll_interval: int = 15, **kwargs) -> None:
        super().__init__(**kwargs)
        self.script, self.arguments = script, list(arguments or [])
        self.namespace, self.poll_interval = namespace, poll_interval
        self._app: str | None = None
        self._kube: KubeClient | None = None

    # ------------------------------------------------------------------ chemins API
    def _apps(self) -> str:
        return f"/apis/{API_VERSION}/namespaces/{self.namespace}/sparkapplications"

    def _app_path(self, name: str) -> str:
        return f"{self._apps()}/{name}"

    # ------------------------------------------------------------------ cycle de vie
    def execute(self, context) -> dict:
        ti = context["ti"]
        name = app_name(ti.dag_id, ti.task_id, context["run_id"], ti.try_number)
        manifest = build_manifest(
            load_template(), name, self.script, self.arguments,
            labels={"waba.io/dag-id": label_value(ti.dag_id), "waba.io/task-id": label_value(ti.task_id),
                    "app.kubernetes.io/managed-by": "airflow"},
            annotations={"waba.io/airflow-run-id": context["run_id"]})
        kube = self._kube = KubeClient()
        self._delete(name, wait=True)
        kube.request("POST", self._apps(), manifest)
        self._app = name
        self.log.info("SparkApplication %s/%s soumise (%s %s)", self.namespace, name, self.script,
                      " ".join(manifest["spec"]["arguments"]))
        try:
            state = self._wait(name)
        except BaseException:
            self._delete(name)              # timeout / arrêt de la tâche : pas de job Spark orphelin
            self._app = None
            raise
        self._app = None
        if state in FAILURE:
            self._driver_log(name, tail=120, only_waba=False)
            raise RuntimeError(f"SparkApplication {name} : {state} ({self._error(name)})")
        self._driver_log(name, tail=200, only_waba=True)
        return {"spark_application": name, "state": state}

    def on_kill(self) -> None:
        if self._app and self._kube:
            self.log.warning("arrêt de la tâche : suppression de la SparkApplication %s", self._app)
            self._delete(self._app)

    def _wait(self, name: str) -> str:
        last = None
        while True:
            app = self._kube.request("GET", self._app_path(name))
            if app is None:
                raise RuntimeError(f"SparkApplication {name} supprimée pendant l'exécution")
            state = (app.get("status") or {}).get("applicationState", {}).get("state") or "PENDING"
            if state != last:
                self.log.info("SparkApplication %s : %s", name, state)
                last = state
            if state in SUCCESS | FAILURE:
                return state
            time.sleep(self.poll_interval)

    def _delete(self, name: str, wait: bool = False) -> None:
        try:
            if self._kube.request("DELETE", self._app_path(name)) is None:
                return
            for _ in range(60 if wait else 0):
                if self._kube.request("GET", self._app_path(name)) is None:
                    return
                time.sleep(2)
        except Exception as exc:  # noqa: BLE001 - le nettoyage ne doit jamais masquer l'erreur d'origine
            self.log.warning("suppression de %s impossible : %s", name, exc)

    def _error(self, name: str) -> str:
        app = self._kube.request("GET", self._app_path(name)) or {}
        return ((app.get("status") or {}).get("applicationState") or {}).get("errorMessage", "")[:500]

    def _driver_log(self, name: str, tail: int, only_waba: bool) -> None:
        q = urllib.parse.urlencode({"tailLines": tail})
        try:
            text = self._kube.request("GET", f"/api/v1/namespaces/{self.namespace}/pods/{name}-driver/log?{q}",
                                      raw=True) or ""
        except Exception as exc:  # noqa: BLE001
            self.log.warning("journal du driver indisponible : %s", exc)
            return
        for line in text.splitlines():
            if only_waba and '"logger": "waba.' not in line:
                continue
            if not only_waba and re.match(r"^\s+at ", line):
                continue                       # traces Java : on garde le message, pas la pile
            self.log.info("[driver] %s", line)
