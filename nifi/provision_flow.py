"""Provisionne le flux NiFi « waba-raw-ingestion » via l'API REST (flow-as-code, idempotent).

  ListS3 (raw-landing, toutes les 5 s, suivi par horodatage)
    -> RouteOnAttribute   (flux transactionnels uniquement ; référentiels ignorés)
    -> FetchS3Object
    -> UpdateRecord       (CSV -> JSON, + ingestion_timestamp, + source_file)
    -> PublishKafkaRecord_2_6 (1 message JSON par ligne, topic raw-<dataset>, clé = country_code, acks=all)

Back-pressure : 10 000 FlowFiles / 1 Go par connexion — si Kafka ralentit, les files se remplissent
puis NiFi cesse de lister MinIO au lieu de saturer le broker. Les échecs sont rejoués (pénalité)
ou parqués dans un entonnoir « quarantaine » visible dans l'UI : rien n'est perdu silencieusement.

Stdlib uniquement (urllib) : s'exécute dans une image python:slim sans dépendance.
Les noms de propriétés sont résolus via les descripteurs exposés par NiFi (nom interne OU libellé),
toute propriété inconnue arrête le script avec la liste des propriétés disponibles.

Pour reconstruire le flux : supprimer le Process Group dans l'UI puis relancer nifi-init.
"""
from __future__ import annotations

import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API = os.environ.get("NIFI_API", "https://nifi:8443/nifi-api")
PG_NAME = "waba-raw-ingestion"
# Certificat autosigné généré par NiFi au premier démarrage ; trafic limité au réseau Docker interne.
# Au Level 4, NiFi est exposé derrière l'Ingress avec un certificat géré.
CTX = ssl._create_unverified_context()  # noqa: S323
TOKEN: str | None = None

TRANSACTION_DATASETS = ["bank_transactions", "insurance_operations", "mobile_money_payments", "loan_repayments"]
BACK_PRESSURE = {"backPressureObjectThreshold": 10000, "backPressureDataSizeThreshold": "1 GB"}


def log(msg: str, **ctx) -> None:
    print(json.dumps({"logger": "waba.nifi_provision", "msg": msg, **ctx}, ensure_ascii=False), flush=True)


def call(method: str, path: str, body: dict | None = None, form: dict | None = None):
    headers, data = {}, None
    if body is not None:
        data, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
    if form is not None:
        data, headers["Content-Type"] = urllib.parse.urlencode(form).encode(), "application/x-www-form-urlencoded"
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    req = urllib.request.Request(API + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, context=CTX, timeout=60) as r:
            raw = r.read().decode()
    except urllib.error.HTTPError as e:
        raise SystemExit(f"NiFi {method} {path} -> HTTP {e.code} : {e.read().decode()[:2000]}") from e
    return json.loads(raw) if raw[:1] in ("{", "[") else raw


def login(user: str, password: str, attempts: int = 90) -> str:
    """NiFi met 1 à 3 minutes à démarrer : on attend que l'API d'authentification réponde."""
    for i in range(1, attempts + 1):
        try:
            req = urllib.request.Request(API + "/access/token", method="POST",
                                         data=urllib.parse.urlencode({"username": user, "password": password}).encode(),
                                         headers={"Content-Type": "application/x-www-form-urlencoded"})
            with urllib.request.urlopen(req, context=CTX, timeout=10) as r:
                return r.read().decode()
        except urllib.error.HTTPError as e:
            if e.code in (400, 401, 403):
                raise SystemExit(f"authentification NiFi refusée ({e.code}) : vérifier NIFI_ADMIN_USER/PASSWORD") from e
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError):
            pass
        log("attente de NiFi", attempt=i)
        time.sleep(5)
    raise SystemExit("NiFi injoignable")


def resolve(descriptors: dict, props: dict, dynamic: tuple[str, ...] = ()) -> dict:
    """Traduit {nom interne | libellé: valeur | libellé de valeur} en {nom interne: valeur}."""
    by_label = {d["displayName"]: d for d in descriptors.values()}
    out = {}
    for key, value in props.items():
        d = descriptors.get(key) or by_label.get(key)
        if d is None:
            if key in dynamic or key.startswith("/"):
                out[key] = value          # propriété dynamique (RouteOnAttribute, RecordPath)
                continue
            raise SystemExit(f"propriété inconnue « {key} » ; disponibles : {sorted(by_label)}")
        allowed = [a["allowableValue"] for a in d.get("allowableValues") or []]
        if allowed and value not in {a["value"] for a in allowed}:
            match = [a["value"] for a in allowed if a["displayName"] == value]
            if not match:
                raise SystemExit(f"valeur « {value} » invalide pour {key} ; possibles : "
                                 f"{[(a['displayName'], a['value']) for a in allowed]}")
            value = match[0]
        out[d["name"]] = value
    return out


def service(pg: str, type_: str, name: str, props: dict) -> str:
    s = call("POST", f"/process-groups/{pg}/controller-services",
             {"revision": {"version": 0}, "component": {"type": type_, "name": name}})
    s = call("PUT", f"/controller-services/{s['id']}", {"revision": s["revision"], "component": {
        "id": s["id"], "properties": resolve(s["component"]["descriptors"], props)}})
    call("PUT", f"/controller-services/{s['id']}/run-status", {"revision": s["revision"], "state": "ENABLED"})
    log("service créé et activé", name=name)
    return s["id"]


def processor(pg: str, type_: str, name: str, x: int, y: int, props: dict, *, dynamic: tuple[str, ...] = (),
              terminate: tuple[str, ...] = (), **config) -> dict:
    p = call("POST", f"/process-groups/{pg}/processors", {"revision": {"version": 0}, "component": {
        "type": type_, "name": name, "position": {"x": x, "y": y}}})
    rels = {r["name"] for r in p["component"]["relationships"]}
    p = call("PUT", f"/processors/{p['id']}", {"revision": p["revision"], "component": {"id": p["id"], "config": {
        "properties": resolve(p["component"]["config"]["descriptors"], props, dynamic),
        "autoTerminatedRelationships": sorted(rels & set(terminate)), **config}}})
    log("processeur créé", name=name, relationships=sorted(rels))
    return p


def connect(pg: str, src: str, dst: str, rels: list[str], dst_type: str = "PROCESSOR") -> None:
    call("POST", f"/process-groups/{pg}/connections", {"revision": {"version": 0}, "component": {
        "source": {"id": src, "groupId": pg, "type": "PROCESSOR"},
        "destination": {"id": dst, "groupId": pg, "type": dst_type},
        "selectedRelationships": rels, **BACK_PRESSURE}})


def main() -> int:
    global TOKEN
    TOKEN = login(os.environ.get("NIFI_ADMIN_USER", "admin"), os.environ["NIFI_ADMIN_PASSWORD"])
    root = call("GET", "/flow/process-groups/root")["processGroupFlow"]["id"]
    existing = [g for g in call("GET", f"/flow/process-groups/{root}")["processGroupFlow"]["flow"]["processGroups"]
                if g["component"]["name"] == PG_NAME]
    if existing:
        log("flux déjà provisionné : rien à faire (supprimer le Process Group pour le reconstruire)",
            id=existing[0]["id"])
        return 0

    pg = call("POST", f"/process-groups/{root}/process-groups", {"revision": {"version": 0}, "component": {
        "name": PG_NAME, "position": {"x": 0, "y": 0}}})["id"]
    log("process group créé", id=pg)

    s3 = {"Region": os.environ.get("AWS_REGION", "us-east-1"),
          "Access Key": os.environ["LAKEHOUSE_ACCESS_KEY"], "Secret Key": os.environ["LAKEHOUSE_SECRET_KEY"],
          "Endpoint Override URL": os.environ.get("MINIO_ENDPOINT", "http://minio:9000")}

    csv_reader = service(pg, "org.apache.nifi.csv.CSVReader", "csv-reader (colonnes texte)",
                         {"Schema Access Strategy": "csv-header-derived"})   # tout en texte : Spark typera
    json_reader = service(pg, "org.apache.nifi.json.JsonTreeReader", "json-reader", {})
    json_writer = service(pg, "org.apache.nifi.json.JsonRecordSetWriter", "json-writer",
                          {"Output Grouping": "output-oneline"})

    lst = processor(pg, "org.apache.nifi.processors.aws.s3.ListS3", "Lister raw-landing", 0, 0,
                    {**s3, "Bucket": os.environ.get("LANDING_BUCKET", "raw-landing"), "Listing Strategy": "timestamps"},
                    schedulingPeriod="5 sec", executionNode="PRIMARY")
    route = processor(pg, "org.apache.nifi.processors.standard.RouteOnAttribute", "Flux transactionnels", 0, 200,
                      {"transactions": "${filename:substringBefore('/'):in("
                                       + ",".join(f"'{d}'" for d in TRANSACTION_DATASETS) + ")}"},
                      dynamic=("transactions",), terminate=("unmatched",))
    fetch = processor(pg, "org.apache.nifi.processors.aws.s3.FetchS3Object", "Lire l'objet", 0, 400,
                      {**s3, "Bucket": "${s3.bucket}", "Object Key": "${filename}"}, penaltyDuration="30 sec")
    enrich = processor(pg, "org.apache.nifi.processors.standard.UpdateRecord", "CSV -> JSON + enrichissement",
                       0, 600, {"Record Reader": csv_reader, "Record Writer": json_writer,
                                "Replacement Value Strategy": "literal-value",
                                "/ingestion_timestamp": "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", 'GMT')}",
                                "/source_file": "${s3.bucket}/${filename}"})
    publish = processor(pg, "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
                        "Publier vers Kafka raw-*", 0, 800,
                        {"Kafka Brokers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
                         "Topic Name": "raw-${filename:substringBefore('/'):replace('_','-')}",
                         "Record Reader": json_reader, "Record Writer": json_writer,
                         "Use Transactions": "false", "Delivery Guarantee": "all",
                         "Message Key Field": "country_code", "Compression Type": "snappy"},
                        terminate=("success",), penaltyDuration="10 sec")
    quarantine = call("POST", f"/process-groups/{pg}/funnels", {"revision": {"version": 0}, "component": {
        "position": {"x": 500, "y": 600}}})["id"]

    connect(pg, lst["id"], route["id"], ["success"])
    connect(pg, route["id"], fetch["id"], ["transactions"])
    connect(pg, fetch["id"], enrich["id"], ["success"])
    connect(pg, fetch["id"], fetch["id"], ["failure"])                 # MinIO indisponible : rejeu pénalisé
    connect(pg, enrich["id"], publish["id"], ["success"])
    connect(pg, enrich["id"], quarantine, ["failure"], dst_type="FUNNEL")  # CSV illisible : parqué, visible
    connect(pg, publish["id"], publish["id"], ["failure"])             # Kafka indisponible : rejeu pénalisé

    # Validation asynchrone (services en cours d'activation) : on laisse jusqu'à 60 s à NiFi
    for _ in range(12):
        invalid = []
        for p in (lst, route, fetch, enrich, publish):
            state = call("GET", f"/processors/{p['id']}")["component"]
            if state.get("validationStatus") != "VALID":
                invalid.append({"processor": state["name"], "errors": state.get("validationErrors")})
        if not invalid:
            break
        time.sleep(5)
    if invalid:
        log("processeurs invalides : flux créé mais non démarré", invalid=invalid)
        return 1
    call("PUT", f"/flow/process-groups/{pg}", {"id": pg, "state": "RUNNING"})
    log("flux NiFi démarré", process_group=PG_NAME)
    return 0


if __name__ == "__main__":
    sys.exit(main())
