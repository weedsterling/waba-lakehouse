"""Level 3 : résolution des propriétés NiFi (flow-as-code) et partage raw-landing batch / streaming."""
import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("provision_flow", ROOT / "nifi" / "provision_flow.py")
nifi = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nifi)

DESCRIPTORS = {
    "acks": {"name": "acks", "displayName": "Delivery Guarantee", "allowableValues": [
        {"allowableValue": {"displayName": "Best Effort", "value": "0"}},
        {"allowableValue": {"displayName": "Guarantee Replicated Delivery", "value": "all"}}]},
    "topic": {"name": "topic", "displayName": "Topic Name"},
}


def test_resolve_accepts_internal_name_label_and_value_label():
    out = nifi.resolve(DESCRIPTORS, {"Topic Name": "raw-x", "acks": "Guarantee Replicated Delivery"})
    assert out == {"topic": "raw-x", "acks": "all"}


def test_resolve_dynamic_properties_and_errors():
    assert nifi.resolve(DESCRIPTORS, {"/source_file": "${filename}"}) == {"/source_file": "${filename}"}
    assert nifi.resolve(DESCRIPTORS, {"transactions": "x"}, dynamic=("transactions",)) == {"transactions": "x"}
    with pytest.raises(SystemExit, match="propriété inconnue"):
        nifi.resolve(DESCRIPTORS, {"Kafka Brokerz": "kafka:9092"})
    with pytest.raises(SystemExit, match="invalide"):
        nifi.resolve(DESCRIPTORS, {"acks": "toujours"})


def test_topic_expression_matches_kafka_topics():
    """Le topic NiFi raw-${dataset avec _ -> -} doit exister dans kafka-init.sh."""
    topics = (ROOT / "scripts" / "kafka-init.sh").read_text()
    for ds in nifi.TRANSACTION_DATASETS:
        assert f"raw-{ds.replace('_', '-')}" in topics


def test_batch_skips_recent_files_for_nifi():
    pytest.importorskip("boto3")
    pytest.importorskip("pyspark")
    from waba_spark.common import ObjectStore

    now = datetime.now(timezone.utc)

    def obj(name: str, age: timedelta) -> dict:
        return {"Key": f"bank_transactions/CI/{name}.csv", "Size": 10, "ETag": '"e"', "LastModified": now - age}

    class Pages:
        def paginate(self, **_):
            return [{"Contents": [obj("old", timedelta(minutes=9)), obj("new", timedelta(seconds=20))]}]

    store = ObjectStore.__new__(ObjectStore)
    store.client = type("C", (), {"get_paginator": lambda self, _: Pages()})()
    assert [o.key for o in store.list_csv("raw-landing", "bank_transactions/")] == \
        ["bank_transactions/CI/new.csv", "bank_transactions/CI/old.csv"]
    assert [o.key for o in store.list_csv("raw-landing", "bank_transactions/", timedelta(minutes=5))] == \
        ["bank_transactions/CI/old.csv"]


def test_trino_kafka_descriptions_match_catalog_and_lambda_query():
    """Chaque table du catalogue kafka a sa description JSON, et la requête Lambda de l'énoncé
    trouve ses colonnes (country_code, event_time, streaming_amount_eur)."""
    import json

    lines = (ROOT / "trino" / "catalog" / "kafka.properties").read_text().splitlines()
    props = dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))
    tables = props["kafka.table-names"].split(",")
    for t in tables:
        desc = json.loads((ROOT / "trino" / "kafka" / f"{t}.json").read_text())
        assert desc["tableName"] == desc["topicName"] == t
    topics = (ROOT / "scripts" / "kafka-init.sh").read_text()
    assert all(t in topics for t in tables)
    cols = {f["name"] for f in json.loads((ROOT / "trino" / "kafka" / "silver-bank-transactions.json").read_text())
            ["message"]["fields"]}
    assert {"country_code", "event_time", "streaming_amount_eur"} <= cols
