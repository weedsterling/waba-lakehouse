"""Speed layer Job 1 : parité stricte avec le batch (mêmes rejets, même schéma Silver) sur des messages
Kafka simulés à l'identique de NiFi (JSON, toutes valeurs en texte, + source_file / ingestion_timestamp)."""
import io
import json
from datetime import datetime

import pandas as pd
import pytest

pytest.importorskip("pyspark")
from conftest import END, START  # noqa: E402,I001
from pyspark.sql import functions as F  # noqa: E402

from waba_gen.storage import df_to_csv_bytes  # noqa: E402
from waba_gen.transactions import Pools, generate_transactions  # noqa: E402
from waba_spark import silver as S  # noqa: E402
from waba_spark import streaming as ST  # noqa: E402
from waba_spark import validation as V  # noqa: E402
from waba_spark.schemas import SPECS  # noqa: E402

DATASETS = list(ST.RAW_TOPICS.values())
TOPIC_OF = {d: t for t, d in ST.RAW_TOPICS.items()}


@pytest.fixture(scope="module")
def landing(tmp_path_factory, silver):
    """Fichiers CSV avec anomalies (5 %) + messages Kafka équivalents."""
    from waba_gen.referentials import generate_referentials

    ref = generate_referentials({"customers": 4_000, "accounts": 6_500, "branches": 60, "products": 50})
    tmp, out = tmp_path_factory.mktemp("landing"), {}
    for ds in DATASETS:
        pdf = pd.concat(generate_transactions(ref, ds, ["CI", "SN", "GH"], 600, START, END, anomaly_rate=0.05,
                                              seed=7, pools=Pools(ref)).values(), ignore_index=True)
        raw = df_to_csv_bytes(pdf)
        path = tmp / f"{ds}.csv"
        path.write_bytes(raw)
        rows = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False).to_dict("records")
        msgs = [json.dumps({**r, "source_file": f"raw-landing/{ds}/x.csv",
                            "ingestion_timestamp": "2026-10-02T23:48:07.654Z"}) for r in rows]
        out[ds] = {"csv": str(path), "messages": msgs}
    return out


def kafka_frame(spark, ds, messages):
    rows = [(TOPIC_OF[ds], i % 8, i, datetime(2026, 10, 2, 23, 48, 8), m) for i, m in enumerate(messages)]
    df = spark.createDataFrame(rows, "topic string, partition int, offset long, timestamp timestamp, value string")
    return ST.kafka_events(df.withColumn("event_id", ST.event_id()))


@pytest.mark.parametrize("ds", DATASETS)
def test_same_rejects_as_batch(spark, landing, ds):
    spec = SPECS[ds]
    b_valid, b_rej = V.validate(V.read_csv(spark, [landing[ds]["csv"]], spec), spec)
    s_valid, s_rej = V.validate(ST.parse_topic(kafka_frame(spark, ds, landing[ds]["messages"]), spec), spec)
    assert s_valid.count() == b_valid.count() and s_rej.count() == b_rej.count() > 0
    reasons = lambda d: sorted(r[0] for r in d.select(F.array_join(V.REJECT_REASONS, ",")).collect())  # noqa: E731
    assert reasons(s_rej) == reasons(b_rej)


def test_event_id_never_merges_messages_without_id(spark):
    msgs = [json.dumps({"transaction_id": v}) for v in ("", " ", "T1")]
    ids = [r.event_id for r in kafka_frame(spark, "bank_transactions", msgs).collect()]
    assert len(set(ids)) == 3 and "T1" in ids


def test_invalid_json_goes_to_dlq(spark):
    spec = SPECS["bank_transactions"]
    _, rej = V.validate(ST.parse_topic(kafka_frame(spark, "bank_transactions", ["{pas du json"]), spec), spec)
    dlq = json.loads(ST.to_dlq(rej, spec, "t").first().value)
    assert "MALFORMED_ROW" in dlq["reject_reasons"] and dlq["raw_value"] == "{pas du json"
    assert dlq["source_topic"] == "raw-bank-transactions" and dlq["source_offset"] == 0


@pytest.mark.parametrize("ds", DATASETS)
def test_streaming_silver_matches_batch_schema(spark, landing, silver, ds):
    spec = SPECS[ds]
    key = {"bank_transactions": "bank", "insurance_operations": "ins",
           "mobile_money_payments": "mm", "loan_repayments": "loans"}[ds]
    amount = "amount_due_eur" if ds == "loan_repayments" else "amount_eur"
    dims = {k: silver[k] for k in ("customers", "accounts", "branches", "products")}
    valid, _ = V.validate(ST.parse_topic(kafka_frame(spark, ds, landing[ds]["messages"]), spec), spec)
    out = ST.build_silver(ds, ST.bronze_like(valid, spec, "s1"), dims, silver["fx"],
                          S.outlier_fences(silver[key], amount))
    assert out.columns == silver[key].columns
    assert out.count() == valid.dropDuplicates([spec.id_col]).count()
    assert out.filter(F.col("amount_eur" if ds != "loan_repayments" else "amount_due_eur").isNull()).count() == 0


def test_streaming_dedup_within_watermark(spark, tmp_path):
    """Bout en bout en mode streaming (source fichiers au format Kafka, trigger availableNow) :
    un même identifiant rejoué dans la fenêtre de 10 min n'est traité qu'une fois, sur plusieurs micro-lots."""
    schema = "topic string, partition int, offset long, timestamp timestamp, value string"
    src = tmp_path / "src"

    def drop(name, rows):
        spark.createDataFrame(rows, schema).coalesce(1).write.mode("append").parquet(str(src / name))

    t = datetime(2026, 10, 2, 23, 48, 0)
    drop("a", [("raw-bank-transactions", 0, 0, t, json.dumps({"transaction_id": "T1"})),
               ("raw-bank-transactions", 0, 1, t, json.dumps({"transaction_id": "T1"})),   # doublon même lot
               ("raw-mobile-money-payments", 1, 0, t, json.dumps({"payment_id": "T1"}))])  # autre topic : gardé
    seen = []
    stream = (ST.deduplicated_events(spark.readStream.schema(schema).parquet(str(src / "*")))
              .writeStream.foreachBatch(lambda df, _: seen.extend(r.event_id for r in df.collect()))
              .option("checkpointLocation", str(tmp_path / "chk")).trigger(availableNow=True).start())
    stream.awaitTermination()
    drop("b", [("raw-bank-transactions", 0, 2, t.replace(minute=52), json.dumps({"transaction_id": "T1"}))])
    stream = (ST.deduplicated_events(spark.readStream.schema(schema).parquet(str(src / "*")))
              .writeStream.foreachBatch(lambda df, _: seen.extend(r.event_id for r in df.collect()))
              .option("checkpointLocation", str(tmp_path / "chk")).trigger(availableNow=True).start())
    stream.awaitTermination()
    assert sorted(seen) == ["T1", "T1"]   # 1 par topic, le rejeu de 23:52 (2e micro-lot) est écarté
