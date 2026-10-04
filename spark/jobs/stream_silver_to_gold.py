"""Speed layer — Job 2 (Spark Structured Streaming) : Silver temps réel -> alertes Gold.

Trois requêtes dans la même application (une par type d'état) :

  burst      silver-bank-transactions -> fenêtres glissantes 5 min / 1 min par compte (état, watermark)
             -> LARGE_TXN_BURST                                  -> gold-fraud-alerts
  liquidity  silver-bank-transactions -> fenêtres glissantes 5 min / 1 min par pays
             -> sorties nettes vs dépôts                          -> gold-liquidity-alerts
  rules      silver-* (sans état)     -> UNUSUAL_COUNTRY, CLAIM_GT_3X_PREMIUM -> gold-fraud-alerts
                                      -> AML (seuil déclaratif)  -> gold-aml-events

Chaque alerte est aussi conservée dans Iceberg (gold.rt_fraud_alerts, gold.rt_aml_events,
gold.rt_liquidity_alerts) par MERGE sur un identifiant déterministe : les mises à jour des fenêtres
(mode update) et les rejeux après incident écrasent l'alerte au lieu de la dupliquer.

Le watermark (10 min) porte sur la date métier : seules les transactions récentes (flux continu du
générateur) alimentent les fenêtres ; un lot historique rejoué ne déclenche pas de fausse « rafale ».
"""
from __future__ import annotations

import os
import sys
import threading
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from waba_spark import fraud as FR
from waba_spark import iceberg
from waba_spark import streaming as ST
from waba_spark.common import build_spark, get_logger

log = get_logger("waba.stream_silver_to_gold")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CHECKPOINT = os.environ.get("STREAM_CHECKPOINT", "s3a://lakehouse/checkpoints/stream_silver_to_gold")
TRIGGER = os.environ.get("STREAM_TRIGGER", "10 seconds")
REFRESH_S = int(os.environ.get("STREAM_DIM_REFRESH_SECONDS", "1800"))
TOPIC = {"bank": "silver-bank-transactions", "mm": "silver-mobile-money", "ins": "silver-insurance-operations"}
RT = {"bank": "bank_transactions", "mm": "mobile_money_payments", "ins": "insurance_operations"}


def wait_for_silver(spark: SparkSession) -> dict:
    """Le schéma des messages silver-* est celui des tables rt_* créées par le Job 1 (source unique)."""
    while True:
        try:
            return {k: spark.table(iceberg.fq(ST.rt_table(v))).schema for k, v in RT.items()}
        except Exception as exc:  # noqa: BLE001
            log.warning("tables silver.rt_* absentes : attente du Job 1", extra={"ctx": {"error": str(exc)[:200]}})
            time.sleep(30)


class Lookups:
    """Profils clients, historique de primes et dépôts (couche Silver batch), rafraîchis périodiquement."""

    def __init__(self, spark: SparkSession):
        self.spark, self.loaded_at, self.f = spark, 0.0, {}
        self.lock = threading.Lock()          # partagé par 3 requêtes (threads) concurrentes

    def get(self) -> dict[str, DataFrame]:
        with self.lock:
            return self._get()

    def _get(self) -> dict[str, DataFrame]:
        if time.time() - self.loaded_at < REFRESH_S:
            return self.f
        for df in self.f.values():
            df.unpersist()
        t = lambda n: self.spark.table(iceberg.fq(f"silver.{n}"))  # noqa: E731
        customers = t("customers").where(F.col("entity_type") == "MOBILE_MONEY")
        self.f = {
            "profiles": FR.mm_profiles(customers, t("mobile_money_payments").select("sender_id", "sender_country")),
            "premium_hist": t("insurance_operations").where("is_premium")
                            .select("customer_id", "event_ts", "amount_eur", "is_premium"),
            "reserves": FR.liquidity_reserves(t("accounts")),
        }
        self.f = {k: v.cache() for k, v in self.f.items()}
        self.loaded_at = time.time()
        log.info("référentiels de détection chargés",
                 extra={"ctx": {"profiles": self.f["profiles"].count(), "reserves": self.f["reserves"].count()}})
        return self.f


def upsert(spark: SparkSession, df: DataFrame, table: str, key: str) -> None:
    target = iceberg.fq(table)
    df = df.dropDuplicates([key])
    if not spark.catalog.tableExists(target):
        (df.limit(0).writeTo(target).using("iceberg")
           .partitionedBy(F.col("country_code"), F.days(F.col("event_time")))
           .tableProperty("format-version", "2").tableProperty("write.merge.mode", "merge-on-read")
           .create())
    df.createOrReplaceTempView("_gold_batch")
    spark.sql(f"""MERGE INTO {target} t USING _gold_batch s ON t.{key} = s.{key}
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


def publish(df: DataFrame, topic: str, table: str, key: str, append: tuple[str, int] | None = None) -> int:
    """Iceberg d'abord (idempotent), puis Kafka. `append` = (query_id, batch_id) pour les événements
    immuables (AML) : append exactement-une-fois, sans relire la table ; sinon MERGE (alertes de
    fenêtres mises à jour en mode update)."""
    df = df.persist()
    n = df.count()
    if n:
        if append:
            iceberg.append_once(df.sparkSession, df.dropDuplicates([key]), table,
                                [F.col("country_code"), F.days(F.col("event_time"))], *append)
        else:
            upsert(df.sparkSession, df, table, key)
        (FR.to_kafka(df).write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("kafka.acks", "all").option("topic", topic).save())
    df.unpersist()
    return n


def source(spark: SparkSession, topics: list[str]) -> DataFrame:
    return (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", ",".join(topics)).option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 20000).option("failOnDataLoss", "false").load())


def _log(query: str, batch_id: int, **counts) -> None:
    if any(counts.values()):
        log.info("alertes publiées", extra={"ctx": {"query": query, "batch_id": batch_id, **counts}})


def main() -> int:
    spark = build_spark("waba-stream-silver-to-gold")
    iceberg.ensure_namespaces(spark, "gold")
    schemas, lookups = wait_for_silver(spark), Lookups(spark)

    # Tables d'alertes créées dès le démarrage (vides) : requêtes Trino / Superset valides avant la 1re alerte
    empty = {k: spark.createDataFrame([], schemas[k]) for k in TOPIC}
    part = [F.col("country_code"), F.days(F.col("event_time"))]
    iceberg.ensure_stream_table(spark, FR.large_txn_alerts(FR.large_txn_windows(empty["bank"])),
                                "gold.rt_fraud_alerts", part)
    iceberg.ensure_stream_table(spark, FR.liquidity_alerts(FR.liquidity_windows(empty["bank"]),
                                                           lookups.get()["reserves"]), "gold.rt_liquidity_alerts", part)
    iceberg.ensure_stream_table(spark, FR.aml_events(empty["bank"], empty["mm"]), "gold.rt_aml_events", part)

    bank = FR.parse_silver(source(spark, [TOPIC["bank"]]), schemas["bank"]).withWatermark("event_ts", FR.WATERMARK)

    def on_burst(df: DataFrame, batch_id: int) -> None:
        _log("burst", batch_id, fraud_alerts=publish(FR.large_txn_alerts(df), "gold-fraud-alerts",
                                                      "gold.rt_fraud_alerts", "alert_id"))

    def on_liquidity(df: DataFrame, batch_id: int) -> None:
        alerts = FR.liquidity_alerts(df, lookups.get()["reserves"])
        _log("liquidity", batch_id, liquidity_alerts=publish(alerts, "gold-liquidity-alerts",
                                                             "gold.rt_liquidity_alerts", "alert_id"))

    def on_rules(batch: DataFrame, batch_id: int) -> None:
        batch = batch.persist()
        lk = lookups.get()
        part = {k: FR.parse_silver(batch.where(F.col("topic") == TOPIC[k]), schemas[k]) for k in TOPIC}
        # Primes : historique batch + primes temps réel déjà écrites par le Job 1 + primes du micro-lot
        premiums_src = (lk["premium_hist"]
                        .unionByName(spark.table(iceberg.fq(ST.rt_table("insurance_operations")))
                                     .where("is_premium").select("customer_id", "event_ts", "amount_eur", "is_premium"))
                        .unionByName(part["ins"].select("customer_id", "event_ts", "amount_eur", "is_premium")))
        fraud = (FR.unusual_country_alerts(part["mm"], lk["profiles"])
                 .unionByName(FR.claim_alerts(part["ins"], FR.premiums_12m(premiums_src))))
        n_fraud = publish(fraud, "gold-fraud-alerts", "gold.rt_fraud_alerts", "alert_id")
        while ids.get("rules") is None:
            time.sleep(0.2)
        n_aml = publish(FR.aml_events(part["bank"], part["mm"]), "gold-aml-events", "gold.rt_aml_events", "event_id",
                        append=(ids["rules"], batch_id))
        batch.unpersist()
        _log("rules", batch_id, fraud_alerts=n_fraud, aml_events=n_aml)

    ids: dict[str, str] = {}

    def start(df: DataFrame, name: str, fn, mode: str):
        q = (df.writeStream.foreachBatch(fn).outputMode(mode).queryName(name)
                  .option("checkpointLocation", f"{CHECKPOINT}/{name}").trigger(processingTime=TRIGGER).start())
        ids[name] = str(q.id)
        return q

    start(FR.large_txn_windows(bank), "burst", on_burst, "update")
    start(FR.liquidity_windows(bank), "liquidity", on_liquidity, "update")
    start(source(spark, list(TOPIC.values())), "rules", on_rules, "append")
    log.info("détection temps réel démarrée", extra={"ctx": {"queries": ["burst", "liquidity", "rules"]}})
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
