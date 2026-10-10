"""Speed layer — Job 1 (Spark Structured Streaming) : Kafka raw-* -> Silver temps réel.

  * lecture des 4 topics raw-* (JSON publiés par NiFi) ;
  * dédoublonnage par identifiant métier dans une fenêtre de 10 min (état Spark, watermark) ;
  * micro-lot (foreachBatch) : typage + validation (règles du Level 1) -> DLQ dlq-financial-events,
    puis transformations Silver du Level 2 (EUR, enrichissement référentiels, is_outlier) ;
  * double écriture : tables Iceberg silver.rt_* (append exactement-une-fois, cf. iceberg.append_once)
    ET topics silver-* (Kafka) ; les 4 types de transactions sont traités en parallèle.

Le watermark porte sur l'horodatage Kafka (arrivée) et non sur la date métier : le générateur produit
des dates métier étalées sur plusieurs mois, qui seraient toutes considérées « en retard ».
Reprise après incident : checkpoint (offsets + état de dédoublonnage) sur MinIO ; un micro-lot rejoué
est reconnu par son numéro (propriété du snapshot Iceberg) et n'est pas réécrit ; côté Kafka, les
consommateurs dédoublonnent par identifiant.
"""
from __future__ import annotations

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from waba_spark import iceberg, monitoring
from waba_spark import silver as S
from waba_spark import streaming as ST
from waba_spark import validation as V
from waba_spark.common import build_spark, get_logger
from waba_spark.schemas import SPECS

log = get_logger("waba.stream_raw_to_silver")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CHECKPOINT = os.environ.get("STREAM_CHECKPOINT", "s3a://lakehouse/checkpoints/stream_raw_to_silver")
TRIGGER = os.environ.get("STREAM_TRIGGER", "10 seconds")
DIM_REFRESH_S = int(os.environ.get("STREAM_DIM_REFRESH_SECONDS", "1800"))
OUTLIER_AMOUNT = {"loan_repayments": "amount_due_eur"}


class Reference:
    """Référentiels Silver (batch) + barrières d'outliers, en cache et rafraîchis périodiquement."""

    def __init__(self, spark: SparkSession):
        self.spark, self.loaded_at, self.frames = spark, 0.0, {}
        self.lock = threading.Lock()

    def _t(self, name: str) -> DataFrame:
        return self.spark.table(iceberg.fq(f"silver.{name}"))

    def get(self) -> dict[str, DataFrame]:
        with self.lock:
            return self._get()

    def _get(self) -> dict[str, DataFrame]:
        if time.time() - self.loaded_at < DIM_REFRESH_S:
            return self.frames
        for df in self.frames.values():
            df.unpersist()
        f = {
            "customers": self._t("customers").select("customer_id", "segment"),
            "accounts": self._t("accounts").select("account_id", "customer_id", "account_type",
                                                   "customer_segment", "balance"),
            "branches": self._t("branches").select("branch_id", "city", "region"),
            "products": self._t("products").select("country_code", "product_code", "product_id", "interest_rate"),
            "fx": S.fx_rates(self.spark, "2020-01-01", "2030-12-31"),
        }
        for ds in ST.RAW_TOPICS.values():
            amount = OUTLIER_AMOUNT.get(ds, "amount_eur")
            f[f"fences_{ds}"] = S.outlier_fences(self._t(ds).select("country_code", amount), amount)
        self.frames = {k: v.cache() for k, v in f.items()}
        self.loaded_at = time.time()
        log.info("référentiels chargés", extra={"ctx": {"accounts": self.frames["accounts"].count()}})
        return self.frames


def rt_partition() -> list:
    return [F.col("country_code"), F.days(F.col("event_ts"))]


def to_topic(df: DataFrame, topic: str) -> None:
    (df.write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("kafka.acks", "all").option("kafka.compression.type", "snappy")
       .option("topic", topic).save())


def process_dataset(spark: SparkSession, batch: DataFrame, topic: str, dataset: str, dims: dict,
                    query_id: str, batch_id: int) -> dict | None:
    """Un type de transaction d'un micro-lot : validation -> DLQ, Silver -> Iceberg + Kafka."""
    spec, tag, t = SPECS[dataset], f"stream-{batch_id}", {}
    t0 = time.time()
    parsed = ST.parse_topic(batch.filter(F.col("_topic") == topic), spec).persist()
    n, latency = parsed.agg(F.count("*"), ST.latency_seconds()).first()
    if n == 0:
        parsed.unpersist()
        return None
    valid, rejected = V.validate(parsed, spec)
    rejected = rejected.persist()
    n_dlq = rejected.count()
    if n_dlq:
        to_topic(ST.to_dlq(rejected, spec, tag), ST.DLQ_TOPIC)
    t["validate_s"] = round(time.time() - t0, 1)
    t1 = time.time()
    silver = ST.build_silver(dataset, ST.bronze_like(valid, spec, tag), dims, dims["fx"],
                             dims[f"fences_{dataset}"]).persist()
    n_silver = silver.count()
    t["silver_s"] = round(time.time() - t1, 1)
    t2 = time.time()
    # Ordre important : Iceberg d'abord (idempotent), puis Kafka (au moins une fois, consommateurs
    # dédoublonnant par identifiant) -> un rejeu ne crée jamais de doublon dans le lakehouse.
    iceberg.append_once(spark, silver, ST.rt_table(dataset), rt_partition(), query_id, batch_id)
    t["iceberg_s"] = round(time.time() - t2, 1)
    t3 = time.time()
    if dataset in ST.SILVER_TOPICS:
        to_topic(ST.to_kafka(silver), ST.SILVER_TOPICS[dataset])
    t["kafka_s"] = round(time.time() - t3, 1)
    for df in (silver, rejected, parsed):
        df.unpersist()
    return {"read": n, "dlq": n_dlq, "silver": n_silver, "max_latency_s": latency, **t}


def process_batch(ref: Reference, run: dict):
    def _run(batch: DataFrame, batch_id: int) -> None:
        while run.get("query_id") is None:      # identifiant connu juste après start()
            time.sleep(0.2)
        spark, t0 = batch.sparkSession, time.time()
        batch = batch.persist()
        dims = ref.get()
        # Les 4 types de transactions sont indépendants : traités en parallèle (jobs Spark concurrents)
        with ThreadPoolExecutor(max_workers=len(ST.RAW_TOPICS)) as pool:
            futures = {ds: pool.submit(process_dataset, spark, batch, topic, ds, dims, run["query_id"], batch_id)
                       for topic, ds in ST.RAW_TOPICS.items()}
            stats = {ds: f.result() for ds, f in futures.items()}
        batch.unpersist()
        stats = {k: v for k, v in stats.items() if v}
        if stats:
            log.info("micro-lot traité", extra={"ctx": {"batch_id": batch_id, "duration_s": round(time.time() - t0, 1),
                                                        "datasets": stats}})
    return _run


REFERENCE_TABLES = ["customers", "accounts", "branches", "products", *ST.RAW_TOPICS.values()]


def wait_for_reference(spark: SparkSession) -> None:
    """Le flux s'appuie sur la couche Silver batch (référentiels + historique pour les barrières
    d'outliers). Sur une plateforme neuve, on attend que le batch l'ait construite plutôt que d'échouer
    en boucle ; les messages Kafka restent en attente (offsets non consommés), rien n'est perdu."""
    while True:
        missing = [t for t in REFERENCE_TABLES if not spark.catalog.tableExists(iceberg.fq(f"silver.{t}"))]
        if not missing:
            return
        log.warning("couche Silver batch incomplète : attente", extra={"ctx": {"missing": missing}})
        time.sleep(30)


def main() -> int:
    spark = build_spark("waba-stream-raw-to-silver")
    iceberg.ensure_namespaces(spark, "silver")
    wait_for_reference(spark)
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", ",".join(ST.RAW_TOPICS))
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 20000)        # borne la taille d'un micro-lot (back-pressure)
           .option("failOnDataLoss", "false")
           .load())
    run: dict = {}
    monitoring.install(spark, BOOTSTRAP)        # lag visible dans Kafka (supervision Grafana)
    query = (ST.deduplicated_events(raw).writeStream.foreachBatch(process_batch(Reference(spark), run))
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime=TRIGGER)
             .queryName("raw_to_silver")
             .start())
    run["query_id"] = str(query.id)       # stable tant que le checkpoint est conservé
    log.info("streaming démarré", extra={"ctx": {"topics": list(ST.RAW_TOPICS), "checkpoint": CHECKPOINT}})
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
