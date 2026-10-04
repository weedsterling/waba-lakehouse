"""Speed layer — Job 1 (Spark Structured Streaming) : Kafka raw-* -> Silver temps réel.

  * lecture des 4 topics raw-* (JSON publiés par NiFi) ;
  * dédoublonnage par identifiant métier dans une fenêtre de 10 min (état Spark, watermark) ;
  * micro-lot (foreachBatch) : typage + validation (règles du Level 1) -> DLQ dlq-financial-events,
    puis transformations Silver du Level 2 (EUR, enrichissement référentiels, is_outlier) ;
  * double écriture : topics silver-* (Kafka) ET tables Iceberg silver.rt_* (MERGE idempotent).

Le watermark porte sur l'horodatage Kafka (arrivée) et non sur la date métier : le générateur produit
des dates métier étalées sur plusieurs mois, qui seraient toutes considérées « en retard ».
Reprise après incident : checkpoint (offsets + état de dédoublonnage) sur MinIO ; un micro-lot rejoué
ne crée pas de doublon dans Iceberg (MERGE) et les consommateurs Kafka dédoublonnent par identifiant.
"""
from __future__ import annotations

import os
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from waba_spark import iceberg
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

    def _t(self, name: str) -> DataFrame:
        return self.spark.table(iceberg.fq(f"silver.{name}"))

    def get(self) -> dict[str, DataFrame]:
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


def merge_rt(spark: SparkSession, df: DataFrame, dataset: str) -> None:
    table, key = iceberg.fq(ST.rt_table(dataset)), SPECS[dataset].id_col
    if not spark.catalog.tableExists(table):
        (df.limit(0).writeTo(table).using("iceberg")
           .partitionedBy(F.col("country_code"), F.days(F.col("event_ts")))
           .tableProperty("format-version", "2").tableProperty("write.merge.mode", "merge-on-read")
           .create())
    df.createOrReplaceTempView("_rt_batch")
    spark.sql(f"""MERGE INTO {table} t USING _rt_batch s
                  ON t.{key} = s.{key} AND t.country_code = s.country_code
                  WHEN NOT MATCHED THEN INSERT *""")


def to_topic(df: DataFrame, topic: str) -> None:
    (df.write.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("kafka.acks", "all").option("kafka.compression.type", "snappy")
       .option("topic", topic).save())


def process_batch(ref: Reference):
    def _run(batch: DataFrame, batch_id: int) -> None:
        spark, t0 = batch.sparkSession, time.time()
        batch = batch.persist()
        tag, stats = f"stream-{batch_id}", {}
        dims = ref.get()
        for topic, dataset in ST.RAW_TOPICS.items():
            spec = SPECS[dataset]
            parsed = ST.parse_topic(batch.filter(F.col("_topic") == topic), spec).persist()
            n = parsed.count()
            if n == 0:
                parsed.unpersist()
                continue
            valid, rejected = V.validate(parsed, spec)
            n_dlq = rejected.count()
            if n_dlq:
                to_topic(ST.to_dlq(rejected, spec, tag), ST.DLQ_TOPIC)
            silver = ST.build_silver(dataset, ST.bronze_like(valid, spec, tag), dims, dims["fx"],
                                     dims[f"fences_{dataset}"]).persist()
            merge_rt(spark, silver, dataset)
            if dataset in ST.SILVER_TOPICS:
                to_topic(ST.to_kafka(silver), ST.SILVER_TOPICS[dataset])
            stats[dataset] = {"read": n, "dlq": n_dlq, "silver": silver.count(),
                              "max_latency_s": parsed.agg(ST.latency_seconds()).first()[0]}
            silver.unpersist()
            parsed.unpersist()
        batch.unpersist()
        if stats:
            log.info("micro-lot traité", extra={"ctx": {"batch_id": batch_id, "duration_s": round(time.time() - t0, 1),
                                                        "datasets": stats}})
    return _run


def main() -> int:
    spark = build_spark("waba-stream-raw-to-silver")
    iceberg.ensure_namespaces(spark, "silver")
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", ",".join(ST.RAW_TOPICS))
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 20000)        # borne la taille d'un micro-lot (back-pressure)
           .option("failOnDataLoss", "false")
           .load())
    query = (ST.deduplicated_events(raw).writeStream.foreachBatch(process_batch(Reference(spark)))
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime=TRIGGER)
             .queryName("raw_to_silver")
             .start())
    log.info("streaming démarré", extra={"ctx": {"topics": list(ST.RAW_TOPICS), "checkpoint": CHECKPOINT}})
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
