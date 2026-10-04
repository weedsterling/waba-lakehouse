"""Speed layer — Job 1 : topics raw-* (JSON NiFi) -> Silver temps réel (fonctions pures, testables sans Kafka).

Principe : chaque micro-lot (foreachBatch) est traité comme un petit lot batch, ce qui permet de
RÉUTILISER À L'IDENTIQUE les contrats de données (schemas.py), la validation (validation.py) et les
transformations Silver (silver.py) du Level 2 : une seule définition des règles métier pour les
deux couches de l'architecture Lambda.

  raw JSON (tout en texte, publié par NiFi)
    -> typage selon le schéma explicite (échec de conversion = enregistrement malformé)
    -> validation (mêmes motifs de rejet que le batch) -> DLQ dlq-financial-events
    -> masquage PII / colonnes techniques -> transformations Silver (EUR, enrichissement, is_outlier)
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from . import silver as S
from . import validation as V
from .schemas import CORRUPT_COL, SPECS, DatasetSpec

RAW_TOPICS = {
    "raw-bank-transactions": "bank_transactions",
    "raw-insurance-operations": "insurance_operations",
    "raw-mobile-money-payments": "mobile_money_payments",
    "raw-loan-repayments": "loan_repayments",
}
# Topics Silver imposés par l'énoncé (les remboursements de prêts ne vont que dans Iceberg)
SILVER_TOPICS = {
    "bank_transactions": "silver-bank-transactions",
    "insurance_operations": "silver-insurance-operations",
    "mobile_money_payments": "silver-mobile-money",
}
DLQ_TOPIC = "dlq-financial-events"
KAFKA_META = ["_topic", "_partition", "_offset", "_kafka_ts"]


def rt_table(dataset: str) -> str:
    """Tables Iceberg du speed layer, distinctes des tables batch : le batch reste la source de vérité
    (il réécrit ses partitions), le temps réel ajoute sans jamais entrer en conflit d'écriture avec lui."""
    return f"silver.rt_{dataset}"


def _blank_to_null(c: str) -> Column:
    return F.when(F.trim(F.col(c)) != "", F.col(c))


def event_id() -> Column:
    """Clé de dédoublonnage par topic. Identifiant vide -> identité Kafka (topic-partition-offset) :
    les messages sans identifiant ne sont jamais fusionnés entre eux et arrivent intacts à la DLQ."""
    ids = [F.get_json_object("value", f"$.{SPECS[d].id_col}") for d in RAW_TOPICS.values()]
    clean = [F.when(F.trim(i) != "", i) for i in ids]
    return F.coalesce(*clean, F.concat_ws("-", "topic", F.col("partition").cast("string"),
                                          F.col("offset").cast("string")))


def kafka_events(raw: DataFrame) -> DataFrame:
    """Colonnes utiles d'un DataFrame Kafka (streaming ou batch)."""
    return raw.select(F.col("topic").alias("_topic"), F.col("partition").alias("_partition"),
                      F.col("offset").alias("_offset"), F.col("timestamp").alias("_kafka_ts"),
                      F.col("value").cast("string").alias("value"), "event_id")


def deduplicated_events(raw: DataFrame, window: str = "10 minutes") -> DataFrame:
    """Flux Kafka -> événements dédoublonnés par (topic, identifiant) dans une fenêtre de 10 min.

    Le watermark porte sur l'horodatage Kafka (arrivée) et non sur la date métier : le générateur produit
    des dates métier étalées sur plusieurs mois, qui seraient toutes jugées « en retard » et perdues."""
    events = kafka_events(raw.withColumn("value", F.col("value").cast("string")).withColumn("event_id", event_id()))
    return events.withWatermark("_kafka_ts", window).dropDuplicatesWithinWatermark(["_topic", "event_id"])


def _typed(c: str, dtype) -> Column:
    raw = _blank_to_null(c)
    if isinstance(dtype, TimestampType):
        return F.to_timestamp(raw, V.TS_FORMAT)
    if isinstance(dtype, DateType):
        return F.to_date(raw, "yyyy-MM-dd")
    if isinstance(dtype, DoubleType):
        return raw.cast("double")
    if isinstance(dtype, IntegerType):
        return raw.cast("double").cast("int")
    return raw


def parse_topic(events: DataFrame, spec: DatasetSpec) -> DataFrame:
    """JSON texte -> DataFrame typé au format « Bronze ». Toute valeur non convertible (montant 'N/A',
    date illisible) ou tout JSON invalide renseigne `_corrupt_record`, comme la lecture CSV PERMISSIVE."""
    text_schema = StructType([StructField(f.name, StringType()) for f in spec.schema.fields]
                             + [StructField("source_file", StringType()),
                                StructField("ingestion_timestamp", StringType())])
    j = events.withColumn("_j", F.from_json("value", text_schema))
    cols = [F.col(f"_j.{f.name}").alias(f.name) for f in text_schema.fields]
    # from_json (PERMISSIVE) renvoie une structure vide pour un JSON invalide : on teste la syntaxe à part
    bad_json = F.col("_j").isNull() | F.get_json_object("value", "$").isNull() | ~F.trim("value").startswith("{")
    j = j.select(*KAFKA_META, "value", bad_json.alias("_bad_json"), *cols)
    typed = [_typed(f.name, f.dataType).alias(f.name) for f in spec.schema.fields]
    failed = [(_blank_to_null(f.name).isNotNull() & _typed(f.name, f.dataType).isNull())
              for f in spec.schema.fields if not isinstance(f.dataType, StringType)]
    corrupt = F.col("_bad_json")
    for cond in failed:
        corrupt = corrupt | cond
    return j.select(
        *KAFKA_META, *typed,
        F.when(corrupt, F.col("value")).alias(CORRUPT_COL),
        F.coalesce(F.col("source_file"), F.col("_topic")).alias("_source_file"),
        F.to_timestamp("ingestion_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSX").alias("_nifi_ts"))


def to_dlq(rejected: DataFrame, spec: DatasetSpec, batch_id: str) -> DataFrame:
    """Message DLQ auto-portant : origine Kafka exacte (rejouable), motifs, message brut."""
    payload = F.struct(
        F.col("_topic").alias("source_topic"), F.col("_partition").alias("source_partition"),
        F.col("_offset").alias("source_offset"), F.col("_kafka_ts").alias("source_timestamp"),
        F.lit(spec.name).alias("dataset"), F.col(spec.id_col).cast("string").alias("record_id"),
        "country_code", F.col(V.REJECT_REASONS).alias("reject_reasons"),
        F.coalesce(F.col(CORRUPT_COL), F.to_json(F.struct(*[f.name for f in spec.schema.fields])))
         .alias("raw_value"),
        F.lit(batch_id).alias("stream_batch_id"), F.current_timestamp().alias("detected_at"))
    return rejected.select(F.col("country_code").alias("key"), F.to_json(payload).alias("value"))


def to_kafka(df: DataFrame) -> DataFrame:
    """Une ligne Silver -> un message JSON, clé = pays (ordre conservé par pays)."""
    return df.select(F.col("country_code").alias("key"), F.to_json(F.struct(*df.columns)).alias("value"))


def bronze_like(valid: DataFrame, spec: DatasetSpec, batch_id: str) -> DataFrame:
    """Valides dédoublonnés -> format attendu par les transformations Silver du Level 2."""
    cols = [f.name for f in spec.schema.fields] + ["_source_file"]
    df = V.deduplicate(valid.select(*cols), spec)
    return V.add_technical_columns(V.mask_pii(df, spec), batch_id)


def build_silver(dataset: str, bronze: DataFrame, dims: dict[str, DataFrame], fx: DataFrame,
                 fences: DataFrame | None) -> DataFrame:
    if dataset == "bank_transactions":
        return S.build_bank_transactions(bronze, dims["accounts"], dims["branches"], fx, fences=fences)
    if dataset == "insurance_operations":
        return S.build_insurance_operations(bronze, dims["customers"], dims["products"], fx, fences=fences)
    if dataset == "mobile_money_payments":
        return S.build_mobile_money(bronze, dims["customers"], fx, fences=fences)
    if dataset == "loan_repayments":
        return S.build_loan_repayments(bronze, dims["accounts"], dims["products"], fx, fences=fences)
    raise ValueError(dataset)


def latency_seconds() -> Column:
    """Latence maximale NiFi -> Spark du micro-lot, en secondes (supervision, Level 4 : Grafana)."""
    return F.max(F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("_nifi_ts"))
