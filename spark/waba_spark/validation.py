"""Lecture CSV à schéma explicite + validation + masquage PII.

Toutes les transformations utilisent des fonctions Spark natives (aucune UDF
Python) : exécution vectorisée dans la JVM, pas de dépendance côté executors.
"""
from __future__ import annotations

import os
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .schemas import CORRUPT_COL, COUNTRIES, CURRENCY_BY_COUNTRY, ENTITY_TYPES, DatasetSpec

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"
REJECT_REASONS = "_reject_reasons"


def read_csv(spark: SparkSession, paths: list[str], spec: DatasetSpec) -> DataFrame:
    """Lecture PERMISSIVE : une ligne dont un champ ne respecte pas le type attendu
    (montant 'N/A', timestamp illisible…) est conservée avec `_corrupt_record`
    renseigné, afin d'être tracée dans audit.rejected_records plutôt que perdue."""
    schema = StructType(spec.schema.fields + [StructField(CORRUPT_COL, StringType(), True)])
    df = (spark.read
          .option("header", "true")
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", CORRUPT_COL)
          .option("timestampFormat", TS_FORMAT)
          .option("dateFormat", "yyyy-MM-dd")
          .option("enforceSchema", "false")   # vérifie l'en-tête vs schéma attendu
          .schema(schema)
          .csv(paths)
          .withColumn("_source_file", F.input_file_name()))
    # Spark interdit les requêtes ne portant que sur _corrupt_record sans matérialisation
    return df.cache()


def _rules(spec: DatasetSpec) -> list[tuple[Column, str]]:
    """Liste (condition_de_rejet, motif)."""
    rules: list[tuple[Column, str]] = [(F.col(CORRUPT_COL).isNotNull(), "MALFORMED_ROW")]
    for c in spec.required:
        rules.append((F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), f"MISSING_{c.upper()}"))
    rules.append((~F.col("country_code").isin(COUNTRIES), "INVALID_COUNTRY_CODE"))
    if "entity_type" not in spec.enums:
        rules.append((~F.col("entity_type").isin(ENTITY_TYPES), "INVALID_ENTITY_TYPE"))
    for c, allowed in spec.enums.items():
        rules.append((F.col(c).isNotNull() & ~F.col(c).isin(allowed), f"INVALID_{c.upper()}"))
    for c in spec.non_negative:
        rules.append((F.col(c) < 0, f"NEGATIVE_{c.upper()}"))
    if spec.check_currency:
        expected = F.create_map(*[F.lit(x) for kv in CURRENCY_BY_COUNTRY.items() for x in kv])
        rules.append((F.col("currency") != expected[F.col("country_code")], "CURRENCY_COUNTRY_MISMATCH"))
    return rules


def validate(df: DataFrame, spec: DatasetSpec) -> tuple[DataFrame, DataFrame]:
    """Retourne (valides, rejets). Chaque rejet porte la liste de TOUS ses motifs."""
    reasons = F.array_compact(F.array(*[F.when(cond, F.lit(msg)) for cond, msg in _rules(spec)]))
    tagged = df.withColumn(REJECT_REASONS, reasons)
    is_valid = F.size(F.col(REJECT_REASONS)) == 0
    valid = tagged.filter(is_valid).drop(REJECT_REASONS, CORRUPT_COL)
    rejected = tagged.filter(~is_valid)
    return valid, rejected


def deduplicate(df: DataFrame, spec: DatasetSpec) -> DataFrame:
    """Dédoublonnage intra-lot sur la clé métier (le MERGE gère l'inter-lots)."""
    return df.dropDuplicates([spec.id_col])


def mask_pii(df: DataFrame, spec: DatasetSpec, salt: str | None = None) -> DataFrame:
    """Remplace chaque colonne PII par :
       <col>_masked : 4 premiers + 4 derniers caractères visibles (affichage métier)
       <col>_hash   : SHA-256 salé (pseudonyme stable, permet les jointures)
    La valeur en clair n'est jamais écrite dans le lakehouse."""
    if not spec.pii_columns:
        return df
    salt = salt if salt is not None else os.environ.get("PII_HASH_SALT", "")
    if not salt:
        raise RuntimeError("PII_HASH_SALT doit être défini (variable d'environnement)")
    for c in spec.pii_columns:
        clean = F.regexp_replace(F.col(c), r"\s", "")
        df = (df.withColumn(f"{c}_masked",
                            F.concat(F.substring(clean, 1, 4), F.lit("****"), F.substring(clean, -4, 4)))
                .withColumn(f"{c}_hash", F.sha2(F.concat(F.lit(salt), clean), 256))
                .drop(c))
    return df


def add_technical_columns(df: DataFrame, batch_id: str) -> DataFrame:
    return (df.withColumn("_ingestion_ts", F.current_timestamp())
              .withColumn("_batch_id", F.lit(batch_id)))


def build_rejects(rejected: DataFrame, spec: DatasetSpec, batch_id: str) -> DataFrame:
    """Format homogène pour audit.rejected_records (toutes sources confondues)."""
    payload_cols = [f.name for f in spec.schema.fields]
    return rejected.select(
        F.lit(batch_id).alias("batch_id"),
        F.lit(spec.name).alias("dataset"),
        F.col("_source_file").alias("source_file"),
        F.col("country_code"),
        F.col(spec.id_col).cast("string").alias("record_id"),
        F.array_join(F.col(REJECT_REASONS), ",").alias("reject_reasons"),
        F.coalesce(F.col(CORRUPT_COL), F.to_json(F.struct(*[c for c in payload_cols if c not in spec.pii_columns])))
         .alias("raw_record"),
        F.current_timestamp().alias("rejected_at"),
    )


def file_stats(df: DataFrame, valid: DataFrame, rejected: DataFrame) -> dict[str, dict[str, int]]:
    """Comptages par fichier source pour audit.ingestion_log."""
    def _counts(d: DataFrame) -> dict[str, int]:
        return {r["_source_file"]: r["n"] for r in d.groupBy("_source_file").agg(F.count("*").alias("n")).collect()}
    read_, ok, ko = _counts(df), _counts(valid), _counts(rejected)
    return {f: {"rows_read": read_.get(f, 0), "rows_valid": ok.get(f, 0), "rows_rejected": ko.get(f, 0)}
            for f in read_}


def union_all(dfs: list[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
