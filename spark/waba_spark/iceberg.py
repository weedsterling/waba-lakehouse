"""Écriture Iceberg : création des tables, MERGE idempotent, tables d'audit."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .common import CATALOG
from .schemas import DatasetSpec

AUDIT_DDL = {
    "ingestion_log": """
        batch_id STRING, dataset STRING, source_file STRING, etag STRING, country_code STRING,
        rows_read BIGINT, rows_valid BIGINT, rows_rejected BIGINT, status STRING,
        error STRING, started_at TIMESTAMP, ended_at TIMESTAMP""",
    "rejected_records": """
        batch_id STRING, dataset STRING, source_file STRING, country_code STRING, record_id STRING,
        reject_reasons STRING, raw_record STRING, rejected_at TIMESTAMP""",
}


def fq(table: str) -> str:
    return f"{CATALOG}.{table}"


def ensure_namespaces(spark: SparkSession, *namespaces: str) -> None:
    for ns in namespaces:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")


def ensure_audit_tables(spark: SparkSession) -> None:
    ensure_namespaces(spark, "audit")
    for name, cols in AUDIT_DDL.items():
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {fq('audit.' + name)} ({cols})
            USING iceberg
            PARTITIONED BY (dataset, days({'started_at' if name == 'ingestion_log' else 'rejected_at'}))
            TBLPROPERTIES ('format-version'='2')""")


def ensure_table(spark: SparkSession, df: DataFrame, spec: DatasetSpec, namespace: str = "raw") -> None:
    """Création à partir du schéma validé. Partitionnement caché Iceberg :
    country_code + days(timestamp) pour les flux, country_code pour les référentiels."""
    table = fq(f"{namespace}.{spec.name}")
    if spark.catalog.tableExists(table):
        return
    parts = [F.col("country_code")] + ([F.days(F.col(spec.ts_col))] if spec.ts_col else [])
    (df.limit(0).writeTo(table).using("iceberg")
       .partitionedBy(*parts)
       .tableProperty("format-version", "2")
       .tableProperty("write.merge.mode", "merge-on-read")
       .tableProperty("write.metadata.delete-after-commit.enabled", "true")
       .tableProperty("write.metadata.previous-versions-max", "50")
       .create())


def _current_snapshot(spark: SparkSession, table: str) -> int | None:
    row = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return row[0] if row else None


def merge_idempotent(spark: SparkSession, df: DataFrame, spec: DatasetSpec, namespace: str = "raw") -> int:
    """MERGE sur la clé métier (+ country_code pour l'élagage de partitions).

    * Transactions (événements immuables) : INSERT si absent, ignoré sinon.
    * Référentiels : upsert (SCD1) pour refléter la dernière version.
    Retourne le nombre de lignes insérées (métrique du snapshot Iceberg)."""
    table = fq(f"{namespace}.{spec.name}")
    view = f"src_{spec.name}"
    df.createOrReplaceTempView(view)
    before = _current_snapshot(spark, table)
    matched = "WHEN MATCHED THEN UPDATE SET *" if spec.is_referential else ""
    spark.sql(f"""
        MERGE INTO {table} t
        USING {view} s
        ON t.{spec.id_col} = s.{spec.id_col} AND t.country_code = s.country_code
        {matched}
        WHEN NOT MATCHED THEN INSERT *""")
    after = _current_snapshot(spark, table)
    if after is None or after == before:
        return 0
    added = spark.sql(f"SELECT summary['added-records'] FROM {table}.snapshots "
                      f"WHERE snapshot_id = {after}").first()[0]
    return int(added or 0)


def append(spark: SparkSession, df: DataFrame, table: str) -> None:
    df.writeTo(fq(table)).append()


def overwrite_partitions(spark: SparkSession, df: DataFrame, table: str, partition_by: list,
                         cluster_by: list[str] | None = None) -> None:
    """Écriture idempotente par remplacement dynamique des partitions présentes dans `df`.

    Rejouer le job pour un pays réécrit exactement ses partitions (backfill sélectif),
    sans toucher aux autres pays. Crée la table au premier passage.

    `cluster_by` : regroupe puis trie les lignes par partition avant écriture, pour que
    chaque tâche n'ouvre qu'un fichier Parquet à la fois (mémoire bornée)."""
    target = fq(table)
    if cluster_by:
        df = df.repartition(cluster_by[0]).sortWithinPartitions(*cluster_by)
    if not spark.catalog.tableExists(target):
        (df.writeTo(target).using("iceberg").partitionedBy(*partition_by)
           .tableProperty("format-version", "2")
           .tableProperty("write.spark.fanout.enabled", "false")
           .tableProperty("write.metadata.delete-after-commit.enabled", "true")
           .tableProperty("write.metadata.previous-versions-max", "50")
           .create())
        return
    df.writeTo(target).overwritePartitions()
