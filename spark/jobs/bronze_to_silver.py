"""Job PySpark Level 2 : couche Bronze -> couche Silver (Iceberg).

  * référentiels : customers, accounts (+ soldes EUR), branches, products
  * faits        : bank_transactions, insurance_operations, mobile_money_payments, loan_repayments
  * silver.fx_rates : table de change quotidienne utilisée pour la conversion EUR
  * audit.dq_metrics : indicateurs de qualité par pays (orphelins, valeurs aberrantes)

Idempotence : remplacement dynamique des partitions (country_code[, jour]) des pays traités.
Exemple : spark-submit jobs/bronze_to_silver.py --countries CI,SN
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta

from pyspark.sql import functions as F

from waba_spark import iceberg
from waba_spark import silver as S
from waba_spark.common import build_spark, get_logger
from waba_spark.schemas import COUNTRIES

log = get_logger("waba.bronze_to_silver")



# Spécifications de partitionnement construites à la demande : en PySpark, F.col() exige une
# SparkSession active et ne peut donc pas être évalué à l'import du module.
def fact_partition() -> list:
    # Partition mensuelle : ~24 partitions au lieu de ~730 partitions journalières pour quelques
    # milliers de lignes -> fichiers de taille raisonnable et beaucoup moins d'écrivains Parquet
    # ouverts simultanément (cause des OutOfMemoryError). L'élagage par date reste automatique
    # grâce au partitionnement caché d'Iceberg (un filtre sur txn_date cible le bon mois).
    return [F.col("country_code"), F.months(F.col("event_ts"))]


def dim_partition() -> list:
    return [F.col("country_code")]


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--countries", default="all")
    p.add_argument("--source-namespace", default="bronze")
    p.add_argument("--target-namespace", default="silver")
    a = p.parse_args(argv)
    a.countries = COUNTRIES if a.countries == "all" else [c.strip().upper() for c in a.countries.split(",")]
    if set(a.countries) - set(COUNTRIES):
        p.error(f"pays inconnus: {sorted(set(a.countries) - set(COUNTRIES))}")
    return a


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    spark = build_spark("waba-bronze-to-silver")
    src, tgt = args.source_namespace, args.target_namespace
    iceberg.ensure_namespaces(spark, tgt, "audit")

    def read(name: str):
        return spark.table(iceberg.fq(f"{src}.{name}")).filter(F.col("country_code").isin(args.countries))

    # Table de change couvrant toute la période des faits (+ aujourd'hui pour les soldes)
    bounds = (read("bank_transactions").select("timestamp")
              .unionByName(read("mobile_money_payments").select("timestamp"))
              .unionByName(read("insurance_operations").select("timestamp"))
              .unionByName(read("loan_repayments").select("timestamp"))
              .agg(F.min("timestamp").alias("lo"), F.max("timestamp").alias("hi")).first())
    today = date.today()
    lo = (bounds["lo"].date() if bounds["lo"] else today) - timedelta(days=1)
    hi = max(bounds["hi"].date() if bounds["hi"] else today, today)
    fx = S.fx_rates(spark, lo.isoformat(), hi.isoformat()).cache()
    iceberg.overwrite_partitions(spark, fx, f"{tgt}.fx_rates", [F.col("currency")])

    results, dq = [], []

    def publish(name: str, df, partition, flags: list[str] | None = None, release: bool = False) -> None:
        t0 = time.time()
        df = df.cache()
        cluster = ["country_code", "event_ts"] if "event_ts" in df.columns else ["country_code"]
        iceberg.overwrite_partitions(spark, df, f"{tgt}.{name}", partition, cluster_by=cluster)
        rows = df.count()
        if flags:  # métriques collectées tout de suite (quelques lignes) : pas de recalcul en fin de job
            dq.extend(S.quality_metrics(df, name, flags).collect())
        if release:
            df.unpersist()
        results.append({"table": f"{tgt}.{name}", "rows": rows, "duration_s": round(time.time() - t0, 1)})
        log.info("table silver publiée", extra={"ctx": results[-1]})

    customers = S.build_customers(read("customers")).cache()
    publish("customers", customers, dim_partition())
    branches = S.build_branches(read("branches")).cache()
    publish("branches", branches, dim_partition())
    products = S.build_products(read("products")).cache()
    publish("products", products, dim_partition())
    accounts = S.build_accounts(read("accounts"), customers, fx, today.isoformat()).cache()
    publish("accounts", accounts, dim_partition(), ["is_orphan_customer"])

    publish("bank_transactions",
            S.build_bank_transactions(read("bank_transactions"), accounts, branches, fx),
            fact_partition(), ["is_orphan_account", "is_orphan_branch", "is_outlier"], release=True)
    publish("insurance_operations",
            S.build_insurance_operations(read("insurance_operations"), customers, products, fx),
            fact_partition(), ["is_orphan_customer", "is_outlier"], release=True)
    publish("mobile_money_payments",
            S.build_mobile_money(read("mobile_money_payments"), customers, fx),
            fact_partition(), ["is_orphan_sender", "is_orphan_receiver", "is_outlier"], release=True)
    publish("loan_repayments",
            S.build_loan_repayments(read("loan_repayments"), accounts, products, fx),
            fact_partition(), ["is_orphan_account", "is_outlier"], release=True)
    for cached in (customers, branches, products, accounts):
        cached.unpersist()

    if dq:
        metrics = spark.createDataFrame(dq)
        target = iceberg.fq("audit.dq_metrics")
        if spark.catalog.tableExists(target):
            metrics.writeTo(target).append()
        else:
            metrics.writeTo(target).using("iceberg").partitionedBy(F.col("dataset")).create()

    log.info("silver terminé", extra={"ctx": {"countries": args.countries, "results": results}})
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
