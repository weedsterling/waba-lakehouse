"""Job PySpark Level 2 : couche Silver -> couche Gold (7 KPIs métier, Iceberg).

  gold.daily_transaction_volume   volume journalier, tous flux
  gold.npl_ratio_by_country       créances en souffrance / encours (seuil BCEAO 5 %)
  gold.customer_arpu_monthly      (commissions + intérêts) / clients actifs
  gold.loss_ratio_by_product      sinistres payés / primes (seuil CIMA 70 %)
  gold.claims_processing_time     délai moyen de traitement des sinistres (jours ouvrés)
  gold.mobile_money_daily_flow    volume, montant, taux d'échec, utilisateurs actifs
  gold.cross_border_transfers     corridors transfrontaliers, évolution hebdomadaire

Recalcul complet des pays demandés puis remplacement dynamique de leurs partitions
(country_code x mois) : rejouable, et backfill sélectif par pays.
Exemple : spark-submit jobs/silver_to_gold.py --countries CI,SN
"""
from __future__ import annotations

import argparse
import sys
import time

from pyspark.sql import functions as F

from waba_spark import gold as G
from waba_spark import iceberg
from waba_spark.common import build_spark, get_logger
from waba_spark.schemas import COUNTRIES

log = get_logger("waba.silver_to_gold")

# table Gold -> colonne de temps de partitionnement (country_code x mois)
TIME_COLUMN = {
    "daily_transaction_volume": "txn_date",
    "npl_ratio_by_country": "report_month",
    "customer_arpu_monthly": "report_month",
    "loss_ratio_by_product": "report_month",
    "claims_processing_time": "report_month",
    "mobile_money_daily_flow": "txn_date",
    "cross_border_transfers": "txn_week",
}


def partition_for(table: str) -> list:
    """Construit à la demande (F.col exige une SparkSession active)."""
    return [F.col("country_code"), F.months(F.col(TIME_COLUMN[table]))]


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--countries", default="all")
    p.add_argument("--source-namespace", default="silver")
    p.add_argument("--target-namespace", default="gold")
    a = p.parse_args(argv)
    a.countries = COUNTRIES if a.countries == "all" else [c.strip().upper() for c in a.countries.split(",")]
    if set(a.countries) - set(COUNTRIES):
        p.error(f"pays inconnus: {sorted(set(a.countries) - set(COUNTRIES))}")
    return a


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    spark = build_spark("waba-silver-to-gold")
    src, tgt = args.source_namespace, args.target_namespace
    iceberg.ensure_namespaces(spark, tgt)

    def read(name: str):
        return spark.table(iceberg.fq(f"{src}.{name}")).filter(F.col("country_code").isin(args.countries))

    # Faits Silver lus une fois et mis en cache : chacun alimente plusieurs KPIs
    bank = read("bank_transactions").cache()
    mm = read("mobile_money_payments").cache()
    ins = read("insurance_operations").cache()
    loans = read("loan_repayments").cache()

    kpis = {
        "daily_transaction_volume": lambda: G.daily_transaction_volume(bank, mm, ins, loans),
        "npl_ratio_by_country": lambda: G.npl_ratio_by_country(loans),
        "customer_arpu_monthly": lambda: G.customer_arpu_monthly(bank, mm, loans),
        "loss_ratio_by_product": lambda: G.loss_ratio_by_product(ins),
        "claims_processing_time": lambda: G.claims_processing_time(ins),
        "mobile_money_daily_flow": lambda: G.mobile_money_daily_flow(mm),
        "cross_border_transfers": lambda: G.cross_border_transfers(mm),
    }
    results = []
    for name, build in kpis.items():
        t0 = time.time()
        df = build().cache()     # agrégats : quelques milliers de lignes au plus
        iceberg.overwrite_partitions(spark, df, f"{tgt}.{name}", partition_for(name),
                                     cluster_by=["country_code", TIME_COLUMN[name]])
        results.append({"table": f"{tgt}.{name}", "rows": df.count(), "duration_s": round(time.time() - t0, 1)})
        df.unpersist()
        log.info("table gold publiée", extra={"ctx": results[-1]})

    # Alertes réglementaires remontées dans les logs (exploitées par Grafana au Level 4)
    for table, label in [("npl_ratio_by_country", "BCEAO NPL > 5 %"),
                         ("loss_ratio_by_product", "CIMA loss ratio > 70 %")]:
        n = spark.table(iceberg.fq(f"{tgt}.{table}")).filter(
            F.col("country_code").isin(args.countries) & F.col("is_above_threshold")).count()
        log.info("alertes réglementaires", extra={"ctx": {"kpi": table, "rule": label, "breaches": n}})

    for df in (bank, mm, ins, loans):
        df.unpersist()
    log.info("gold terminé", extra={"ctx": {"countries": args.countries, "results": results}})
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
