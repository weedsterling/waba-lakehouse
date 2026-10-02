"""Job PySpark Level 2 : reporting réglementaire quotidien BCEAO / CIMA (depuis la couche Gold).

  reporting.bceao_prudential   NPL par pays x entité (dernier mois clos), seuil 5 %
  reporting.cima_technical     loss ratio cumulé annuel par produit + délai sinistres, seuil 70 %

Sorties :
  * tables Iceberg partitionnées par report_date x pays (rejouer une date = même rapport) ;
  * exports CSV par régulateur : s3://lakehouse/exports/regulatory/<rapport>/report_date=<date>/regulator=<X>/
  * résumé JSON des dépassements : s3://lakehouse/exports/regulatory/breaches/report_date=<date>.json
    (lu par la tâche d'alerte Airflow).

Exemple : spark-submit jobs/regulatory_report.py --report-date 2026-10-02 --countries all
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from pyspark.sql import functions as F

from waba_spark import iceberg
from waba_spark import regulatory as R
from waba_spark.common import ObjectStore, build_spark, get_logger
from waba_spark.schemas import COUNTRIES

log = get_logger("waba.regulatory_report")
EXPORT_BUCKET = "lakehouse"
EXPORT_PREFIX = "exports/regulatory"


def breaches_key(report_date: str) -> str:
    return f"{EXPORT_PREFIX}/breaches/report_date={report_date}.json"


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--report-date", required=True, type=lambda v: date.fromisoformat(v.strip()),
                   help="AAAA-MM-JJ")
    p.add_argument("--countries", default="all")
    p.add_argument("--source-namespace", default="gold")
    p.add_argument("--target-namespace", default="reporting")
    a = p.parse_args(argv)
    a.countries = COUNTRIES if a.countries == "all" else [c.strip().upper() for c in a.countries.split(",")]
    if set(a.countries) - set(COUNTRIES):
        p.error(f"pays inconnus: {sorted(set(a.countries) - set(COUNTRIES))}")
    a.report_date = a.report_date.isoformat()
    return a


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    spark = build_spark("waba-regulatory-report")
    src, tgt, day = args.source_namespace, args.target_namespace, args.report_date
    iceberg.ensure_namespaces(spark, tgt)

    def read(name: str):
        return spark.table(iceberg.fq(f"{src}.{name}")).filter(F.col("country_code").isin(args.countries))

    reports = {
        "bceao_prudential": (R.bceao_report(read("npl_ratio_by_country"), day),
                             ["country_code", "entity_type"], "npl_ratio"),
        "cima_technical": (R.cima_report(read("loss_ratio_by_product"), read("claims_processing_time"), day),
                           ["country_code", "product_line"], "loss_ratio_ytd"),
    }
    breaches = []
    for name, (df, keys, ratio) in reports.items():
        df = df.cache()
        iceberg.overwrite_partitions(spark, df, f"{tgt}.{name}",
                                     [F.col("report_date"), F.col("country_code")], cluster_by=["country_code"])
        (df.coalesce(1).write.mode("overwrite").option("header", True).partitionBy("regulator")
           .csv(f"s3a://{EXPORT_BUCKET}/{EXPORT_PREFIX}/{name}/report_date={day}"))
        found = R.breach_summary(df, name, keys, ratio)
        breaches.extend(found)
        log.info("rapport publié", extra={"ctx": {"report": name, "report_date": day,
                                                  "rows": df.count(), "breaches": len(found)}})
        df.unpersist()

    summary = {"report_date": day, "countries": args.countries, "breach_count": len(breaches),
               "breaches": breaches}
    ObjectStore().client.put_object(Bucket=EXPORT_BUCKET, Key=breaches_key(day), ContentType="application/json",
                                    Body=json.dumps(summary, ensure_ascii=False, default=str).encode())
    log.info("reporting réglementaire terminé", extra={"ctx": {"report_date": day, "breach_count": len(breaches)}})
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
