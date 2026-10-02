"""Reporting réglementaire quotidien (BCEAO / CIMA) construit à partir de la couche Gold.

Fonctions pures, testables sans Iceberg. Chaque rapport est une photographie datée
(report_date) : rejouer une date réécrit exactement le même rapport (idempotence).

  * BCEAO (prudentiel bancaire) : taux de créances en souffrance par pays et entité,
    au dernier mois clos disponible, seuil d'alerte 5 %.
  * CIMA (technique assurance) : loss ratio cumulé depuis le 1er janvier par produit,
    délai moyen de traitement des sinistres, seuil d'alerte 70 %.

Hors UEMOA, le régulateur national est indiqué (Guinée, Ghana) : même indicateur,
autorité différente — utile pour router le rapport vers le bon destinataire.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .gold import BCEAO_NPL_THRESHOLD, CIMA_LOSS_RATIO_THRESHOLD, UEMOA

BANKING_REGULATOR = {**{c: "BCEAO" for c in UEMOA}, "GN": "BCRG", "GH": "BOG"}
INSURANCE_REGULATOR = {**{c: "CIMA" for c in UEMOA}, "GN": "DNA_GN", "GH": "NIC"}


def _regulator(mapping: dict[str, str]) -> Column:
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])[F.col("country_code")]


def _stamp(df: DataFrame, report_date: str) -> DataFrame:
    return (df.withColumn("report_date", F.to_date(F.lit(report_date)))
              .withColumn("generated_at", F.current_timestamp()))


def _as_of_month(report_date: str) -> Column:
    """Données arrêtées à J-1 : un rapport du 1er du mois porte sur le mois précédent."""
    return F.trunc(F.date_sub(F.to_date(F.lit(report_date)), 1), "month")


def bceao_report(npl: DataFrame, report_date: str) -> DataFrame:
    """Dernier mois disponible <= mois d'arrêté, par pays x entité (banque, microfinance)."""
    eligible = npl.where(F.col("report_month") <= _as_of_month(report_date))
    latest = eligible.groupBy("country_code", "entity_type").agg(F.max("report_month").alias("report_month"))
    out = (eligible.join(latest, ["country_code", "entity_type", "report_month"])
                   .select("country_code", "entity_type", F.col("report_month").alias("data_month"),
                           "loans_count", "npl_loans_count", "total_outstanding_eur", "npl_outstanding_eur",
                           "npl_ratio", "npl_ratio_count")
                   .withColumn("regulator", _regulator(BANKING_REGULATOR))
                   .withColumn("threshold", F.lit(BCEAO_NPL_THRESHOLD))
                   .withColumn("is_breach", F.coalesce(F.col("npl_ratio") > BCEAO_NPL_THRESHOLD, F.lit(False))))
    return _stamp(out, report_date)


def cima_report(loss: DataFrame, claims: DataFrame, report_date: str) -> DataFrame:
    """Cumul depuis le 1er janvier de l'année d'arrêté, par pays x produit, enrichi du délai
    moyen de traitement des sinistres (jours ouvrés, pondéré par le nombre de sinistres)."""
    as_of = _as_of_month(report_date)

    def ytd(df: DataFrame) -> DataFrame:
        return df.where((F.col("report_month") <= as_of) & (F.year("report_month") == F.year(as_of)))

    lr = ytd(loss).groupBy("country_code", "entity_type", "insurance_branch", "product_line").agg(
        F.min("report_month").alias("period_start"), F.max("report_month").alias("data_month"),
        F.round(F.sum("premiums_eur"), 2).alias("premiums_ytd_eur"),
        F.round(F.sum("claims_paid_eur"), 2).alias("claims_paid_ytd_eur"),
        F.sum("claims_paid_count").alias("claims_paid_ytd_count"))
    delay = ytd(claims).groupBy("country_code", "insurance_branch").agg(
        F.round(F.sum(F.col("avg_working_days") * F.col("closed_claims_count"))
                / F.sum("closed_claims_count"), 2).alias("avg_claim_working_days_ytd"))
    out = (lr.join(delay, ["country_code", "insurance_branch"], "left")
             .withColumn("loss_ratio_ytd", F.when(F.col("premiums_ytd_eur") > 0,
                                                  F.round(F.col("claims_paid_ytd_eur") / F.col("premiums_ytd_eur"), 4)))
             .withColumn("regulator", _regulator(INSURANCE_REGULATOR))
             .withColumn("threshold", F.lit(CIMA_LOSS_RATIO_THRESHOLD))
             .withColumn("is_breach", F.coalesce(F.col("loss_ratio_ytd") > CIMA_LOSS_RATIO_THRESHOLD,
                                                 F.lit(False))))
    return _stamp(out, report_date)


def breach_summary(report: DataFrame, report_name: str, key_cols: list[str], ratio_col: str) -> list[dict]:
    """Résumé compact (quelques lignes) des dépassements, exporté en JSON pour l'alerting."""
    rows = (report.where("is_breach")
                  .select(*key_cols, "regulator", F.round(ratio_col, 4).alias("ratio"), "threshold")
                  .orderBy(*key_cols).collect())
    return [{"report": report_name, **r.asDict()} for r in rows]
