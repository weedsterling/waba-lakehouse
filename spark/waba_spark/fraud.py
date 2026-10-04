"""Speed layer — Job 2 : règles de fraude, AML et liquidité sur la couche Silver (fonctions pures).

Les fonctions acceptent indifféremment un DataFrame batch ou streaming (window(), filtres, jointures
stream-statique) : les mêmes règles sont testées en batch et exécutées en continu.

  LARGE_TXN_BURST  >= 3 transactions > 500 000 XOF (équivalent EUR) depuis un même compte en 5 min
                   (fenêtre glissante 5 min, pas 1 min)
  UNUSUAL_COUNTRY  paiement mobile money depuis un pays absent du profil du client
                   (pays de résidence + pays déjà utilisés dans l'historique Silver)
  CLAIM_GT_3X      sinistre > 3 x la prime annuelle de l'assuré (primes 12 mois, annualisées)
  AML              virement au-dessus du seuil déclaratif : 1 000 000 XOF (UEMOA) / 5 000 GHS (Ghana)
  LIQUIDITY        sorties nettes d'un pays sur 5 min > 1 % des dépôts (comptes courants + épargne)
"""
from __future__ import annotations

import os

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .silver import XOF_PER_EUR

LARGE_TXN_XOF = float(os.environ.get("FRAUD_LARGE_TXN_XOF", 500_000))
# « Transactions multiples » : 3 minimum. Avec 2, les coïncidences aléatoires (comptes tirés au hasard,
# ~23 % des virements > 500 000 XOF) produisent des faux positifs en continu ; ajustable par variable.
LARGE_TXN_MIN_COUNT = int(os.environ.get("FRAUD_LARGE_TXN_MIN_COUNT", 3))
CLAIM_PREMIUM_RATIO = float(os.environ.get("FRAUD_CLAIM_PREMIUM_RATIO", 3))
PREMIUM_PAYMENTS_PER_YEAR = int(os.environ.get("PREMIUM_PAYMENTS_PER_YEAR", 12))
AML_THRESHOLD = {"XOF": 1_000_000.0, "GHS": 5_000.0}
LIQUIDITY_OUTFLOW_RATIO = float(os.environ.get("LIQUIDITY_OUTFLOW_RATIO", 0.01))
WINDOW, SLIDE, WATERMARK = "5 minutes", "1 minute", "10 minutes"

BANK_TRANSFERS = ["TRANSFER", "INTERNATIONAL_WIRE"]
MM_TRANSFERS = ["P2P", "CROSS_BORDER_TRANSFER"]
OUTFLOWS = ["WITHDRAWAL", "TRANSFER", "INTERNATIONAL_WIRE", "PAYMENT"]


# --------------------------------------------------------------------------- #
# Utilitaires
# --------------------------------------------------------------------------- #
def parse_silver(kafka: DataFrame, schema: StructType) -> DataFrame:
    """Messages silver-* (JSON écrit par le Job 1) -> colonnes typées selon le schéma de la table rt_*."""
    return (kafka.select(F.from_json(F.col("value").cast("string"), schema).alias("r"))
                 .select("r.*").where(F.col("country_code").isNotNull()))


def _alert_id(*parts: Column) -> Column:
    """Identifiant déterministe : une même alerte rejouée (reprise, mode update) écrase la précédente."""
    return F.sha2(F.concat_ws("|", *[p.cast("string") for p in parts]), 256)


def _alert(rule: str, severity: str, subject_type: str, subject: Column, key: list[Column], *,
           event_time: Column, amount_eur: Column, window_start: Column | None = None,
           window_end: Column | None = None, txn_count: Column | None = None,
           transaction_ids: Column | None = None, details: Column | None = None) -> list[Column]:
    null_ts = F.lit(None).cast("timestamp")
    return [
        _alert_id(F.lit(rule), *key).alias("alert_id"),
        F.lit(rule).alias("rule_code"), F.lit(severity).alias("severity"),
        F.col("country_code"), F.col("entity_type"),
        F.lit(subject_type).alias("subject_type"), subject.cast("string").alias("subject_id"),
        event_time.alias("event_time"),
        (window_start if window_start is not None else null_ts).alias("window_start"),
        (window_end if window_end is not None else null_ts).alias("window_end"),
        F.round(amount_eur, 2).alias("amount_eur"),
        (txn_count if txn_count is not None else F.lit(1)).cast("int").alias("txn_count"),
        (transaction_ids if transaction_ids is not None else F.array().cast("array<string>")).alias("transaction_ids"),
        (details if details is not None else F.lit("{}")).alias("details"),
        F.current_timestamp().alias("detected_at"),
    ]


# --------------------------------------------------------------------------- #
# Règle 1 : rafale de grosses transactions (fenêtre glissante, avec état)
# --------------------------------------------------------------------------- #
def large_txn_windows(bank: DataFrame) -> DataFrame:
    big = bank.where((F.col("amount_eur") > LARGE_TXN_XOF / XOF_PER_EUR) & (F.col("transaction_status") != "FAILED"))
    return (big.groupBy(F.window("event_ts", WINDOW, SLIDE).alias("w"), "account_id", "country_code", "entity_type")
               .agg(F.count("*").alias("n"), F.sum("amount_eur").alias("total_eur"),
                    F.max("event_ts").alias("last_ts"), F.sort_array(F.collect_set("transaction_id")).alias("ids")))


def large_txn_alerts(windows: DataFrame) -> DataFrame:
    hits = windows.where(F.col("n") >= LARGE_TXN_MIN_COUNT)
    return hits.select(*_alert(
        "LARGE_TXN_BURST", "HIGH", "ACCOUNT", F.col("account_id"), [F.col("account_id"), F.col("w.start")],
        event_time=F.col("last_ts"), amount_eur=F.col("total_eur"), window_start=F.col("w.start"),
        window_end=F.col("w.end"), txn_count=F.col("n"), transaction_ids=F.col("ids"),
        details=F.to_json(F.struct(F.lit(LARGE_TXN_XOF).alias("threshold_xof_per_txn"),
                                   F.lit(LARGE_TXN_MIN_COUNT).alias("min_count")))))


# --------------------------------------------------------------------------- #
# Règle 2 : pays inhabituel pour le profil du client (mobile money)
# --------------------------------------------------------------------------- #
def mm_profiles(customers: DataFrame, mm_history: DataFrame) -> DataFrame:
    """sender_id -> pays habituels = pays de résidence + pays d'émission déjà observés."""
    hist = mm_history.groupBy("sender_id").agg(F.collect_set("sender_country").alias("seen"))
    home = customers.select(F.col("customer_id").alias("sender_id"), F.col("country_code").alias("home_country"))
    return (home.join(hist, "sender_id", "left")
                .select("sender_id", "home_country",
                        F.array_union(F.array("home_country"), F.coalesce("seen", F.array().cast("array<string>")))
                         .alias("usual_countries")))


def unusual_country_alerts(mm: DataFrame, profiles: DataFrame) -> DataFrame:
    j = mm.where(F.col("status") != "FAILED").join(F.broadcast(profiles), "sender_id", "inner")
    hits = j.where(~F.array_contains("usual_countries", F.col("sender_country")))
    return hits.select(*_alert(
        "UNUSUAL_COUNTRY", "MEDIUM", "CUSTOMER", F.col("sender_id"), [F.col("payment_id")],
        event_time=F.col("event_ts"), amount_eur=F.col("amount_eur"),
        transaction_ids=F.array("payment_id"),
        details=F.to_json(F.struct("sender_country", "home_country", "usual_countries", "operator", "payment_type"))))


# --------------------------------------------------------------------------- #
# Règle 3 : sinistre > 3 x la prime annuelle versée (assurance)
# --------------------------------------------------------------------------- #
def premiums_12m(ins: DataFrame, as_of: Column | None = None) -> DataFrame:
    """Prime annuelle par assuré = max(primes versées sur 12 mois, prime moyenne x échéances par an).

    Hypothèse documentée : les PREMIUM_PAYMENT sont des échéances mensuelles (cas courant en
    micro-assurance UEMOA) ; l'annualisation évite de comparer un sinistre à une seule mensualité
    lorsque l'historique de l'assuré est incomplet."""
    as_of = as_of if as_of is not None else F.current_timestamp()
    recent = ins.where(F.col("is_premium") & (F.col("event_ts") >= as_of - F.expr("INTERVAL 365 DAYS")))
    return (recent.groupBy("customer_id")
                  .agg(F.sum("amount_eur").alias("_paid"), F.avg("amount_eur").alias("_avg"))
                  .select("customer_id", F.greatest("_paid", F.col("_avg") * PREMIUM_PAYMENTS_PER_YEAR)
                          .alias("premiums_12m_eur")))


def claim_alerts(ins: DataFrame, premiums: DataFrame) -> DataFrame:
    """Seuls les assurés ayant versé au moins une prime sont évalués (sinon ratio non défini)."""
    claims = ins.where(F.col("operation_type").isin("CLAIM_SUBMISSION", "CLAIM_PAYMENT"))
    j = claims.join(premiums, "customer_id", "inner").where(F.col("premiums_12m_eur") > 0)
    hits = j.where(F.col("amount_eur") > CLAIM_PREMIUM_RATIO * F.col("premiums_12m_eur"))
    return hits.select(*_alert(
        "CLAIM_GT_3X_PREMIUM", "HIGH", "CUSTOMER", F.col("customer_id"), [F.col("operation_id")],
        event_time=F.col("event_ts"), amount_eur=F.col("amount_eur"), transaction_ids=F.array("operation_id"),
        details=F.to_json(F.struct("operation_type", "product_line",
                                   F.round("premiums_12m_eur", 2).alias("premiums_12m_eur"),
                                   F.round(F.col("amount_eur") / F.col("premiums_12m_eur"), 2).alias("ratio")))))


# --------------------------------------------------------------------------- #
# AML : virements au-dessus du seuil déclaratif (en devise locale)
# --------------------------------------------------------------------------- #
def _aml_threshold() -> Column:
    return F.create_map(*[F.lit(x) for kv in AML_THRESHOLD.items() for x in kv])[F.col("currency")]


def aml_events(bank: DataFrame, mm: DataFrame) -> DataFrame:
    def shape(df: DataFrame, source: str, txn_id: str, ttype: str, sender: str, receiver: str) -> DataFrame:
        return df.select(
            _alert_id(F.lit("AML"), F.col(txn_id)).alias("event_id"), F.lit(source).alias("source"),
            F.col(txn_id).alias("transaction_id"), F.col(ttype).alias("transaction_type"),
            "country_code", "entity_type", F.col(sender).cast("string").alias("sender"),
            F.col(receiver).cast("string").alias("receiver"), "currency", F.col("amount").alias("amount_local"),
            _aml_threshold().alias("threshold_local"), F.round("amount_eur", 2).alias("amount_eur"),
            F.col("event_ts").alias("event_time"), F.current_timestamp().alias("detected_at"))

    b = bank.where(F.col("transaction_type").isin(BANK_TRANSFERS) & (F.col("transaction_status") != "FAILED"))
    m = mm.where(F.col("payment_type").isin(MM_TRANSFERS) & (F.col("status") != "FAILED"))
    out = (shape(b, "BANK", "transaction_id", "transaction_type", "account_id", "beneficiary_account")
           .unionByName(shape(m, "MOBILE_MONEY", "payment_id", "payment_type", "sender_id", "receiver_id")))
    return out.where(F.col("amount_local") > F.col("threshold_local"))


# --------------------------------------------------------------------------- #
# Liquidité : sorties nettes par pays sur fenêtre glissante vs dépôts
# --------------------------------------------------------------------------- #
def liquidity_reserves(accounts: DataFrame) -> DataFrame:
    return (accounts.where(F.col("account_type").isin("CURRENT", "SAVINGS") & (F.col("status") != "CLOSED"))
                    .groupBy("country_code").agg(F.sum("balance_eur").alias("deposits_eur")))


def liquidity_windows(bank: DataFrame) -> DataFrame:
    ok = bank.where(F.col("transaction_status") == "SUCCESS")
    signed = (F.when(F.col("transaction_type") == "DEPOSIT", -F.col("amount_eur"))
               .when(F.col("transaction_type").isin(OUTFLOWS), F.col("amount_eur")).otherwise(0.0))
    return (ok.groupBy(F.window("event_ts", WINDOW, SLIDE).alias("w"), "country_code")
              .agg(F.sum(signed).alias("net_outflow_eur"), F.count("*").alias("n"), F.max("event_ts").alias("last_ts")))


def liquidity_alerts(windows: DataFrame, reserves: DataFrame) -> DataFrame:
    j = windows.join(F.broadcast(reserves), "country_code", "inner").where(F.col("deposits_eur") > 0)
    j = j.withColumn("outflow_ratio", F.col("net_outflow_eur") / F.col("deposits_eur"))
    hits = j.where(F.col("outflow_ratio") > LIQUIDITY_OUTFLOW_RATIO).withColumn("entity_type", F.lit("BANK"))
    return hits.select(*_alert(
        "LIQUIDITY_COVERAGE", "CRITICAL", "COUNTRY", F.col("country_code"), [F.col("country_code"), F.col("w.start")],
        event_time=F.col("last_ts"), amount_eur=F.col("net_outflow_eur"), window_start=F.col("w.start"),
        window_end=F.col("w.end"), txn_count=F.col("n"),
        details=F.to_json(F.struct(F.round("deposits_eur", 2).alias("deposits_eur"),
                                   F.round(F.col("outflow_ratio") * 100, 3).alias("outflow_pct"),
                                   F.round((1 - F.col("outflow_ratio")) * 100, 3).alias("coverage_pct"),
                                   F.lit(LIQUIDITY_OUTFLOW_RATIO * 100).alias("threshold_pct")))))


def to_kafka(df: DataFrame) -> DataFrame:
    return df.select(F.col("country_code").alias("key"), F.to_json(F.struct(*df.columns)).alias("value"))
