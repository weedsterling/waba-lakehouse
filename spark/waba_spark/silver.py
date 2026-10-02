"""Transformations Bronze -> Silver (fonctions pures sur DataFrames, testables sans Iceberg).

Règles appliquées à tous les flux :
  * dédoublonnage défensif sur la clé métier (dernière version selon _ingestion_ts) ;
  * conversion des montants en EUR (XOF : parité fixe BCEAO ; GHS : taux journalier) ;
  * enrichissement par les référentiels (LEFT JOIN : aucune ligne perdue) avec
    indicateurs is_orphan_* plutôt que suppression silencieuse ;
  * normalisation des nulls (valeurs 'UNKNOWN') ;
  * détection des valeurs aberrantes (barrière de Tukey sur log-montants, par pays),
    signalées par is_outlier, jamais supprimées ;
  * colonnes de temps dérivées (txn_date, txn_month, txn_hour) pour les agrégats Gold.
"""
from __future__ import annotations

import math

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

XOF_PER_EUR = 655.957          # parité fixe franc CFA / euro (BCEAO)
GHS_PER_EUR_REF = 14.6         # niveau de référence du cedi (taux simulé, cf. fx_rates)
GHS_AMPLITUDE = 0.03           # variation saisonnière simulée ±3 %
UNKNOWN = "UNKNOWN"


# --------------------------------------------------------------------------- #
# Utilitaires
# --------------------------------------------------------------------------- #
def dedup_latest(df: DataFrame, id_col: str, order_col: str = "_ingestion_ts") -> DataFrame:
    """Garde la dernière version de chaque clé (défensif : Bronze est déjà idempotent)."""
    w = Window.partitionBy(id_col).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def restrict_dim(dim: DataFrame, fact: DataFrame, fact_key: str, dim_key: str) -> DataFrame:
    """Réduit une grande dimension aux seules clés présentes dans le lot de faits, puis la diffuse.

    Pattern « semi-join + broadcast » : au lieu de mélanger (shuffle) 800 000 comptes pour
    enrichir quelques milliers de transactions, on filtre la dimension avec les clés du lot
    (left_semi, clés diffusées) et on diffuse le résultat — coût mémoire proportionnel au lot,
    pas au référentiel. Indispensable pour un traitement incrémental à grande échelle."""
    keys = fact.select(F.col(fact_key).alias(dim_key)).where(F.col(dim_key).isNotNull()).distinct()
    return F.broadcast(dim.join(F.broadcast(keys), dim_key, "left_semi"))


def with_time_columns(df: DataFrame, ts_col: str = "timestamp") -> DataFrame:
    return (df.withColumnRenamed(ts_col, "event_ts")
              .withColumn("txn_date", F.to_date("event_ts"))
              .withColumn("txn_month", F.trunc("event_ts", "month")))


def fx_rates(spark: SparkSession, start: str, end: str) -> DataFrame:
    """Table de change quotidienne (unités de devise locale pour 1 EUR).

    XOF : parité fixe 655,957. GHS : taux SIMULÉ déterministe (référence 14,6 ± 3 %
    saisonnier) — en production, alimenté par une source officielle (Bank of Ghana)."""
    days = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS rate_date")
    ghs = F.lit(GHS_PER_EUR_REF) * (
        F.lit(1.0) + F.lit(GHS_AMPLITUDE) * F.sin(F.lit(2 * math.pi) * F.dayofyear("rate_date") / F.lit(365.0)))
    return (days.withColumn("XOF", F.lit(XOF_PER_EUR)).withColumn("GHS", F.round(ghs, 6))
                .select("rate_date", F.expr("stack(2, 'XOF', XOF, 'GHS', GHS) AS (currency, units_per_eur)")))


def to_eur(df: DataFrame, fx: DataFrame, amount_cols: list[str], date_col: str = "txn_date") -> DataFrame:
    """Ajoute fx_units_per_eur et <col>_eur pour chaque colonne montant (jointure broadcast)."""
    rates = F.broadcast(fx.withColumnRenamed("rate_date", "_fx_date").withColumnRenamed("currency", "_fx_cur"))
    out = (df.join(rates, (F.col(date_col) == F.col("_fx_date")) & (F.col("currency") == F.col("_fx_cur")), "left")
             .withColumnRenamed("units_per_eur", "fx_units_per_eur")
             .drop("_fx_date", "_fx_cur"))
    for c in amount_cols:
        out = out.withColumn(f"{c}_eur", F.round(F.col(c) / F.col("fx_units_per_eur"), 2))
    return out


def flag_outliers(df: DataFrame, amount_col: str = "amount_eur", k: float = 1.5) -> DataFrame:
    """is_outlier = montant au-delà de la barrière de Tukey (Q3 + k·IQR) calculée par pays
    sur l'échelle logarithmique — adaptée aux montants financiers (distribution log-normale).
    Les valeurs aberrantes sont SIGNALÉES pour revue, jamais supprimées."""
    log_amt = F.log1p(F.col(amount_col))
    q = (df.groupBy("country_code")
           .agg(F.percentile_approx(log_amt, [0.25, 0.75], 10_000).alias("_q"))
           .select("country_code",
                   (F.col("_q")[1] + F.lit(k) * (F.col("_q")[1] - F.col("_q")[0])).alias("_fence")))
    return (df.join(F.broadcast(q), "country_code", "left")
              .withColumn("is_outlier", F.coalesce(log_amt > F.col("_fence"), F.lit(False)))
              .drop("_fence"))


def _unknown(col: str) -> Column:
    return F.coalesce(F.col(col), F.lit(UNKNOWN)).alias(col)


def _silver_ts(df: DataFrame) -> DataFrame:
    return df.withColumn("_silver_ts", F.current_timestamp())


# --------------------------------------------------------------------------- #
# Référentiels (dimensions)
# --------------------------------------------------------------------------- #
def build_customers(customers: DataFrame) -> DataFrame:
    c = dedup_latest(customers, "customer_id")
    return _silver_ts(c.select(
        "customer_id", "country_code", "entity_type", _unknown("segment"), _unknown("kyc_level"),
        "onboarding_date", _unknown("region"),
        F.coalesce("is_active", F.lit(False)).alias("is_active")))


def build_branches(branches: DataFrame) -> DataFrame:
    b = dedup_latest(branches, "branch_id")
    return _silver_ts(b.select(
        "branch_id", "country_code", "entity_type", _unknown("city"), _unknown("region"),
        _unknown("branch_type"), F.coalesce("is_active", F.lit(False)).alias("is_active")))


def build_products(products: DataFrame) -> DataFrame:
    p = dedup_latest(products, "product_id")
    return _silver_ts(p.select(
        "product_id", "country_code", "product_code", "product_name", "product_category", "entity_type",
        "currency", F.coalesce("commission_rate", F.lit(0.0)).alias("commission_rate"),
        F.coalesce("interest_rate", F.lit(0.0)).alias("interest_rate"), "launch_date", "is_active"))


def build_accounts(accounts: DataFrame, customers_silver: DataFrame, fx: DataFrame,
                   as_of: str) -> DataFrame:
    """Comptes + segment client + soldes en EUR au taux du jour `as_of`."""
    a = dedup_latest(accounts, "account_id").withColumn("_as_of", F.to_date(F.lit(as_of)))
    cust = customers_silver.select("customer_id", F.col("segment").alias("customer_segment"))
    a = (a.join(cust, "customer_id", "left")
          .withColumn("is_orphan_customer", F.col("customer_segment").isNull())
          .withColumn("customer_segment", F.coalesce("customer_segment", F.lit(UNKNOWN))))
    a = to_eur(a, fx, ["balance", "credit_limit"], date_col="_as_of")
    return _silver_ts(a.select(
        "account_id", "customer_id", "country_code", "entity_type", "account_type", "status", "currency",
        "balance", "credit_limit", "fx_units_per_eur", "balance_eur", "credit_limit_eur", "opened_date",
        "iban_masked", "iban_hash", "customer_segment", "is_orphan_customer"))


# --------------------------------------------------------------------------- #
# Faits (transactions)
# --------------------------------------------------------------------------- #
def build_bank_transactions(bank: DataFrame, accounts_silver: DataFrame, branches_silver: DataFrame,
                            fx: DataFrame) -> DataFrame:
    t = with_time_columns(dedup_latest(bank, "transaction_id"))
    acc = accounts_silver.select("account_id", "customer_id", "account_type", "customer_segment")
    br = branches_silver.select("branch_id", F.col("city").alias("branch_city"),
                                F.col("region").alias("branch_region"), F.lit(True).alias("_branch_found"))
    acc = restrict_dim(acc, t, "account_id", "account_id")
    t = (t.join(acc, "account_id", "left").join(F.broadcast(br), "branch_id", "left")
          .withColumn("is_orphan_account", F.col("account_type").isNull())
          .withColumn("is_orphan_branch", F.col("_branch_found").isNull())
          .withColumn("customer_segment", F.coalesce("customer_segment", F.lit(UNKNOWN)))
          .withColumn("channel", F.coalesce("channel", F.lit(UNKNOWN))))
    t = flag_outliers(to_eur(t, fx, ["amount", "fee_amount"]))
    return _silver_ts(t.select(
        "transaction_id", "event_ts", "txn_date", "txn_month", "country_code", "entity_type",
        "account_id", "beneficiary_account", "customer_id", "customer_segment", "account_type",
        "branch_id", "branch_city", "branch_region", "transaction_type", "channel", "transaction_status",
        "currency", "amount", "fee_amount", "fx_units_per_eur", "amount_eur", "fee_amount_eur",
        "is_orphan_account", "is_orphan_branch", "is_outlier", "_source_file", "_batch_id"))


def build_insurance_operations(ops: DataFrame, customers_silver: DataFrame, products_silver: DataFrame,
                               fx: DataFrame) -> DataFrame:
    o = with_time_columns(dedup_latest(ops, "operation_id"))
    cust = customers_silver.select("customer_id", F.col("segment").alias("customer_segment"))
    prod = products_silver.select("country_code", F.col("product_code").alias("product_line"), "product_id")
    cust = restrict_dim(cust, o, "customer_id", "customer_id")
    o = (o.join(cust, "customer_id", "left").join(F.broadcast(prod), ["country_code", "product_line"], "left")
          .withColumn("is_orphan_customer", F.col("customer_segment").isNull())
          .withColumn("customer_segment", F.coalesce("customer_segment", F.lit(UNKNOWN)))
          .withColumn("insurance_branch",
                      F.when(F.col("product_line").startswith("IARD"), "IARD").otherwise("VIE"))
          .withColumn("is_premium", F.col("operation_type").isin("PREMIUM_PAYMENT", "POLICY_RENEWAL"))
          .withColumn("is_claim_paid", F.col("operation_type") == "CLAIM_PAYMENT")
          .withColumn("is_claim", F.col("operation_type").isin("CLAIM_SUBMISSION", "CLAIM_PAYMENT")))
    o = flag_outliers(to_eur(o, fx, ["amount"]))
    return _silver_ts(o.select(
        "operation_id", "event_ts", "txn_date", "txn_month", "country_code", "entity_type", "customer_id",
        "customer_segment", "account_id", "operation_type", "product_line", "insurance_branch", "product_id",
        "claim_status", "processing_days", "currency", "amount", "fx_units_per_eur", "amount_eur",
        "is_premium", "is_claim", "is_claim_paid", "is_orphan_customer", "is_outlier",
        "_source_file", "_batch_id"))


def build_mobile_money(mm: DataFrame, customers_silver: DataFrame, fx: DataFrame) -> DataFrame:
    m = with_time_columns(dedup_latest(mm, "payment_id"))
    snd = customers_silver.select(F.col("customer_id").alias("sender_id"),
                                  F.col("segment").alias("sender_segment"))
    rcv = customers_silver.select(F.col("customer_id").alias("receiver_id"), F.lit(True).alias("_rcv_found"))
    snd = restrict_dim(snd, m, "sender_id", "sender_id")
    rcv = restrict_dim(rcv, m, "receiver_id", "receiver_id")
    m = (m.join(snd, "sender_id", "left").join(rcv, "receiver_id", "left")
          .withColumn("is_orphan_sender", F.col("sender_segment").isNull())
          .withColumn("is_orphan_receiver", F.col("_rcv_found").isNull())
          .withColumn("sender_segment", F.coalesce("sender_segment", F.lit(UNKNOWN)))
          .withColumn("is_cross_border", F.col("sender_country") != F.col("receiver_country"))
          .withColumn("corridor", F.concat_ws("-", "sender_country", "receiver_country"))
          .withColumn("txn_hour", F.hour("event_ts"))
          .withColumn("txn_week", F.to_date(F.date_trunc("week", "event_ts"))))
    m = flag_outliers(to_eur(m, fx, ["amount", "fee_amount"]))
    return _silver_ts(m.select(
        "payment_id", "event_ts", "txn_date", "txn_week", "txn_month", "txn_hour", "country_code",
        "entity_type", "sender_id", "sender_segment", "receiver_id", "sender_country", "receiver_country",
        "corridor", "is_cross_border", "payment_type", "operator", "status", "currency", "amount",
        "fee_amount", "fx_units_per_eur", "amount_eur", "fee_amount_eur", "is_orphan_sender",
        "is_orphan_receiver", "is_outlier", "_source_file", "_batch_id"))


def build_loan_repayments(loans: DataFrame, accounts_silver: DataFrame, products_silver: DataFrame,
                          fx: DataFrame, default_rate: float = 0.12) -> DataFrame:
    """Ajoute l'encours du prêt (solde du compte LOAN) et la part d'intérêts du paiement.

    Hypothèse documentée : interest_paid = amount_paid × r / (1 + r), r = taux annuel du produit
    (défaut 12 % si le produit n'est pas au catalogue du pays)."""
    ln = with_time_columns(dedup_latest(loans, "repayment_id"))
    acc = accounts_silver.select(F.col("account_id").alias("loan_account_id"),
                                 F.col("balance").alias("loan_outstanding"), "customer_segment")
    prod = products_silver.select("country_code", F.col("product_code").alias("loan_type"), "interest_rate")
    acc = restrict_dim(acc, ln, "loan_account_id", "loan_account_id")
    ln = (ln.join(acc, "loan_account_id", "left").join(F.broadcast(prod), ["country_code", "loan_type"], "left")
            .withColumn("is_orphan_account", F.col("loan_outstanding").isNull())
            .withColumn("customer_segment", F.coalesce("customer_segment", F.lit(UNKNOWN)))
            .withColumn("interest_rate", F.when(F.col("interest_rate") > 0, F.col("interest_rate"))
                                          .otherwise(F.lit(default_rate)))
            .withColumn("loan_outstanding", F.coalesce("loan_outstanding", F.lit(0.0))))
    ln = to_eur(ln, fx, ["amount_due", "amount_paid", "loan_outstanding"])
    ln = (ln.withColumn("interest_paid_eur",
                        F.round(F.col("amount_paid_eur") * F.col("interest_rate") / (1 + F.col("interest_rate")), 2))
            .withColumn("is_default", F.col("repayment_status") == "DEFAULT"))
    ln = flag_outliers(ln, "amount_due_eur")
    return _silver_ts(ln.select(
        "repayment_id", "event_ts", "txn_date", "txn_month", "country_code", "entity_type", "loan_account_id",
        "customer_id", "customer_segment", "loan_type", "repayment_status", "is_default", "due_date",
        "payment_date", "days_overdue", "currency", "amount_due", "amount_paid", "loan_outstanding",
        "fx_units_per_eur", "amount_due_eur", "amount_paid_eur", "loan_outstanding_eur", "interest_rate",
        "interest_paid_eur", "is_orphan_account", "is_outlier", "_source_file", "_batch_id"))


# --------------------------------------------------------------------------- #
# Qualité
# --------------------------------------------------------------------------- #
def quality_metrics(df: DataFrame, dataset: str, flag_cols: list[str]) -> DataFrame:
    """Une ligne par pays : volumétrie + nombre de lignes par indicateur de qualité."""
    aggs = [F.count("*").alias("rows")] + [F.sum(F.col(c).cast("int")).alias(c) for c in flag_cols]
    m = df.groupBy("country_code").agg(*aggs)
    return m.select(F.lit(dataset).alias("dataset"), "country_code", "rows",
                    F.to_json(F.struct(*flag_cols)).alias("flags"), F.current_timestamp().alias("computed_at"))
