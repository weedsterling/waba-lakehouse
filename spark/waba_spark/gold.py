"""Transformations Silver -> Gold : les 7 KPIs métier (fonctions pures, testables sans Iceberg).

Conventions communes :
  * montants en EUR (colonnes *_eur de Silver) pour comparer XOF et GHS ;
  * toutes les tables portent country_code et entity_type ;
  * les transactions FAILED sont comptées en volume mais exclues des montants et revenus ;
  * les ratios sont NULL quand le dénominateur est nul (jamais de division par zéro) ;
  * les seuils réglementaires sont stockés dans la table (traçabilité des alertes).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

BCEAO_NPL_THRESHOLD = 0.05      # créances en souffrance / encours : alerte au-delà de 5 %
CIMA_LOSS_RATIO_THRESHOLD = 0.70  # sinistres / primes : alerte au-delà de 70 %
DEFAULT_DAYS_OVERDUE = 90       # créance en souffrance : impayé > 90 jours (norme BCEAO)
UEMOA = ["CI", "SN", "ML", "BF", "TG", "BJ"]   # GN et GH hors UEMOA


def _ratio(num: Column, den: Column, scale: int = 4) -> Column:
    return F.when(den > 0, F.round(num / den, scale))


def _gold_ts(df: DataFrame) -> DataFrame:
    return df.withColumn("_gold_ts", F.current_timestamp())


# --------------------------------------------------------------------------- #
# 1. Volume journalier des transactions (tous flux)
# --------------------------------------------------------------------------- #
def daily_transaction_volume(bank: DataFrame, mm: DataFrame, ins: DataFrame, loans: DataFrame) -> DataFrame:
    """Une ligne par jour x pays x entité x flux x type d'opération."""
    def norm(df: DataFrame, flow: str, txn_type: str, amount: str, failed: Column) -> DataFrame:
        return df.select("txn_date", "country_code", "entity_type", F.lit(flow).alias("flow"),
                         F.col(txn_type).alias("txn_type"), F.col(amount).alias("amount_eur"),
                         failed.alias("is_failed"), "is_outlier")

    flows = (norm(bank, "BANK", "transaction_type", "amount_eur", F.col("transaction_status") == "FAILED")
             .unionByName(norm(mm, "MOBILE_MONEY", "payment_type", "amount_eur", F.col("status") == "FAILED"))
             .unionByName(norm(ins, "INSURANCE", "operation_type", "amount_eur", F.lit(False)))
             .unionByName(norm(loans, "LOAN_REPAYMENT", "loan_type", "amount_paid_eur", F.lit(False))))
    ok_amount = F.when(~F.col("is_failed"), F.col("amount_eur"))
    return _gold_ts(flows.groupBy("txn_date", "country_code", "entity_type", "flow", "txn_type").agg(
        F.count("*").alias("txn_count"),
        F.sum(F.col("is_failed").cast("int")).alias("failed_count"),
        F.round(F.sum(ok_amount), 2).alias("total_amount_eur"),
        F.round(F.avg(ok_amount), 2).alias("avg_amount_eur"),
        F.sum(F.col("is_outlier").cast("int")).alias("outlier_count")))


# --------------------------------------------------------------------------- #
# 2. Taux de créances en souffrance (NPL) — BCEAO
# --------------------------------------------------------------------------- #
def npl_ratio_by_country(loans: DataFrame) -> DataFrame:
    """NPL = encours des prêts en défaut / encours total, photographié à chaque fin de mois.

    Le portefeuille d'un mois contient tous les prêts observés jusqu'à ce mois, avec leur
    dernier statut connu (un prêt sans échéance ce mois-ci reste dans l'encours avec son
    statut précédent). Défaut = statut DEFAULT ou impayé > 90 jours."""
    w_last = Window.partitionBy("loan_account_id", "txn_month").orderBy(F.col("event_ts").desc())
    last_in_month = (loans.withColumn("_rn", F.row_number().over(w_last)).filter("_rn = 1")
                          .select("loan_account_id", "country_code", "entity_type", "txn_month",
                                  "loan_outstanding_eur",
                                  (F.col("is_default") | (F.col("days_overdue") > DEFAULT_DAYS_OVERDUE))
                                  .alias("is_npl")))
    # Validité de chaque état : du mois observé jusqu'au mois précédant l'observation suivante
    w_next = Window.partitionBy("loan_account_id").orderBy("txn_month")
    states = last_in_month.withColumn("_valid_to", F.lead("txn_month").over(w_next))
    months = (loans.groupBy("country_code").agg(F.min("txn_month").alias("lo"), F.max("txn_month").alias("hi"))
                   .select("country_code",
                           F.explode(F.expr("sequence(lo, hi, interval 1 month)")).alias("month")))
    snapshot = states.join(F.broadcast(months), "country_code").where(
        (F.col("month") >= F.col("txn_month"))
        & (F.col("_valid_to").isNull() | (F.col("month") < F.col("_valid_to"))))
    npl_amt = F.when(F.col("is_npl"), F.col("loan_outstanding_eur")).otherwise(0.0)
    out = snapshot.groupBy("month", "country_code", "entity_type").agg(
        F.count("*").alias("loans_count"),
        F.sum(F.col("is_npl").cast("int")).alias("npl_loans_count"),
        F.round(F.sum("loan_outstanding_eur"), 2).alias("total_outstanding_eur"),
        F.round(F.sum(npl_amt), 2).alias("npl_outstanding_eur"))
    return _gold_ts(out.withColumnRenamed("month", "report_month")
                       .withColumn("npl_ratio", _ratio(F.col("npl_outstanding_eur"), F.col("total_outstanding_eur")))
                       # Ratio en nombre de prêts : moins sensible aux très gros encours (concentration)
                       .withColumn("npl_ratio_count", _ratio(F.col("npl_loans_count"), F.col("loans_count")))
                       .withColumn("bceao_threshold", F.lit(BCEAO_NPL_THRESHOLD))
                       .withColumn("is_above_threshold", F.coalesce(F.col("npl_ratio") > BCEAO_NPL_THRESHOLD,
                                                                    F.lit(False))))


# --------------------------------------------------------------------------- #
# 3. Revenu moyen par client (ARPU) mensuel
# --------------------------------------------------------------------------- #
def customer_arpu_monthly(bank: DataFrame, mm: DataFrame, loans: DataFrame) -> DataFrame:
    """ARPU = (commissions + intérêts) / nombre de clients actifs distincts,
    par pays x entité x segment x mois. Client actif = au moins une opération dans le mois."""
    def rev(df: DataFrame, cust: str, seg: str, commission: Column, interest: Column) -> DataFrame:
        return df.select("txn_month", "country_code", "entity_type", F.col(cust).alias("customer_id"),
                         F.col(seg).alias("customer_segment"), commission.alias("commission_eur"),
                         interest.alias("interest_eur"))

    zero = F.lit(0.0)
    bank_fee = F.when(F.col("transaction_status") == "SUCCESS", F.col("fee_amount_eur")).otherwise(zero)
    mm_fee = F.when(F.col("status") == "SUCCESS", F.col("fee_amount_eur")).otherwise(zero)
    rows = (rev(bank, "customer_id", "customer_segment", bank_fee, zero)
            .unionByName(rev(mm, "sender_id", "sender_segment", mm_fee, zero))
            .unionByName(rev(loans, "customer_id", "customer_segment", zero, F.col("interest_paid_eur")))
            .where(F.col("customer_id").isNotNull()))
    out = rows.groupBy("txn_month", "country_code", "entity_type", "customer_segment").agg(
        F.countDistinct("customer_id").alias("active_customers"),
        F.round(F.sum("commission_eur"), 2).alias("commission_revenue_eur"),
        F.round(F.sum("interest_eur"), 2).alias("interest_revenue_eur"))
    out = out.withColumn("total_revenue_eur",
                         F.round(F.col("commission_revenue_eur") + F.col("interest_revenue_eur"), 2))
    return _gold_ts(out.withColumnRenamed("txn_month", "report_month")
                       .withColumn("arpu_eur", _ratio(F.col("total_revenue_eur"), F.col("active_customers"), 2)))


# --------------------------------------------------------------------------- #
# 4. Ratio sinistres / primes (loss ratio) — CIMA
# --------------------------------------------------------------------------- #
def loss_ratio_by_product(ins: DataFrame) -> DataFrame:
    """Loss ratio = sinistres payés / primes encaissées, par produit x pays x mois,
    avec cumul depuis le début de l'année (plus stable pour le pilotage CIMA)."""
    prem = F.when(F.col("is_premium"), F.col("amount_eur")).otherwise(0.0)
    paid = F.when(F.col("is_claim_paid"), F.col("amount_eur")).otherwise(0.0)
    m = ins.groupBy("txn_month", "country_code", "entity_type", "product_line", "insurance_branch").agg(
        F.round(F.sum(prem), 2).alias("premiums_eur"),
        F.round(F.sum(paid), 2).alias("claims_paid_eur"),
        F.sum(F.col("is_claim_paid").cast("int")).alias("claims_paid_count"),
        F.sum(F.col("is_premium").cast("int")).alias("premiums_count"))
    ytd = (Window.partitionBy("country_code", "entity_type", "product_line", F.year("txn_month"))
                 .orderBy("txn_month").rowsBetween(Window.unboundedPreceding, Window.currentRow))
    m = (m.withColumn("loss_ratio", _ratio(F.col("claims_paid_eur"), F.col("premiums_eur")))
          .withColumn("loss_ratio_ytd", _ratio(F.sum("claims_paid_eur").over(ytd), F.sum("premiums_eur").over(ytd)))
          .withColumn("cima_threshold", F.lit(CIMA_LOSS_RATIO_THRESHOLD))
          .withColumn("is_above_threshold", F.coalesce(F.col("loss_ratio") > CIMA_LOSS_RATIO_THRESHOLD,
                                                       F.lit(False))))
    return _gold_ts(m.withColumnRenamed("txn_month", "report_month"))


# --------------------------------------------------------------------------- #
# 5. Délai moyen de traitement des sinistres (jours ouvrés)
# --------------------------------------------------------------------------- #
def working_days(end_date: Column, calendar_days: Column) -> Column:
    """Jours ouvrés (lundi-vendredi) de la période ]end - calendar_days, end].
    Jours fériés non gérés : à brancher sur un calendrier pays en production."""
    days = F.sequence(F.date_sub(end_date, calendar_days - 1), end_date)
    return F.when(calendar_days > 0,
                  F.size(F.filter(days, lambda d: ~F.dayofweek(d).isin(1, 7)))).otherwise(F.lit(0))


def claims_processing_time(ins: DataFrame) -> DataFrame:
    """Sinistres clos (payés, acceptés ou rejetés) : délai déclaration -> décision.
    Hypothèse : processing_days = jours calendaires écoulés à la date de l'opération."""
    closed = ins.where(F.col("is_claim") & F.col("processing_days").isNotNull()
                       & F.col("claim_status").isin("PAID", "APPROVED", "REJECTED"))
    closed = closed.withColumn("_wd", working_days(F.col("txn_date"), F.col("processing_days")))
    out = closed.groupBy("txn_month", "country_code", "entity_type", "insurance_branch").agg(
        F.count("*").alias("closed_claims_count"),
        F.round(F.avg("_wd"), 2).alias("avg_working_days"),
        F.percentile_approx("_wd", 0.5).alias("median_working_days"),
        F.percentile_approx("_wd", 0.9).alias("p90_working_days"),
        F.round(F.avg("processing_days"), 2).alias("avg_calendar_days"))
    return _gold_ts(out.withColumnRenamed("txn_month", "report_month"))


# --------------------------------------------------------------------------- #
# 6. Flux Mobile Money journaliers
# --------------------------------------------------------------------------- #
def mobile_money_daily_flow(mm: DataFrame) -> DataFrame:
    """Volume, montant, taux d'échec et utilisateurs actifs (émetteurs ou bénéficiaires)."""
    ok = F.col("status") == "SUCCESS"
    out = mm.groupBy("txn_date", "country_code", "entity_type", "operator").agg(
        F.count("*").alias("txn_count"),
        F.sum(ok.cast("int")).alias("success_count"),
        F.sum((F.col("status") == "FAILED").cast("int")).alias("failed_count"),
        F.sum((F.col("status") == "PENDING").cast("int")).alias("pending_count"),
        F.round(F.sum(F.when(ok, F.col("amount_eur"))), 2).alias("total_amount_eur"),
        F.round(F.avg(F.when(ok, F.col("amount_eur"))), 2).alias("avg_amount_eur"),
        F.round(F.sum(F.when(ok, F.col("fee_amount_eur"))), 2).alias("fee_revenue_eur"),
        F.countDistinct("sender_id").alias("active_senders"),
        F.size(F.array_distinct(F.flatten(F.collect_set(F.array("sender_id", "receiver_id")))))
         .alias("active_users"))
    return _gold_ts(out.withColumn("failure_rate", _ratio(F.col("failed_count"), F.col("txn_count"))))


# --------------------------------------------------------------------------- #
# 7. Transferts transfrontaliers (corridors)
# --------------------------------------------------------------------------- #
def cross_border_transfers(mm: DataFrame) -> DataFrame:
    """Par corridor et par semaine : nombre, montant total et moyen, évolution vs semaine précédente
    (calculée sur la semaine calendaire S-1, NULL si le corridor était inactif)."""
    ok = F.col("status") == "SUCCESS"
    w = mm.where("is_cross_border").groupBy(
        "txn_week", "country_code", "entity_type", "corridor", "sender_country", "receiver_country").agg(
        F.count("*").alias("transfer_count"),
        F.sum(ok.cast("int")).alias("success_count"),
        F.round(F.sum(F.when(ok, F.col("amount_eur"))), 2).alias("total_amount_eur"),
        F.round(F.avg(F.when(ok, F.col("amount_eur"))), 2).alias("avg_amount_eur"),
        F.round(F.sum(F.when(ok, F.col("fee_amount_eur"))), 2).alias("fee_revenue_eur"))
    prev = w.select("corridor", "entity_type", F.date_add("txn_week", 7).alias("txn_week"),
                    F.col("total_amount_eur").alias("prev_week_amount_eur"),
                    F.col("transfer_count").alias("prev_week_count"))
    out = (w.join(prev, ["corridor", "entity_type", "txn_week"], "left")
            .withColumn("wow_amount_change_pct",
                        F.round(_ratio(F.col("total_amount_eur") - F.col("prev_week_amount_eur"),
                                       F.col("prev_week_amount_eur"), 6) * 100, 2))
            .withColumn("is_uemoa_corridor",
                        F.col("sender_country").isin(UEMOA) & F.col("receiver_country").isin(UEMOA)))
    return _gold_ts(out)
