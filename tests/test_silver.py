"""Tests des transformations Silver (Spark local, sans Iceberg). Fixtures : conftest.py."""
import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from waba_spark import silver as S  # noqa: E402


def test_row_counts_preserved_by_left_joins(bronze, silver):
    assert silver["bank"].count() == bronze["bank_transactions"].count()
    assert silver["ins"].count() == bronze["insurance_operations"].count()
    assert silver["mm"].count() == bronze["mobile_money_payments"].count()
    assert silver["loans"].count() == bronze["loan_repayments"].count()
    assert silver["accounts"].count() == bronze["accounts"].count()


def test_unique_keys(silver):
    for key, col in [("bank", "transaction_id"), ("ins", "operation_id"), ("mm", "payment_id"),
                     ("loans", "repayment_id")]:
        df = silver[key]
        assert df.count() == df.select(col).distinct().count(), key


def test_eur_conversion(silver):
    xof = silver["bank"].filter("currency = 'XOF'").select(
        F.max(F.abs(F.col("amount_eur") - F.round(F.col("amount") / 655.957, 2)))).first()[0]
    assert xof <= 0.01
    ghs = silver["bank"].filter("currency = 'GHS'").select(F.min("fx_units_per_eur"), F.max("fx_units_per_eur")).first()
    assert 14.6 * 0.96 < ghs[0] <= ghs[1] < 14.6 * 1.04
    assert silver["bank"].filter(F.col("amount_eur").isNull()).count() == 0


def test_no_orphans_on_generated_data(silver):
    assert silver["bank"].filter("is_orphan_account OR is_orphan_branch").count() == 0
    assert silver["mm"].filter("is_orphan_sender OR is_orphan_receiver").count() == 0
    assert silver["loans"].filter("is_orphan_account").count() == 0
    assert silver["ins"].filter("is_orphan_customer").count() == 0


def test_outliers_flagged_not_dropped(silver):
    n = silver["bank"].count()
    flagged = silver["bank"].filter("is_outlier").count()
    assert 0 < flagged < n * 0.03


def test_business_derivations(silver):
    mm = silver["mm"]
    assert mm.filter("is_cross_border AND sender_country = receiver_country").count() == 0
    assert mm.filter("corridor = concat(sender_country, '-', receiver_country)").count() == mm.count()
    loans = silver["loans"]
    assert loans.filter("interest_paid_eur > amount_paid_eur").count() == 0
    assert loans.filter("is_default <> (repayment_status = 'DEFAULT')").count() == 0
    ins = silver["ins"]
    assert set(r[0] for r in ins.select("insurance_branch").distinct().collect()) <= {"IARD", "VIE"}


def test_quality_metrics_shape(silver):
    m = S.quality_metrics(silver["bank"], "bank_transactions", ["is_orphan_account", "is_outlier"])
    assert set(m.columns) == {"dataset", "country_code", "rows", "flags", "computed_at"}
    assert m.count() == silver["bank"].select("country_code").distinct().count()


def test_fact_enrichment_avoids_shuffling_dimensions(spark, bronze, silver):
    """Régression mémoire : l'enrichissement des faits ne doit jamais faire de SortMergeJoin
    avec les grands référentiels (cause d'OutOfMemory sur 800 000 comptes).
    La diffusion automatique est désactivée pour simuler des référentiels volumineux."""
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        cust = S.build_customers(bronze["customers"])
        prod = S.build_products(bronze["products"])
        acc = S.build_accounts(bronze["accounts"], cust, silver["fx"], "2026-09-30")
        frames = {
            "bank": S.build_bank_transactions(bronze["bank_transactions"], acc,
                                              S.build_branches(bronze["branches"]), silver["fx"]),
            "ins": S.build_insurance_operations(bronze["insurance_operations"], cust, prod, silver["fx"]),
            "mm": S.build_mobile_money(bronze["mobile_money_payments"], cust, silver["fx"]),
            "loans": S.build_loan_repayments(bronze["loan_repayments"], acc, prod, silver["fx"]),
        }
        for key, df in frames.items():
            plan = df._jdf.queryExecution().executedPlan().toString()
            # Seule jointure avec shuffle tolérée : accounts x customers, qui construit la dimension
            # accounts (mise en cache une seule fois dans le job). Ancienne version : 4 SortMergeJoin.
            allowed = 1 if key in ("bank", "loans") else 0
            assert plan.count("SortMergeJoin") <= allowed, f"{key} : jointure fait/référentiel avec shuffle"
            assert "LeftSemi" in plan, f"{key} : réduction semi-join de la dimension absente"
    finally:
        spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
