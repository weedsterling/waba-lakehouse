"""Tests des transformations Silver (Spark local, sans Iceberg).

Les DataFrames « Bronze » sont produits par le vrai pipeline d'ingestion
(lecture CSV à schéma explicite -> validation -> masquage PII -> colonnes techniques).
"""
from datetime import datetime

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from waba_gen.referentials import generate_referentials  # noqa: E402
from waba_gen.storage import df_to_csv_bytes  # noqa: E402
from waba_gen.transactions import Pools, generate_transactions  # noqa: E402
from waba_spark import silver as S  # noqa: E402
from waba_spark import validation as V  # noqa: E402
from waba_spark.schemas import SPECS  # noqa: E402

START, END = datetime(2026, 4, 1), datetime(2026, 6, 30, 23, 59)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("silver-tests")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def bronze(spark, tmp_path_factory):
    """Construit les 8 tables Bronze à partir du générateur et du pipeline d'ingestion réel."""
    tmp = tmp_path_factory.mktemp("bronze")
    ref = generate_referentials({"customers": 4_000, "accounts": 6_500, "branches": 60, "products": 50})
    pools = Pools(ref)
    frames = dict(ref.as_dict())
    for ds, n in [("bank_transactions", 3_000), ("insurance_operations", 2_000),
                  ("mobile_money_payments", 3_000), ("loan_repayments", 2_000)]:
        import pandas as pd
        frames[ds] = pd.concat(generate_transactions(ref, ds, ["CI", "SN", "GH", "ML"], n, START, END,
                                                     seed=11, pools=pools).values(), ignore_index=True)
    out = {}
    for name, pdf in frames.items():
        path = tmp / f"{name}.csv"
        path.write_bytes(df_to_csv_bytes(pdf))
        spec = SPECS[name]
        valid, _ = V.validate(V.read_csv(spark, [str(path)], spec), spec)
        out[name] = V.add_technical_columns(V.mask_pii(V.deduplicate(valid, spec), spec, salt="t"), "b1").cache()
    return out


@pytest.fixture(scope="module")
def silver(spark, bronze):
    fx = S.fx_rates(spark, "2026-03-31", "2026-12-31").cache()
    customers = S.build_customers(bronze["customers"]).cache()
    branches = S.build_branches(bronze["branches"]).cache()
    products = S.build_products(bronze["products"]).cache()
    accounts = S.build_accounts(bronze["accounts"], customers, fx, "2026-09-30").cache()
    return {
        "fx": fx, "customers": customers, "accounts": accounts,
        "bank": S.build_bank_transactions(bronze["bank_transactions"], accounts, branches, fx).cache(),
        "ins": S.build_insurance_operations(bronze["insurance_operations"], customers, products, fx).cache(),
        "mm": S.build_mobile_money(bronze["mobile_money_payments"], customers, fx).cache(),
        "loans": S.build_loan_repayments(bronze["loan_repayments"], accounts, products, fx).cache(),
    }


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
