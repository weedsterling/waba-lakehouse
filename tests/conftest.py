"""Fixtures partagées (session) : couches Bronze et Silver construites en Spark local, sans Iceberg.

Les DataFrames « Bronze » sont produits par le vrai pipeline d'ingestion
(lecture CSV à schéma explicite -> validation -> masquage PII -> colonnes techniques).
Imports PySpark différés : les tests sans Spark (générateur, DAGs) restent exécutables.
"""
from datetime import datetime

import pandas as pd
import pytest

from waba_gen.referentials import generate_referentials
from waba_gen.storage import df_to_csv_bytes
from waba_gen.transactions import Pools, generate_transactions

START, END = datetime(2026, 4, 1), datetime(2026, 6, 30, 23, 59)


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.master("local[2]").appName("silver-tests")
         .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="session")
def bronze(spark, tmp_path_factory):
    """Construit les 8 tables Bronze à partir du générateur et du pipeline d'ingestion réel."""
    from waba_spark import validation as V
    from waba_spark.schemas import SPECS

    tmp = tmp_path_factory.mktemp("bronze")
    ref = generate_referentials({"customers": 4_000, "accounts": 6_500, "branches": 60, "products": 50})
    pools = Pools(ref)
    frames = dict(ref.as_dict())
    for ds, n in [("bank_transactions", 3_000), ("insurance_operations", 2_000),
                  ("mobile_money_payments", 3_000), ("loan_repayments", 2_000)]:
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


@pytest.fixture(scope="session")
def silver(spark, bronze):
    from waba_spark import silver as S

    fx = S.fx_rates(spark, "2026-03-31", "2026-12-31").cache()
    customers = S.build_customers(bronze["customers"]).cache()
    branches = S.build_branches(bronze["branches"]).cache()
    products = S.build_products(bronze["products"]).cache()
    accounts = S.build_accounts(bronze["accounts"], customers, fx, "2026-09-30").cache()
    return {
        "fx": fx, "customers": customers, "accounts": accounts, "branches": branches, "products": products,
        "bank": S.build_bank_transactions(bronze["bank_transactions"], accounts, branches, fx).cache(),
        "ins": S.build_insurance_operations(bronze["insurance_operations"], customers, products, fx).cache(),
        "mm": S.build_mobile_money(bronze["mobile_money_payments"], customers, fx).cache(),
        "loans": S.build_loan_repayments(bronze["loan_repayments"], accounts, products, fx).cache(),
    }


