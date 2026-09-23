"""Tests unitaires du générateur : intégrité référentielle, devises, distributions.

Exécution : pytest -q tests/   (depuis la racine, avec generator/ dans le PYTHONPATH)
"""
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from waba_gen import config as C
from waba_gen.referentials import generate_referentials
from waba_gen.transactions import Pools, generate_transactions, inject_anomalies

START, END = datetime(2026, 4, 1), datetime(2026, 6, 30, 23, 59)


@pytest.fixture(scope="module")
def ref():
    return generate_referentials({"customers": 20_000, "accounts": 32_000, "branches": 200, "products": 50})


@pytest.fixture(scope="module")
def pools(ref):
    return Pools(ref)


def _gen(ref, pools, ds, n=4_000):
    return pd.concat(generate_transactions(ref, ds, C.COUNTRIES, n, START, END, seed=7, pools=pools).values())


def test_referential_sizes_and_uniqueness(ref):
    assert len(ref.customers) == 20_000 and ref.customers["customer_id"].is_unique
    assert len(ref.accounts) == 32_000 and ref.accounts["account_id"].is_unique
    assert len(ref.branches) >= 200 and ref.branches["branch_id"].is_unique
    assert len(ref.products) == 50


def test_accounts_reference_existing_customers(ref):
    assert ref.accounts["customer_id"].isin(ref.customers["customer_id"]).all()


def test_entity_perimeter_respected(ref):
    mm = ref.customers[ref.customers["entity_type"] == "MOBILE_MONEY"]
    mf = ref.customers[ref.customers["entity_type"] == "MICROFINANCE"]
    assert set(mm["country_code"]) <= set(C.ENTITY_COUNTRIES["MOBILE_MONEY"])
    assert set(mf["country_code"]) <= set(C.ENTITY_COUNTRIES["MICROFINANCE"])


@pytest.mark.parametrize("ds", C.TRANSACTIONAL_DATASETS)
def test_common_columns_and_currency(ref, pools, ds):
    df = _gen(ref, pools, ds)
    assert {"country_code", "entity_type"} <= set(df.columns)
    assert (df["currency"] == df["country_code"].map(C.CURRENCY_MAP)).all()


def test_bank_txn_no_orphans(ref, pools):
    df = _gen(ref, pools, "bank_transactions")
    assert df["account_id"].isin(ref.accounts["account_id"]).all()
    assert df["beneficiary_account"].isin(ref.accounts["account_id"]).all()
    assert df["branch_id"].isin(ref.branches["branch_id"]).all()
    assert df["transaction_id"].is_unique


def test_mobile_money_no_orphans(ref, pools):
    df = _gen(ref, pools, "mobile_money_payments")
    ids = ref.customers["customer_id"]
    assert df["sender_id"].isin(ids).all() and df["receiver_id"].isin(ids).all()


def test_loss_ratio_in_regulatory_band(ref, pools):
    df = _gen(ref, pools, "insurance_operations", n=20_000)
    prem = df[df["operation_type"].isin(["PREMIUM_PAYMENT", "POLICY_RENEWAL"])].groupby("country_code")["amount"].sum()
    paid = df[df["operation_type"] == "CLAIM_PAYMENT"].groupby("country_code")["amount"].sum()
    lr = paid / prem
    assert lr.between(0.45, 0.90).all(), lr


def test_loans_reference_loan_accounts(ref, pools):
    df = _gen(ref, pools, "loan_repayments")
    loans = ref.accounts[ref.accounts["account_type"] == "LOAN"]["account_id"]
    assert df["loan_account_id"].isin(loans).all()
    assert (df.loc[df["repayment_status"] == "ON_TIME", "days_overdue"] == 0).all()


def test_anomaly_injection_adds_duplicates(ref, pools):
    clean = _gen(ref, pools, "bank_transactions", n=1_000)
    dirty = inject_anomalies(clean.reset_index(drop=True), "bank_transactions", 0.02, np.random.default_rng(1))
    assert len(dirty) == len(clean) + 10                  # 2 % de 1000 = 20 anomalies -> 10 doublons
    assert dirty["transaction_id"].duplicated().sum() >= 9  # doublons présents (hors ids vidés)
    changed = (dirty.iloc[: len(clean)].astype(str) != clean.reset_index(drop=True).astype(str)).any(axis=1)
    assert changed.sum() == 20                             # exactement 20 lignes corrompues
