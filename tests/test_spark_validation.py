"""Tests de la couche de validation Spark (sans Iceberg ni MinIO).

Génère des CSV avec anomalies injectées puis vérifie que :
  * chaque ligne corrompue est rejetée avec un motif explicite ;
  * les doublons de clé sont éliminés ;
  * l'IBAN n'apparaît jamais en clair.
"""
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from waba_gen.referentials import generate_referentials  # noqa: E402
from waba_gen.storage import df_to_csv_bytes  # noqa: E402
from waba_gen.transactions import Pools, generate_transactions, inject_anomalies  # noqa: E402
from waba_spark import validation as V  # noqa: E402
from waba_spark.schemas import SPECS  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def ref():
    return generate_referentials({"customers": 5_000, "accounts": 8_000, "branches": 50, "products": 50})


def _write(tmp_path, name, df):
    p = tmp_path / name
    p.write_bytes(df_to_csv_bytes(df))
    return str(p)


@pytest.mark.parametrize("ds", ["bank_transactions", "insurance_operations",
                                "mobile_money_payments", "loan_repayments"])
def test_anomalies_rejected_and_deduplicated(spark, ref, tmp_path, ds):
    frames = generate_transactions(ref, ds, ["CI", "GH"], 1_000, datetime(2026, 4, 1),
                                   datetime(2026, 6, 30), seed=3, pools=Pools(ref))
    clean = pd.concat(frames.values(), ignore_index=True)
    dirty = inject_anomalies(clean, ds, 0.02, np.random.default_rng(5))
    n_corrupt = int(len(clean) * 0.02)
    path = _write(tmp_path, f"{ds}.csv", dirty)

    spec = SPECS[ds]
    raw = V.read_csv(spark, [path], spec)
    valid, rejected = V.validate(raw, spec)
    deduped = V.deduplicate(valid, spec)

    assert raw.count() == len(dirty)
    # Les lignes corrompues (et leurs éventuels doublons) sont toutes rejetées
    assert n_corrupt <= rejected.count() <= n_corrupt * 2
    assert all(r[0] for r in rejected.select(V.REJECT_REASONS).collect())
    # Plus aucun doublon après dédoublonnage, et aucune ligne propre perdue
    assert deduped.count() == deduped.select(spec.id_col).distinct().count()
    assert deduped.count() == len(clean) - n_corrupt
    rejects = V.build_rejects(rejected, spec, "test").toPandas()
    assert set(rejects["dataset"]) == {ds} and rejects["reject_reasons"].str.len().gt(0).all()


def test_referential_iban_masked(spark, ref, tmp_path):
    spec = SPECS["accounts"]
    path = _write(tmp_path, "accounts.csv", ref.accounts.head(500))
    raw = V.read_csv(spark, [path], spec)
    valid, rejected = V.validate(raw, spec)
    out = V.mask_pii(valid, spec, salt="unit-test-salt").toPandas()
    assert rejected.count() == 0
    assert "iban" not in out.columns
    assert out["iban_masked"].str.contains(r"\*\*\*\*").all()
    assert out["iban_hash"].str.len().eq(64).all() and out["iban_hash"].is_unique


def test_currency_mismatch_detected(spark, ref, tmp_path):
    frames = generate_transactions(ref, "bank_transactions", ["GH"], 50, datetime(2026, 4, 1),
                                   datetime(2026, 4, 2), seed=1, pools=Pools(ref))
    df = frames["GH"].copy()
    df.loc[:4, "currency"] = "XOF"   # devise valide mais incohérente avec le pays
    spec = SPECS["bank_transactions"]
    valid, rejected = V.validate(V.read_csv(spark, [_write(tmp_path, "gh.csv", df)], spec), spec)
    reasons = [r[0] for r in rejected.select(V.REJECT_REASONS).collect()]
    assert len(reasons) == 5 and all("CURRENCY_COUNTRY_MISMATCH" in r for r in reasons)
