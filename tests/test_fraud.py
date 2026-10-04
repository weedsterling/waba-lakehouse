"""Job 2 : règles fraude / AML / liquidité sur un micro-lot réaliste (flux normal + scénarios injectés
par le générateur), passé par le vrai pipeline Bronze -> Silver. Chaque scénario doit déclencher
exactement sa règle, et le flux normal ne doit déclencher aucune alerte de fraude."""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from waba_gen.fraud import fraud_scenarios  # noqa: E402
from waba_gen.referentials import generate_referentials  # noqa: E402
from waba_gen.storage import df_to_csv_bytes  # noqa: E402
from waba_gen.transactions import Pools, generate_transactions  # noqa: E402
from waba_spark import fraud as FR  # noqa: E402
from waba_spark import silver as S  # noqa: E402
from waba_spark import validation as V  # noqa: E402
from waba_spark.schemas import SPECS  # noqa: E402

NOW = datetime(2026, 6, 15, 12, 0, 0)
COUNTRIES = ["CI", "SN", "GH", "ML"]
DATASETS = ["bank_transactions", "mobile_money_payments", "insurance_operations"]


def to_silver(spark, tmp, silver, frames: dict[str, pd.DataFrame]) -> dict:
    out = {}
    for ds, pdf in frames.items():
        path = tmp / f"{ds}.csv"
        path.write_bytes(df_to_csv_bytes(pdf))
        spec = SPECS[ds]
        valid, rej = V.validate(V.read_csv(spark, [str(path)], spec), spec)
        assert rej.count() == 0, f"{ds} : scénario injecté invalide"
        bronze = V.add_technical_columns(V.deduplicate(valid, spec), "t")
        if ds == "bank_transactions":
            out["bank"] = S.build_bank_transactions(bronze, silver["accounts"], silver["branches"], silver["fx"])
        elif ds == "mobile_money_payments":
            out["mm"] = S.build_mobile_money(bronze, silver["customers"], silver["fx"])
        else:
            out["ins"] = S.build_insurance_operations(bronze, silver["customers"], silver["products"], silver["fx"])
    return {k: v.cache() for k, v in out.items()}


@pytest.fixture(scope="module")
def batches(spark, silver, tmp_path_factory):
    ref = generate_referentials({"customers": 4_000, "accounts": 6_500, "branches": 60, "products": 50})
    pools, rng = Pools(ref), np.random.default_rng(3)
    normal = {ds: pd.concat(generate_transactions(ref, ds, COUNTRIES, 400, NOW - timedelta(minutes=10), NOW,
                                                  seed=5, pools=pools).values(), ignore_index=True)
              for ds in DATASETS}
    extra = fraud_scenarios(pools, COUNTRIES, NOW, rng, bank_run=True)
    injected = {ds: pd.concat([normal[ds], *extra[ds].values()], ignore_index=True) for ds in DATASETS}
    return {"normal": to_silver(spark, tmp_path_factory.mktemp("n"), silver, normal),
            "injected": to_silver(spark, tmp_path_factory.mktemp("i"), silver, injected)}


def rules(b, silver):
    profiles = FR.mm_profiles(silver["customers"], silver["mm"])
    premiums = FR.premiums_12m(silver["ins"].unionByName(b["ins"]), F.lit(NOW).cast("timestamp"))
    # La fixture compte 6 500 comptes contre 800 000 en production : on remet les dépôts à l'échelle
    # pour garder le rapport réaliste entre volume de transactions et portefeuille de dépôts.
    reserves = FR.liquidity_reserves(silver["accounts"])
    reserves = reserves.withColumn("deposits_eur", F.col("deposits_eur") * 800_000 / 6_500)
    return {
        "LARGE_TXN_BURST": FR.large_txn_alerts(FR.large_txn_windows(b["bank"])),
        "UNUSUAL_COUNTRY": FR.unusual_country_alerts(b["mm"], profiles),
        "CLAIM_GT_3X_PREMIUM": FR.claim_alerts(b["ins"], premiums),
        "LIQUIDITY_COVERAGE": FR.liquidity_alerts(FR.liquidity_windows(b["bank"]), reserves),
        "AML": FR.aml_events(b["bank"], b["mm"]),
    }


def test_normal_flow_raises_no_fraud_alert(batches, silver):
    r = rules(batches["normal"], silver)
    for rule in ("LARGE_TXN_BURST", "UNUSUAL_COUNTRY", "CLAIM_GT_3X_PREMIUM"):
        assert r[rule].count() == 0, rule


def test_each_scenario_triggers_its_rule(batches, silver):
    r, base = rules(batches["injected"], silver), rules(batches["normal"], silver)
    burst = r["LARGE_TXN_BURST"]
    assert burst.count() >= 1 and burst.agg(F.max("txn_count")).first()[0] == 3
    assert burst.select("subject_id").distinct().count() == 1          # un seul compte en rafale
    assert r["UNUSUAL_COUNTRY"].count() == 2
    claims = r["CLAIM_GT_3X_PREMIUM"].collect()
    assert len(claims) == 1 and claims[0].severity == "HIGH"
    assert r["LIQUIDITY_COVERAGE"].count() >= 1
    assert base["LIQUIDITY_COVERAGE"].count() == 0                      # flux normal sous le seuil
    assert r["AML"].count() > base["AML"].count()                       # virement > seuil déclaratif ajouté


def test_aml_threshold_in_local_currency(batches):
    aml = FR.aml_events(batches["injected"]["bank"], batches["injected"]["mm"])
    assert aml.filter("amount_local <= threshold_local").count() == 0
    t = {r.currency: r.threshold_local for r in aml.select("currency", "threshold_local").distinct().collect()}
    assert t.get("XOF", 1e6) == 1e6 and t.get("GHS", 5e3) == 5e3


def test_alert_ids_are_deterministic(batches, silver):
    a = FR.unusual_country_alerts(batches["injected"]["mm"], FR.mm_profiles(silver["customers"], silver["mm"]))
    ids = sorted(r.alert_id for r in a.collect())
    assert ids == sorted(r.alert_id for r in a.collect()) and len(set(ids)) == len(ids)
    msg = FR.to_kafka(a).first()
    assert msg.key in ("CI", "SN", "GH", "ML") and '"rule_code":"UNUSUAL_COUNTRY"' in msg.value


def test_windowed_rules_run_as_streaming_queries(spark, batches, tmp_path):
    """Même règle exécutée en vrai streaming (source fichiers, watermark, mode update, foreachBatch) :
    le plan doit être accepté par Spark et produire les alertes de la rafale injectée."""
    bank = batches["injected"]["bank"]
    bank.write.parquet(str(tmp_path / "bank"))
    stream = (spark.readStream.schema(bank.schema).parquet(str(tmp_path / "bank"))
              .withWatermark("event_ts", FR.WATERMARK))
    seen = []
    q = (FR.large_txn_windows(stream).writeStream.outputMode("update")
         .foreachBatch(lambda df, _: seen.extend(r.rule_code for r in FR.large_txn_alerts(df).collect()))
         .option("checkpointLocation", str(tmp_path / "chk")).trigger(availableNow=True).start())
    q.awaitTermination()
    assert seen and set(seen) == {"LARGE_TXN_BURST"}
