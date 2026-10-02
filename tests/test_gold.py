"""Tests des KPIs Gold (Spark local, sans Iceberg) : formules, grain, seuils, réalisme des données."""
from datetime import date

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from waba_spark import gold as G  # noqa: E402


@pytest.fixture(scope="module")
def gold(silver):
    s = silver
    return {
        "volume": G.daily_transaction_volume(s["bank"], s["mm"], s["ins"], s["loans"]).cache(),
        "npl": G.npl_ratio_by_country(s["loans"]).cache(),
        "arpu": G.customer_arpu_monthly(s["bank"], s["mm"], s["loans"]).cache(),
        "loss": G.loss_ratio_by_product(s["ins"]).cache(),
        "claims": G.claims_processing_time(s["ins"]).cache(),
        "mm": G.mobile_money_daily_flow(s["mm"]).cache(),
        "xb": G.cross_border_transfers(s["mm"]).cache(),
    }


def test_every_gold_table_has_country_and_entity(gold):
    for name, df in gold.items():
        assert {"country_code", "entity_type"} <= set(df.columns), name
        assert df.count() > 0, name


def test_daily_volume_reconciles_with_silver(silver, gold):
    v = gold["volume"]
    total = v.agg(F.sum("txn_count")).first()[0]
    assert total == sum(silver[k].count() for k in ("bank", "mm", "ins", "loans"))
    bank_ok = silver["bank"].filter("transaction_status <> 'FAILED'").agg(F.sum("amount_eur")).first()[0]
    got = v.filter("flow = 'BANK'").agg(F.sum("total_amount_eur")).first()[0]
    assert got == pytest.approx(bank_ok, rel=1e-6)
    assert v.groupBy("txn_date", "country_code", "entity_type", "flow", "txn_type").count() \
            .filter("count > 1").count() == 0


def test_npl_ratio_formula_and_realistic_range(silver, gold):
    npl = gold["npl"]
    bad = npl.filter(F.abs(F.col("npl_ratio") - F.col("npl_outstanding_eur") / F.col("total_outstanding_eur"))
                     > 1e-4).count()
    assert bad == 0
    assert npl.filter("npl_ratio < 0 OR npl_ratio > 1").count() == 0
    # Portefeuille cumulé : le nombre de prêts ne décroît jamais d'un mois sur l'autre
    rows = npl.groupBy("country_code", "report_month").agg(F.sum("loans_count").alias("n")) \
              .orderBy("country_code", "report_month").collect()
    for a, b in zip(rows, rows[1:], strict=False):
        if a.country_code == b.country_code:
            assert b.n >= a.n
    # Données réalistes : part des prêts en défaut entre 2 % et 10 % (cible générateur 3-8 %)
    tot = npl.agg(F.sum("npl_loans_count"), F.sum("loans_count")).first()
    assert 0.02 <= tot[0] / tot[1] <= 0.10
    assert npl.filter("is_above_threshold <> (npl_ratio > bceao_threshold)").count() == 0


def test_npl_snapshot_keeps_last_known_status(spark):
    rows = [  # prêt L1 en défaut en avril, sans échéance en mai : toujours en défaut en mai
        ("L1", "CI", "BANK", "2026-04-10 10:00:00", date(2026, 4, 1), 1000.0, True, 120),
        ("L2", "CI", "BANK", "2026-04-11 10:00:00", date(2026, 4, 1), 3000.0, False, 0),
        ("L2", "CI", "BANK", "2026-05-11 10:00:00", date(2026, 5, 1), 3000.0, False, 0),
    ]
    df = spark.createDataFrame(rows, "loan_account_id string, country_code string, entity_type string, "
                                     "event_ts string, txn_month date, loan_outstanding_eur double, "
                                     "is_default boolean, days_overdue int") \
              .withColumn("event_ts", F.to_timestamp("event_ts"))
    out = {r.report_month: r for r in G.npl_ratio_by_country(df).collect()}
    assert out[date(2026, 5, 1)].loans_count == 2
    assert out[date(2026, 5, 1)].npl_ratio == pytest.approx(0.25)


def test_arpu_formula(gold):
    a = gold["arpu"]
    assert a.filter(F.abs(F.col("arpu_eur") - F.col("total_revenue_eur") / F.col("active_customers")) > 0.01) \
            .count() == 0
    assert a.filter("interest_revenue_eur > 0").count() > 0
    assert a.filter("commission_revenue_eur > 0").count() > 0


def test_loss_ratio_realistic_and_flagged(gold):
    loss = gold["loss"]
    ytd = loss.groupBy("country_code").agg(
        (F.sum("claims_paid_eur") / F.sum("premiums_eur")).alias("lr")).collect()
    for r in ytd:  # ratio global par pays dans la fourchette réglementaire réaliste
        assert 0.45 <= r.lr <= 0.95, r
    assert loss.filter("is_above_threshold <> coalesce(loss_ratio > 0.70, false)").count() == 0
    assert set(r[0] for r in loss.select("insurance_branch").distinct().collect()) <= {"IARD", "VIE"}


def test_working_days(spark):
    # Vendredi 2026-05-08, 7 jours calendaires -> sam 02 .. ven 08 = 5 jours ouvrés
    df = spark.createDataFrame([(date(2026, 5, 8), 7), (date(2026, 5, 4), 1), (date(2026, 5, 9), 1),
                                (date(2026, 5, 9), 0)], "d date, n int")
    got = [r[0] for r in df.select(G.working_days(F.col("d"), F.col("n"))).collect()]
    assert got == [5, 1, 0, 0]


def test_claims_processing_time(gold):
    c = gold["claims"]
    assert c.filter("avg_working_days > avg_calendar_days").count() == 0
    by_branch = {r.insurance_branch: r.d for r in
                 c.groupBy("insurance_branch").agg(F.avg("avg_working_days").alias("d")).collect()}
    assert by_branch["VIE"] > by_branch["IARD"]   # Vie plus long à traiter que IARD


def test_mobile_money_flow(silver, gold):
    m = gold["mm"]
    assert m.agg(F.sum("txn_count")).first()[0] == silver["mm"].count()
    assert m.filter("failure_rate < 0 OR failure_rate > 1").count() == 0
    assert m.filter("active_users < active_senders").count() == 0
    rate = m.agg(F.sum("failed_count") / F.sum("txn_count")).first()[0]
    assert 0.01 < rate < 0.10


def test_cross_border_transfers(silver, gold):
    xb = gold["xb"]
    assert xb.agg(F.sum("transfer_count")).first()[0] == silver["mm"].filter("is_cross_border").count()
    assert xb.filter("sender_country = receiver_country").count() == 0
    assert xb.filter("corridor = 'CI-SN'").select("is_uemoa_corridor").first()[0] is True
    assert xb.filter("corridor LIKE 'GH-%'").filter("is_uemoa_corridor").count() == 0
    # évolution S/S-1 cohérente avec les montants
    chk = xb.filter("prev_week_amount_eur > 0").withColumn(
        "exp", F.round((F.col("total_amount_eur") / F.col("prev_week_amount_eur") - 1) * 100, 2))
    assert chk.count() > 0
    assert chk.filter(F.abs(F.col("exp") - F.col("wow_amount_change_pct")) > 0.02).count() == 0
