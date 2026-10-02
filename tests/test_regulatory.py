"""Tests du reporting réglementaire BCEAO / CIMA (Spark local, sans Iceberg)."""
import json
from datetime import date

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from waba_spark import gold as G  # noqa: E402
from waba_spark import regulatory as R  # noqa: E402

REPORT_DATE = "2026-07-01"   # données arrêtées au 30/06 -> dernier mois clos = juin


@pytest.fixture(scope="module")
def gold(silver):
    return {"npl": G.npl_ratio_by_country(silver["loans"]).cache(),
            "loss": G.loss_ratio_by_product(silver["ins"]).cache(),
            "claims": G.claims_processing_time(silver["ins"]).cache()}


def test_bceao_report_latest_closed_month(gold):
    rep = R.bceao_report(gold["npl"], REPORT_DATE)
    assert rep.groupBy("country_code", "entity_type").count().filter("count > 1").count() == 0
    assert {r[0] for r in rep.select("data_month").distinct().collect()} == {date(2026, 6, 1)}
    regs = {r.country_code: r.regulator for r in rep.select("country_code", "regulator").collect()}
    assert regs["CI"] == "BCEAO" and regs["GH"] == "BOG"
    assert rep.filter("is_breach <> coalesce(npl_ratio > threshold, false)").count() == 0
    # Rapport du 1er mai : données arrêtées au 30 avril -> mois d avril
    early = R.bceao_report(gold["npl"], "2026-05-01")
    assert {r[0] for r in early.select("data_month").distinct().collect()} == {date(2026, 4, 1)}


def test_cima_report_ytd(gold):
    rep = R.cima_report(gold["loss"], gold["claims"], REPORT_DATE)
    assert rep.groupBy("country_code", "product_line").count().filter("count > 1").count() == 0
    # Le cumul annuel égale la somme des mois Gold de l'année
    exp = gold["loss"].where("report_month <= date'2026-06-01'").agg(F.sum("premiums_eur")).first()[0]
    assert rep.agg(F.sum("premiums_ytd_eur")).first()[0] == pytest.approx(exp, rel=1e-6)
    assert rep.filter("avg_claim_working_days_ytd IS NULL").count() == 0
    assert rep.filter("is_breach <> coalesce(loss_ratio_ytd > threshold, false)").count() == 0
    assert {r[0] for r in rep.select("regulator").distinct().collect()} <= {"CIMA", "NIC", "DNA_GN"}


def test_breach_summary_is_json_serializable(gold):
    rep = R.cima_report(gold["loss"], gold["claims"], REPORT_DATE)
    out = R.breach_summary(rep, "cima_technical", ["country_code", "product_line"], "loss_ratio_ytd")
    assert len(out) == rep.filter("is_breach").count()
    json.dumps(out, default=str)
