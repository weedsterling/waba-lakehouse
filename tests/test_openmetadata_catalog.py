"""Level 4 (9.6b) : cohérence du catalogue as code OpenMetadata avec le code des pipelines."""
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "openmetadata"))
import catalog as C  # noqa: E402

GOLD_SRC = (ROOT / "spark" / "waba_spark" / "gold.py").read_text()
SILVER_SRC = (ROOT / "spark" / "waba_spark" / "silver.py").read_text()


def test_documents_every_gold_kpi_of_the_pipeline():
    kpis = set(re.findall(r"^def ([a-z][a-z_]+)\(", GOLD_SRC, re.M)) - {"working_days"}
    assert {g.name for g in C.GOLD} == kpis
    assert len(C.GOLD) >= 5                                   # exigence : au moins 5 tables Gold documentées
    assert all(len(g.description) > 60 for g in C.GOLD)


def test_regulatory_tags_on_regulatory_tables():
    tags = {g.name: set(g.tags) for g in C.GOLD}
    assert "BCEAO" in tags["npl_ratio_by_country"] and "CIMA" in tags["loss_ratio_by_product"]
    assert set().union(*tags.values()) <= set(C.TAGS)


def test_pii_columns_exist_in_silver_and_cover_the_challenge_fields():
    for table, cols in C.PII.items():
        for col in cols:
            assert f'"{col}"' in SILVER_SRC, f"{table}.{col}"
    flat = {c for cols in C.PII.values() for c in cols}
    assert {"customer_id", "account_id", "iban_masked", "iban_hash"} <= flat


def test_lineage_covers_raw_to_reporting_with_a_pipeline_on_every_edge():
    edges = C.lineage_edges()
    layers = {e[1].split(".")[-2] if e[0] == "table" else "raw" for e in edges}
    assert {"raw", "bronze", "silver", "gold"} <= layers
    assert all(e[4] in C.PIPELINES for e in edges)
    gold_targets = {e[3].split(".")[-1] for e in edges if ".gold." in e[3]}
    assert gold_targets == {g.name for g in C.GOLD}


def test_requests_validate_against_the_openmetadata_sdk():
    pytest.importorskip("metadata.generated.schema.api.data.createContainer")
    assert C.run(dry_run=True) == 0
