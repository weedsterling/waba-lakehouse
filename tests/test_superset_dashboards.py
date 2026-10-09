"""Level 4 (9.5) : cohérence des tableaux de bord Superset « as code » (sans Superset ni Trino).

Contrôles : références croisées (graphique -> jeu de données -> colonnes), mise en page, UUID stables,
bundle d'import lisible, SQL Trino syntaxiquement valide et colonnes de sortie = colonnes déclarées.
Le rendu réel (import + affichage des 15 graphiques) a été vérifié sur Superset 6.1.0 ; la requête de
chaque jeu de données s'exécute dans le cluster par :
  python3 superset/dashboards/waba_dashboards.py --check-sql | ./scripts/k8s/trino.sh
"""
import io
import sys
import zipfile
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "superset" / "dashboards"))
import waba_dashboards as W  # noqa: E402


def referenced_columns(c: W.Chart) -> set[str]:
    p = c.params
    cols = set()
    for key in ("x_axis", "entity"):
        if p.get(key):
            cols.add(p[key])
    g = p.get("groupby") or []
    cols |= {g} if isinstance(g, str) else set(g)
    for key in ("all_columns", "groupbyRows", "groupbyColumns"):
        cols |= set(p.get(key) or [])
    for f in p.get("adhoc_filters") or []:
        if f.get("subject"):
            cols.add(f["subject"])
    return cols


def test_three_dashboards_as_required():
    assert len(W.DASHBOARDS) == 3
    assert {"world_map", "heatmap_v2", "pivot_table_v2", "table", "echarts_timeseries_bar",
            "echarts_timeseries_line"} <= {c.viz_type for c in W.CHARTS}


def test_every_chart_is_on_a_dashboard_and_layout_fits():
    placed = [n for d in W.DASHBOARDS for row in d.rows for n in row]
    assert sorted(placed) == sorted(W.CH) and len(placed) == len(set(placed))
    for d in W.DASHBOARDS:
        for row in d.rows:
            assert sum(W.CH[n].width for n in row) <= 12, (d.slug, row)


@pytest.mark.parametrize("chart", W.CHARTS, ids=[c.name for c in W.CHARTS])
def test_chart_columns_exist_in_dataset(chart):
    assert chart.dataset in W.DS
    missing = referenced_columns(chart) - set(W.DS[chart.dataset].columns)
    assert not missing, missing


def test_every_dataset_has_country_code_for_filters_and_rls():
    for d in W.DATASETS:
        assert "country_code" in d.columns, d.name
        assert d.main_dttm_col is None or d.main_dttm_col in d.columns


def test_uuids_are_unique_and_stable():
    ids = [W.uid("database", W.DATABASE_NAME)] + [x.uuid for x in [*W.DATASETS, *W.CHARTS, *W.DASHBOARDS]]
    assert len(ids) == len(set(ids))
    assert W.DS["ds_npl_mensuel"].uuid == W.uid("dataset", "ds_npl_mensuel")   # pas d'aléa : ré-import = mise à jour


def test_bundle_is_a_valid_superset_v1_export():
    z = zipfile.ZipFile(io.BytesIO(W.build_bundle()))
    docs = {n: yaml.safe_load(z.read(n)) for n in z.namelist()}
    assert docs["waba_dashboards/metadata.yaml"]["type"] == "Dashboard"
    dbs = [d for n, d in docs.items() if "/databases/" in n]
    assert len(dbs) == 1 and dbs[0]["sqlalchemy_uri"].startswith("trino://")
    assert "password" not in yaml.safe_dump(dbs[0]).lower()       # aucun secret dans le bundle
    datasets = {d["uuid"]: d for n, d in docs.items() if "/datasets/" in n}
    charts = {d["uuid"]: d for n, d in docs.items() if "/charts/" in n}
    dashboards = [d for n, d in docs.items() if "/dashboards/" in n]
    assert len(datasets) == len(W.DATASETS) and len(charts) == len(W.CHARTS) and len(dashboards) == 3
    assert all(c["dataset_uuid"] in datasets for c in charts.values())
    for d in dashboards:
        metas = [v["meta"] for v in d["position"].values() if isinstance(v, dict) and v.get("type") == "CHART"]
        assert metas and all(m["uuid"] in charts and isinstance(m["chartId"], int) for m in metas)
        [flt] = d["metadata"]["native_filter_configuration"]
        assert flt["targets"][0]["datasetUuid"] in datasets and flt["targets"][0]["column"]["name"] == "country_code"


@pytest.mark.parametrize("ds", W.DATASETS, ids=[d.name for d in W.DATASETS])
def test_dataset_sql_is_valid_trino_with_declared_columns(ds):
    sqlglot = pytest.importorskip("sqlglot")
    tree = sqlglot.parse_one(ds.sql, read="trino")
    assert [s.alias_or_name for s in tree.selects] == list(ds.columns)
    assert "lakehouse." in ds.sql                                   # catalogue explicite


def test_check_sql_covers_every_dataset(capsys):
    assert W.main(["--check-sql"]) == 0
    out = capsys.readouterr().out
    assert out.count("SELECT '") == len(W.DATASETS) and out.count(";") >= len(W.DATASETS)
