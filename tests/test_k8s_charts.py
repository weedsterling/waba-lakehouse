"""Level 4 : validité YAML des charts Helm maison (Chart.yaml, values.yaml) et du helmfile.
Les templates sont validés par `helm lint` dans deploy.sh (binaire Helm requis)."""
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")
ROOT = Path(__file__).resolve().parents[1] / "k8s"
CHARTS = sorted(p for p in (ROOT / "charts").iterdir() if p.is_dir())


@pytest.mark.parametrize("chart", CHARTS, ids=[c.name for c in CHARTS])
def test_chart_metadata_and_values_parse(chart):
    meta = yaml.safe_load((chart / "Chart.yaml").read_text())
    assert meta["apiVersion"] == "v2" and meta["name"] == chart.name and meta["version"]
    assert isinstance(yaml.safe_load((chart / "values.yaml").read_text()), dict)


def test_helmfile_releases_reference_existing_charts():
    hf = yaml.safe_load((ROOT / "helmfile.yaml").read_text())
    names = {f"{r['namespace']}/{r['name']}" for r in hf["releases"]}
    for r in hf["releases"]:
        if r["chart"].startswith("./"):
            assert (ROOT / r["chart"]).is_dir(), r["chart"]
        assert set(r.get("needs", [])) <= names, r["name"]
