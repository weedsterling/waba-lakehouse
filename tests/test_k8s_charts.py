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


def test_airflow_passwords_file_is_writable():
    """Régression : SimpleAuthManager ouvre passwords.json en « a+ » ; un volume Secret (lecture seule)
    monté directement fait planter l'api-server (PermissionError) -> copie dans un emptyDir."""
    tpl = (ROOT / "charts/airflow/templates/api-server.yaml").read_text()
    assert "{ name: auth, mountPath: /opt/airflow/auth }" in tpl
    assert "emptyDir: { medium: Memory" in tpl and "chown 50000:0 /auth/passwords.json" in tpl


def test_superset_pods_disable_service_links():
    """Régression : le Service « superset » injecterait SUPERSET_PORT=tcp://… dans le pod, lu comme port
    par gunicorn (CrashLoopBackOff « 'tcp' is not a valid port number »)."""
    for f in ("web.yaml", "init-job.yaml"):
        assert "enableServiceLinks: false" in (ROOT / "charts/superset/templates" / f).read_text(), f
