"""Régression : les jobs Spark doivent être importables SANS SparkSession active
(aucune expression Column évaluée au niveau module), comme lors de spark-submit."""
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
ROOT = Path(__file__).resolve().parents[1]
JOBS = sorted((ROOT / "spark" / "jobs").glob("*.py"))


@pytest.mark.parametrize("job", JOBS, ids=[j.stem for j in JOBS])
def test_job_module_imports_without_spark(job):
    code = (f"import importlib.util as u; s = u.spec_from_file_location('j', r'{job}'); "
            "m = u.module_from_spec(s); s.loader.exec_module(m); print('ok')")
    env = {**os.environ, "PYTHONPATH": str(ROOT / "spark")}
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
