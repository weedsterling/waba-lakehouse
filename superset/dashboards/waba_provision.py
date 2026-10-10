"""Provisionnement Superset depuis le dépôt (Job d'init, à chaque déploiement) : base Trino, jeux de données,
graphiques, tableaux de bord, puis sécurité (rôles, droits, RLS).

Pourquoi pas seulement `superset import-dashboards` : cette commande n'écrase que les tableaux de bord ;
graphiques, jeux de données et base existants sont conservés tels quels (import_chart(..., overwrite=False)).
Une correction de SQL ou d'un graphique dans le dépôt ne serait jamais appliquée. Ici, chaque type d'objet est
importé avec sa propre commande en mode écrasement -> l'état Superset = l'état Git, sans doublon (UUID stables).

  python waba_provision.py --sqlalchemy-uri trino://superset@trino.serving.svc.cluster.local:8080/lakehouse
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import zipfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import waba_dashboards as W  # noqa: E402
import waba_security as S  # noqa: E402

# Type attendu dans metadata.yaml par chaque commande d'import Superset
STEPS = [("Database", "databases"), ("SqlaTable", "datasets"), ("Slice", "charts"), ("Dashboard", "dashboards")]


def contents_for(bundle: bytes, metadata_type: str) -> dict[str, str]:
    with zipfile.ZipFile(io.BytesIO(bundle)) as z:
        contents = {n.split("/", 1)[1]: z.read(n).decode() for n in z.namelist()}
    contents["metadata.yaml"] = contents["metadata.yaml"].replace("type: Dashboard", f"type: {metadata_type}")
    return contents


def provision(uri: str, owner: str = "admin") -> dict:
    from flask import g

    from superset import security_manager
    from superset.commands.chart.importers.dispatcher import ImportChartsCommand
    from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
    from superset.commands.database.importers.dispatcher import ImportDatabasesCommand
    from superset.commands.dataset.importers.dispatcher import ImportDatasetsCommand

    g.user = security_manager.find_user(username=owner)
    if g.user is None:
        raise SystemExit(f"utilisateur {owner} absent : lancer `superset fab create-admin` d'abord")
    bundle = W.build_bundle(uri)
    commands = {"Database": ImportDatabasesCommand, "SqlaTable": ImportDatasetsCommand,
                "Slice": ImportChartsCommand, "Dashboard": ImportDashboardsCommand}
    for metadata_type, label in STEPS:
        commands[metadata_type](contents_for(bundle, metadata_type), overwrite=True).run()
        print(f"import {label} : OK", flush=True)
    return S.apply()


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sqlalchemy-uri", default=W.DEFAULT_URI)
    p.add_argument("--owner", default="admin")
    a = p.parse_args(argv)
    from superset.app import create_app

    with create_app().app_context():
        print("sécurité appliquée :", provision(a.sqlalchemy_uri, a.owner))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
