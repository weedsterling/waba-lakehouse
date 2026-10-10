"""Sécurité Superset « as code » : rôles métier, droits par jeu de données / tableau de bord, Row Level Security.

Exécuté par le Job d'initialisation APRÈS l'import des tableaux de bord (python waba_security.py). Idempotent :
chaque exécution remet rôles, droits et filtres dans l'état déclaré ici (une modification manuelle dans l'UI
est écrasée — Git fait foi).

Correspondance avec Keycloak (realm waba, claim « groups » = rôles du realm), cf. superset_config.py :
  group_admin        -> Admin                              tout, y compris SQL Lab et administration
  country_analyst    -> WABA Analyste pays (+ WABA Pays XX)  3 tableaux de bord, lignes de SON pays uniquement
  compliance_officer -> WABA Conformité + sql_lab           tableau de bord Risque & conformité ; SQL Lab
                                                           limité aux schémas gold et reporting
  viewer             -> WABA Lecteur                        3 tableaux de bord (agrégats), ni SQL Lab ni export

Row Level Security : un rôle « WABA Pays XX » par pays, filtre country_code = 'XX' sur tous les jeux de données.
Les filtres d'un même group_key (« pays ») sont combinés en OU ; le rôle analyste porte en plus « 1 = 0 » dans
ce groupe : un analyste sans rôle pays ne voit AUCUNE ligne (refus par défaut, jamais tout).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import waba_dashboards as W  # noqa: E402

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
ANALYST, COMPLIANCE, VIEWER = "WABA Analyste pays", "WABA Conformité", "WABA Lecteur"
ALL_DASHBOARDS = [d.slug for d in W.DASHBOARDS]
RISK_DASHBOARD = "waba-risque-conformite"

# Rôle -> tableaux de bord accessibles (les jeux de données en découlent)
ROLE_DASHBOARDS = {ANALYST: ALL_DASHBOARDS, COMPLIANCE: [RISK_DASHBOARD], VIEWER: ALL_DASHBOARDS}
COMPLIANCE_SCHEMAS = ["gold", "reporting"]
# Droits retirés au lecteur : pas d'export des données brutes des graphiques
VIEWER_DENY = {("can_csv", "Superset"), ("can_export_csv", "SQLLab"), ("can_share_chart", "Superset")}

# Correspondance rôles Keycloak -> rôles Superset (importée par superset_config.py)
ROLES_MAPPING = {
    "group_admin": ["Admin"],
    "country_analyst": [ANALYST],
    "compliance_officer": [COMPLIANCE, "sql_lab"],
    "viewer": [VIEWER],
    **{f"country_{c}": [f"WABA Pays {c}"] for c in COUNTRIES},
}


def datasets_of(slugs: list[str]) -> set[str]:
    return {W.CH[n].dataset for d in W.DASHBOARDS if d.slug in slugs for row in d.rows for n in row}


def apply() -> dict:
    from superset import db
    from superset import security_manager as sm
    from superset.connectors.sqla.models import RowLevelSecurityFilter, SqlaTable
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard

    database = db.session.query(Database).filter_by(database_name=W.DATABASE_NAME).one()
    tables = {t.table_name: t for t in db.session.query(SqlaTable).filter_by(database_id=database.id)
              if t.table_name in W.DS}
    missing = set(W.DS) - set(tables)
    if missing:
        raise SystemExit(f"jeux de données absents (importer les tableaux de bord d'abord) : {sorted(missing)}")
    gamma = sm.find_role("Gamma")

    def pvm(perm: str, view: str):
        return sm.find_permission_view_menu(perm, view) or sm.add_permission_view_menu(perm, view)

    def role(name: str, perms: list) -> object:
        r = sm.find_role(name) or sm.add_role(name)
        r.permissions = list(dict.fromkeys(perms))      # état déclaré, dédoublonné (idempotence)
        return r

    base = list(gamma.permissions)
    roles = {}
    for name, slugs in ROLE_DASHBOARDS.items():
        perms = [p for p in base if not (name == VIEWER and (p.permission.name, p.view_menu.name) in VIEWER_DENY)]
        perms += [pvm("datasource_access", tables[ds].perm) for ds in sorted(datasets_of(slugs))]
        if name == COMPLIANCE:
            catalog = database.get_default_catalog()
            perms += [pvm("schema_access", sm.get_schema_perm(database.database_name, catalog, s))
                      for s in COMPLIANCE_SCHEMAS]
        roles[name] = role(name, perms)
    for c in COUNTRIES:
        roles[f"WABA Pays {c}"] = role(f"WABA Pays {c}", [])   # rôle porteur du seul filtre RLS

    # Row Level Security : filtres nommés « waba-rls-* », mis à jour sur place (pas de suppression en masse :
    # les tables d'association rôles/jeux de données doivent suivre)
    all_tables = [tables[n] for n in sorted(tables)]
    existing = {f.name: f for f in db.session.query(RowLevelSecurityFilter)
                .filter(RowLevelSecurityFilter.name.like("waba-rls-%"))}
    declared = set()

    def rls(name: str, clause: str, rls_roles: list, description: str) -> None:
        f = existing.get(name) or RowLevelSecurityFilter(name=name)
        f.filter_type, f.group_key, f.clause, f.description = "Regular", "pays", clause, description
        f.roles, f.tables = rls_roles, all_tables
        db.session.add(f)
        declared.add(name)

    rls("waba-rls-refus-par-defaut", "1 = 0", [roles[ANALYST]],
        "Analyste sans rôle pays : aucune ligne (combiné en OU avec les filtres pays)")
    for c in COUNTRIES:
        rls(f"waba-rls-pays-{c}", f"country_code = '{c}'", [roles[f"WABA Pays {c}"]], f"Périmètre pays {c}")
    for name in set(existing) - declared:
        db.session.delete(existing[name])                 # filtre retiré du code -> retiré de Superset

    # Visibilité des tableaux de bord (DASHBOARD_RBAC)
    for d in db.session.query(Dashboard).filter(Dashboard.slug.in_(ALL_DASHBOARDS)):
        d.roles = [roles[r] for r, slugs in ROLE_DASHBOARDS.items() if d.slug in slugs]
    db.session.commit()
    return {"roles": sorted(roles), "datasets": len(tables), "rls_filters": 1 + len(COUNTRIES)}


def main() -> int:
    from superset.app import create_app

    with create_app().app_context():
        print("sécurité Superset appliquée :", apply())
    return 0


if __name__ == "__main__":
    sys.exit(main())
