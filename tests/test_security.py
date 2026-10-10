"""Level 4 (9.6a) : cohérence de la sécurité as code — realm Keycloak, rôles Superset, règles Trino.

Le parcours complet (connexion Keycloak de chaque utilisateur de démo dans Superset 6.1, rôles obtenus,
tableaux de bord visibles, filtrage RLS par pays) a été vérifié sur Keycloak 26.0.7 + Superset 6.1.0 ;
ces tests empêchent les trois sources (realm, Superset, Trino) de diverger.
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "superset" / "dashboards"))
import waba_dashboards as W  # noqa: E402
import waba_security as S  # noqa: E402

REALM = json.loads((ROOT / "keycloak" / "waba-realm.json").read_text())
RULES = json.loads((ROOT / "trino" / "security" / "rules.json").read_text())
GROUPS = {g: set(u.split(",")) for g, u in
          (line.split(":") for line in (ROOT / "trino" / "security" / "groups.txt").read_text().split())}
REALM_ROLES = {r["name"] for r in REALM["roles"]["realm"]}
USERS = {u["username"]: set(u["realmRoles"]) for u in REALM["users"]}


def test_realm_contains_no_secret():
    for c in REALM["clients"]:
        assert re.fullmatch(r"\$\{[A-Z_]+\}", c["secret"]), c["clientId"]
    for u in REALM["users"]:
        assert all(re.fullmatch(r"\$\{[A-Z_]+\}", cred["value"]) for cred in u["credentials"])


def test_challenge_roles_and_country_scopes_exist():
    assert {"group_admin", "country_analyst", "compliance_officer", "viewer"} <= REALM_ROLES
    assert {f"country_{c}" for c in S.COUNTRIES} <= REALM_ROLES


def test_every_keycloak_role_maps_to_superset_and_back():
    assert set(S.ROLES_MAPPING) == REALM_ROLES


def test_every_demo_user_has_a_role_and_analysts_a_country():
    for name, roles in USERS.items():
        assert roles & REALM_ROLES, name
        if "country_analyst" in roles:
            assert any(r.startswith("country_") and r != "country_analyst" for r in roles), name


def test_trino_groups_match_keycloak_roles():
    for group, users in GROUPS.items():
        assert group in REALM_ROLES
        assert all(group in USERS[u] for u in users), group
    for name, roles in USERS.items():                       # chaque utilisateur est connu de Trino
        assert any(name in GROUPS.get(r, set()) for r in roles), name


def test_trino_rules_default_deny_and_viewer_never_reads_silver():
    for section in ("catalogs", "tables"):
        last = RULES[section][-1]
        assert set(last) <= {"catalog", "allow", "privileges"} and last.get("allow", "none") == "none" \
            and last.get("privileges", []) == []
    viewer = [r for r in RULES["tables"] if r.get("group") == "viewer"]
    assert viewer and all(r["schema"] == "gold" for r in viewer)


def test_trino_analysts_are_filtered_on_their_country():
    for name, roles in USERS.items():
        if "country_analyst" not in roles:
            continue
        country = next(r.split("_")[1] for r in roles if re.fullmatch(r"country_[A-Z]{2}", r))
        rule = next(r for r in RULES["tables"] if r.get("user") and re.fullmatch(r["user"], name))
        assert rule["filter"] == f"country_code = '{country}'"
        assert {c["name"] for c in rule["columns"] if not c["allow"]} >= {"iban_masked", "iban_hash"}


def test_superset_role_dashboards_exist_and_compliance_is_restricted():
    slugs = {d.slug for d in W.DASHBOARDS}
    for role, dashboards in S.ROLE_DASHBOARDS.items():
        assert set(dashboards) <= slugs, role
    assert S.ROLE_DASHBOARDS[S.COMPLIANCE] == ["waba-risque-conformite"]
    assert S.datasets_of(["waba-risque-conformite"]) == {
        "ds_npl_mensuel", "ds_loss_ratio_12m", "ds_aml_journalier", "ds_sinistres_sla"}


def test_superset_redirect_and_trino_https():
    clients = {c["clientId"]: c for c in REALM["clients"]}
    assert clients["superset"]["redirectUris"] == ["http://superset.waba.local/*"]
    assert clients["trino"]["redirectUris"] == ["https://trino.waba.local/*"]


def test_trino_client_does_not_require_pkce():
    """Régression : Keycloak refusait le retour OAuth2 de Trino (« Missing parameter: code_challenge_method »)."""
    clients = {c["clientId"]: c for c in REALM["clients"]}
    assert "pkce.code.challenge.method" not in clients["trino"]["attributes"]
    assert clients["superset"]["attributes"]["pkce.code.challenge.method"] == "S256"


def test_realm_sync_substitutes_variables(monkeypatch):
    sys.path.insert(0, str(ROOT / "keycloak"))
    import sync_realm

    monkeypatch.setenv("TRINO_OIDC_SECRET", 'a"b')
    assert sync_realm.substitute('{"secret": "${TRINO_OIDC_SECRET}"}') == '{"secret": "a\\"b"}'
