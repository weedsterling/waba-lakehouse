"""Configuration Superset WABA (montée par ConfigMap, SUPERSET_CONFIG_PATH). Aucun secret ici :
clé de session et mot de passe de la base viennent du Secret Kubernetes waba-superset (.env)."""
import os
from urllib.parse import quote_plus

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{quote_plus(os.environ['SUPERSET_DB_PASSWORD'])}"
    f"@{os.environ.get('SUPERSET_DB_HOST', 'superset-postgres')}:5432/superset"
)
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Derrière l'Ingress NGINX : en-têtes X-Forwarded-* (schéma, hôte) pris en compte
ENABLE_PROXY_FIX = True

FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True,              # visibilité par tableau de bord selon le rôle (waba_security.py)
    "ENABLE_TEMPLATE_PROCESSING": False,  # pas de Jinja dans le SQL : surface d'injection réduite
}

# --------------------------------------------------------------------------- SSO Keycloak (étape 9.6)
# Activé quand KEYCLOAK_URL est défini (Kubernetes). Les rôles Superset sont recalculés à CHAQUE connexion
# depuis les rôles du realm (claim « groups ») : retirer un rôle dans Keycloak retire l'accès dans Superset.
# Utilisateur sans rôle reconnu -> rôle Public, sans aucun droit (refus par défaut).
KEYCLOAK_URL = os.environ.get("KEYCLOAK_URL")
if KEYCLOAK_URL:
    import sys

    from flask_appbuilder.security.manager import AUTH_OAUTH

    sys.path.insert(0, os.environ.get("WABA_DASHBOARDS_DIR", "/app/waba/dashboards"))
    from waba_security import ROLES_MAPPING  # noqa: E402  (même source que les rôles créés)

    _realm = f"{KEYCLOAK_URL}/realms/{os.environ.get('KEYCLOAK_REALM', 'waba')}"
    AUTH_TYPE = AUTH_OAUTH
    OAUTH_PROVIDERS = [{
        "name": "keycloak", "icon": "fa-key", "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ.get("SUPERSET_OIDC_CLIENT_ID", "superset"),
            "client_secret": os.environ["SUPERSET_OIDC_SECRET"],
            "server_metadata_url": f"{_realm}/.well-known/openid-configuration",
            "api_base_url": f"{_realm}/protocol/",          # + openid-connect/userinfo (provider keycloak)
            "client_kwargs": {"scope": "openid profile email", "code_challenge_method": "S256"},
        },
    }]
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Public"
    AUTH_ROLES_SYNC_AT_LOGIN = True
    AUTH_ROLES_MAPPING = ROLES_MAPPING

# Caches en mémoire (1 réplique, pas de Redis) : résultats des graphiques 10 min, métadonnées 5 min.
# En production multi-réplique : Redis (CACHE_TYPE=RedisCache) et Celery pour les requêtes asynchrones.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300, "CACHE_KEY_PREFIX": "waba_"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 600, "CACHE_KEY_PREFIX": "waba_data_"}
RATELIMIT_STORAGE_URI = "memory://"

ROW_LIMIT = 50000
SQL_MAX_ROW = 100000
SUPERSET_WEBSERVER_TIMEOUT = 120
