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
    "DASHBOARD_RBAC": True,              # accès par tableau de bord (rôles Keycloak, étape 9.6)
    "ENABLE_TEMPLATE_PROCESSING": False,  # pas de Jinja dans le SQL : surface d'injection réduite
}

# Caches en mémoire (1 réplique, pas de Redis) : résultats des graphiques 10 min, métadonnées 5 min.
# En production multi-réplique : Redis (CACHE_TYPE=RedisCache) et Celery pour les requêtes asynchrones.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300, "CACHE_KEY_PREFIX": "waba_"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 600, "CACHE_KEY_PREFIX": "waba_data_"}
RATELIMIT_STORAGE_URI = "memory://"

ROW_LIMIT = 50000
SQL_MAX_ROW = 100000
SUPERSET_WEBSERVER_TIMEOUT = 120
