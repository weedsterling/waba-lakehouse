# Superset 6.1 + pilotes Trino (SQLAlchemy) et PostgreSQL (base de métadonnées) + Authlib (SSO OIDC Keycloak).
# L'image « lean » officielle n'embarque aucun pilote de base de données. La configuration et les
# tableaux de bord ne sont PAS dans l'image : ConfigMaps (k8s/charts/superset) -> modifiables sans rebuild.
#   docker build -t waba/superset:6.1.0-sso superset/      (fait automatiquement par scripts/k8s/deploy.sh)
FROM apache/superset:6.1.0

USER root
RUN set -eux; \
    PY=/app/.venv/bin/python; [ -x "$PY" ] || PY="$(command -v python3)"; \
    if command -v uv >/dev/null 2>&1; then \
      uv pip install --python "$PY" --no-cache trino==0.340.0 psycopg2-binary==2.9.10 authlib==1.8.0; \
    else \
      "$PY" -m pip --version >/dev/null 2>&1 || "$PY" -m ensurepip; \
      "$PY" -m pip install --no-cache-dir trino==0.340.0 psycopg2-binary==2.9.10 authlib==1.8.0; \
    fi; \
    "$PY" -c "import trino.sqlalchemy, psycopg2, authlib; print('pilotes Trino, PostgreSQL et Authlib OK')"
USER superset
