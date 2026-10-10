# Client OpenMetadata (SDK + connecteur Trino) pour le catalogue as code (openmetadata/catalog.py).
# Version du SDK = version du serveur (1.12.14). Le code est monté par ConfigMap, pas copié dans l'image.
#   docker build -t waba/om-catalog:1.12.14 openmetadata/      (fait automatiquement par scripts/k8s/deploy.sh)
FROM python:3.11-slim
RUN pip install --no-cache-dir "openmetadata-ingestion[trino]==1.12.14.2" "cachetools>=5,<6" \
 && python -c "import metadata.ingestion.source.database.trino.metadata; print('SDK OpenMetadata + Trino OK')" \
 && useradd --uid 1000 --create-home catalog
USER 1000
