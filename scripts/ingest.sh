#!/usr/bin/env bash
# Lance le job Spark d'ingestion raw-landing -> Iceberg raw.* (Linux / macOS / WSL)
set -euo pipefail
docker compose exec spark-master spark-submit /opt/waba/jobs/ingest_raw.py "$@"
