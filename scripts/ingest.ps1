# Lance le job Spark d'ingestion raw-landing -> Iceberg raw.* (Windows PowerShell)
# Exemples :
#   .\scripts\ingest.ps1
#   .\scripts\ingest.ps1 --datasets bank_transactions --countries CI,SN
#   .\scripts\ingest.ps1 --source archive --no-archive     # rejouer : test d'idempotence
docker compose exec spark-master spark-submit /opt/waba/jobs/ingest_raw.py @args
exit $LASTEXITCODE
