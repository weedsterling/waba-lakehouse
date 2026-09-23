"""Job PySpark Level 1 : raw-landing (CSV) -> tables Iceberg raw.*

Étapes par jeu de données :
  1. liste les CSV présents dans s3://raw-landing/<dataset>/[<pays>/]
  2. lecture à schéma explicite (mode PERMISSIVE, lignes corrompues tracées)
  3. validation (champs obligatoires, domaines, montants >= 0, devise/pays)
  4. dédoublonnage intra-lot + masquage PII (IBAN) + colonnes techniques
  5. MERGE idempotent dans lakehouse.raw.<dataset> (clé = *_id)
  6. rejets -> audit.rejected_records ; métriques -> audit.ingestion_log
  7. fichiers traités déplacés vers le bucket `archive`

Les référentiels sont toujours traités avant les transactions.

Exemples :
  spark-submit jobs/ingest_raw.py                               # tout
  spark-submit jobs/ingest_raw.py --datasets bank_transactions --countries CI,SN
  spark-submit jobs/ingest_raw.py --source archive --no-archive # rejouer (test d'idempotence)
"""
from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import Row

from waba_spark import iceberg
from waba_spark import validation as V
from waba_spark.common import ObjectStore, build_spark, get_logger
from waba_spark.schemas import COUNTRIES, REFERENTIALS, SPECS, TRANSACTIONS

log = get_logger("waba.ingest_raw")


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--datasets", default="all",
                   help="Liste séparée par des virgules (défaut: all). Ex: customers,bank_transactions")
    p.add_argument("--countries", default="all", help="Filtre pays pour les flux transactionnels (ex: CI,SN)")
    p.add_argument("--source", choices=["landing", "archive"], default="landing",
                   help="`archive` permet de rejouer des fichiers déjà ingérés (démonstration d'idempotence)")
    p.add_argument("--no-archive", action="store_true", help="Ne pas déplacer les fichiers après ingestion")
    p.add_argument("--namespace", default="raw",
                   help="Espace de noms Iceberg cible (raw au Level 1, bronze au Level 2)")
    p.add_argument("--landing-bucket", default="raw-landing")
    p.add_argument("--archive-bucket", default="archive")
    args = p.parse_args(argv)

    requested = list(SPECS) if args.datasets == "all" else [d.strip() for d in args.datasets.split(",")]
    unknown = set(requested) - set(SPECS)
    if unknown:
        p.error(f"datasets inconnus: {sorted(unknown)}")
    # ordre imposé : référentiels puis transactions
    args.datasets = [d for d in REFERENTIALS + TRANSACTIONS if d in requested]
    args.countries = COUNTRIES if args.countries == "all" else [c.strip().upper() for c in args.countries.split(",")]
    if set(args.countries) - set(COUNTRIES):
        p.error(f"pays inconnus: {sorted(set(args.countries) - set(COUNTRIES))}")
    return args


def _country_from_key(key: str) -> str | None:
    parts = key.split("/")
    return parts[1] if len(parts) >= 3 and parts[1] in COUNTRIES else None


def ingest_dataset(spark, store: ObjectStore, name: str, args, batch_id: str) -> dict:
    spec = SPECS[name]
    bucket = args.archive_bucket if args.source == "archive" else args.landing_bucket
    if spec.is_referential:
        objects = store.list_csv(bucket, f"{name}/")
    else:
        objects = [o for cc in args.countries for o in store.list_csv(bucket, f"{name}/{cc}/")]
    ctx = {"dataset": name, "namespace": args.namespace, "batch_id": batch_id, "files": len(objects)}
    if not objects:
        log.info("aucun fichier à ingérer", extra={"ctx": ctx})
        return {"dataset": name, "files": 0}

    started = datetime.now(timezone.utc).replace(tzinfo=None)
    log.info("début ingestion", extra={"ctx": ctx})
    try:
        raw = V.read_csv(spark, [o.s3a for o in objects], spec)
        valid, rejected = V.validate(raw, spec)
        clean = V.add_technical_columns(V.mask_pii(V.deduplicate(valid, spec), spec), batch_id)

        iceberg.ensure_table(spark, clean, spec, args.namespace)
        inserted = iceberg.merge_idempotent(spark, clean, spec, args.namespace)
        iceberg.append(spark, V.build_rejects(rejected, spec, batch_id), "audit.rejected_records")
        stats = V.file_stats(raw, valid, rejected)
        status, error = "SUCCESS", None
    except Exception as exc:  # noqa: BLE001 - tracé puis relancé
        stats, status, error, inserted = {}, "FAILED", str(exc)[:2000], 0
        log.exception("échec ingestion", extra={"ctx": ctx})
    finally:
        ended = datetime.now(timezone.utc).replace(tzinfo=None)

    log_rows = []
    for o in objects:
        s = stats.get(o.s3a, {"rows_read": 0, "rows_valid": 0, "rows_rejected": 0})
        log_rows.append(Row(batch_id=batch_id, dataset=name, source_file=o.s3a, etag=o.etag,
                            country_code=_country_from_key(o.key), rows_read=s["rows_read"],
                            rows_valid=s["rows_valid"], rows_rejected=s["rows_rejected"], status=status,
                            error=error, started_at=started, ended_at=ended))
    spark.createDataFrame(log_rows, schema=spark.table(iceberg.fq("audit.ingestion_log")).schema) \
         .writeTo(iceberg.fq("audit.ingestion_log")).append()

    if status == "FAILED":
        raise RuntimeError(f"Ingestion {name} en échec : {error}")

    if not args.no_archive and args.source == "landing":
        for o in objects:
            store.move(o, args.archive_bucket)

    summary = {"dataset": name, "files": len(objects), "inserted": inserted,
               "rows_read": sum(s["rows_read"] for s in stats.values()),
               "rows_rejected": sum(s["rows_rejected"] for s in stats.values()),
               "duration_s": round((ended - started).total_seconds(), 1)}
    log.info("fin ingestion", extra={"ctx": {**ctx, **summary}})
    return summary


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    batch_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    spark = build_spark("waba-ingest-raw")
    store = ObjectStore()
    iceberg.ensure_namespaces(spark, args.namespace)
    iceberg.ensure_audit_tables(spark)

    results, failures = [], []
    for name in args.datasets:
        try:
            results.append(ingest_dataset(spark, store, name, args, batch_id))
        except Exception as exc:  # noqa: BLE001 - on continue les autres datasets
            failures.append(name)
            log.error("dataset en échec", extra={"ctx": {"dataset": name, "error": str(exc)[:500]}})
        finally:
            spark.catalog.clearCache()

    log.info("batch terminé", extra={"ctx": {"batch_id": batch_id, "results": results, "failures": failures}})
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
