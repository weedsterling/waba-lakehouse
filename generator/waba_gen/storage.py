"""Accès MinIO (S3) + cache local des référentiels."""
from __future__ import annotations

import io
import logging
import re
import threading
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config

from . import config as C
from .referentials import Referentials

log = logging.getLogger(__name__)


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """CSV UTF-8, séparateur ',', valeurs nulles -> champ vide."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, date_format="%Y-%m-%d")
    return buf.getvalue().encode("utf-8")


class LakeStorage:
    """Encapsule l'écriture dans le bucket raw-landing.

    Organisation : raw-landing/<dataset>/<CC>/<prefix>_<CC>_<YYYYMMDD>_<NN>.csv
                   raw-landing/<referential>/<referential>.csv
    """

    def __init__(self, settings: C.StorageSettings | None = None):
        self.s = settings or C.StorageSettings()
        self._s3 = boto3.client(
            "s3",
            endpoint_url=self.s.endpoint,
            aws_access_key_id=self.s.access_key,
            aws_secret_access_key=self.s.secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                          retries={"max_attempts": 5, "mode": "standard"}),
            region_name="us-east-1",
        )
        self._seq: dict[tuple[str, str, str], int] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    def healthcheck(self) -> bool:
        try:
            self._s3.head_bucket(Bucket=self.s.raw_bucket)
            return True
        except Exception as exc:  # noqa: BLE001 - affiché dans l'UI
            log.error("MinIO indisponible: %s", exc)
            return False

    def put(self, key: str, payload: bytes) -> str:
        self._s3.put_object(Bucket=self.s.raw_bucket, Key=key, Body=payload, ContentType="text/csv")
        log.info("upload s3://%s/%s (%s octets)", self.s.raw_bucket, key, len(payload))
        return f"s3://{self.s.raw_bucket}/{key}"

    # ------------------------------------------------------------------ #
    def _next_seq(self, dataset: str, cc: str, day: str) -> int:
        """Numéro de séquence NN du jour, initialisé depuis raw-landing + archive."""
        k = (dataset, cc, day)
        with self._lock:
            if k not in self._seq:
                prefix = C.FILE_PREFIX[dataset]
                pattern = re.compile(rf"{prefix}_{cc}_{day}_(\d+)\.csv$")
                current = 0
                for bucket in (self.s.raw_bucket, "archive"):
                    try:
                        pages = self._s3.get_paginator("list_objects_v2").paginate(
                            Bucket=bucket, Prefix=f"{dataset}/{cc}/")
                        for page in pages:
                            for obj in page.get("Contents", []):
                                m = pattern.search(obj["Key"])
                                if m:
                                    current = max(current, int(m.group(1)))
                    except self._s3.exceptions.NoSuchBucket:
                        continue
                self._seq[k] = current
            self._seq[k] += 1
            return self._seq[k]

    def upload_transactions(self, dataset: str, frames: dict[str, pd.DataFrame],
                            run_date: datetime | None = None) -> list[str]:
        day = (run_date or datetime.now(timezone.utc)).strftime("%Y%m%d")
        keys = []
        for cc, df in frames.items():
            nn = self._next_seq(dataset, cc, day)
            name = f"{C.FILE_PREFIX[dataset]}_{cc}_{day}_{nn:02d}.csv"
            keys.append(self.put(f"{dataset}/{cc}/{name}", df_to_csv_bytes(df)))
        return keys

    def upload_referentials(self, ref: Referentials) -> list[str]:
        return [self.put(f"{name}/{name}.csv", df_to_csv_bytes(df)) for name, df in ref.as_dict().items()]


# --------------------------------------------------------------------------- #
# Cache local : les transactions sont générées à partir de la même version des
# référentiels que celle envoyée dans le lakehouse.
# --------------------------------------------------------------------------- #
class ReferentialCache:
    def __init__(self, cache_dir: str | None = None):
        self.dir = Path(cache_dir or C.StorageSettings().cache_dir)

    def exists(self) -> bool:
        return all((self.dir / f"{n}.parquet").exists() for n in C.REFERENTIAL_DATASETS)

    def save(self, ref: Referentials) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        for name, df in ref.as_dict().items():
            df.to_parquet(self.dir / f"{name}.parquet", index=False)

    def load(self) -> Referentials:
        if not self.exists():
            raise FileNotFoundError("Référentiels absents : générez-les d'abord (onglet Référentiels).")
        return Referentials(**{n: pd.read_parquet(self.dir / f"{n}.parquet") for n in C.REFERENTIAL_DATASETS})
