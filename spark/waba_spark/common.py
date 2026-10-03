"""Utilitaires transverses : logging JSON, SparkSession, accès S3 (MinIO)."""
from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import boto3
from botocore.client import Config
from pyspark.sql import SparkSession

CATALOG = os.environ.get("ICEBERG_CATALOG", "lakehouse")


# --------------------------------------------------------------------------- #
# Logging structuré
# --------------------------------------------------------------------------- #
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {"ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"), "level": record.levelname,
                   "logger": record.name, "msg": record.getMessage()}
        payload.update(getattr(record, "ctx", {}))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(JsonFormatter())
        logger.addHandler(h)
        logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
        logger.propagate = False
    return logger


# --------------------------------------------------------------------------- #
# Spark
# --------------------------------------------------------------------------- #
def build_spark(app_name: str) -> SparkSession:
    """La configuration statique (catalogue Iceberg REST, S3A) vit dans
    spark/conf/spark-defaults.conf ; les secrets sont lus depuis l'environnement."""
    builder = SparkSession.builder.appName(app_name)
    access, secret = os.environ.get("AWS_ACCESS_KEY_ID"), os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access and secret:
        builder = (builder
                   .config("spark.hadoop.fs.s3a.access.key", access)
                   .config("spark.hadoop.fs.s3a.secret.key", secret))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------------------------------- #
# S3 / MinIO
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class S3Object:
    bucket: str
    key: str
    etag: str
    size: int
    last_modified: datetime | None = None

    @property
    def s3a(self) -> str:
        return f"s3a://{self.bucket}/{self.key}"


class ObjectStore:
    def __init__(self):
        self.client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                          retries={"max_attempts": 5, "mode": "standard"}),
        )

    def list_csv(self, bucket: str, prefix: str, min_age: timedelta = timedelta(0)) -> list[S3Object]:
        """Fichiers CSV non vides. `min_age` : ignore les objets plus récents (architecture Lambda :
        laisse à NiFi le temps de lire un fichier avant que le batch ne l'archive)."""
        cutoff = datetime.now(timezone.utc) - min_age
        out = []
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                if o["Key"].endswith(".csv") and o["Size"] > 0 and o["LastModified"] <= cutoff:
                    out.append(S3Object(bucket, o["Key"], o["ETag"].strip('"'), o["Size"], o["LastModified"]))
        return sorted(out, key=lambda o: o.key)

    def move(self, obj: S3Object, dest_bucket: str) -> None:
        """Copie puis suppression (S3 n'a pas de rename atomique). La copie est
        idempotente : relancer après un échec partiel ne crée pas de doublon."""
        self.client.copy_object(Bucket=dest_bucket, Key=obj.key,
                                CopySource={"Bucket": obj.bucket, "Key": obj.key})
        self.client.delete_object(Bucket=obj.bucket, Key=obj.key)
