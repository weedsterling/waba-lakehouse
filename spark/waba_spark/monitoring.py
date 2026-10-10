"""Observabilité des flux Spark : progression de chaque requête publiée dans un groupe de consommateurs Kafka.

Spark Structured Streaming ne rejoint aucun groupe Kafka : sa progression vit dans le checkpoint (MinIO). Les
outils standard (kafka-exporter, Grafana) ne voient donc aucun « consumer lag ». Ce listener publie, après chaque
micro-lot, les offsets traités par chaque requête dans le groupe ``waba-spark-<requête>`` :

  * groupe sans membre actif : simple marque-page, jamais utilisé pour lire (la reprise exacte reste pilotée par
    le checkpoint -> exactement-une-fois inchangé) ;
  * lag = fin du topic - offset traité, mesuré côté Kafka : il continue de croître quand le job est arrêté ou en
    erreur, ce qu'une métrique émise par le job lui-même ne pourrait pas montrer.

Alerte « lag consumer AML > 5000 » : groupe ``waba-spark-rules`` (requête « rules » de stream_silver_to_gold, qui
produit gold-aml-events). Les commits sont « best effort » : un échec est journalisé, jamais propagé au flux.
"""
from __future__ import annotations

import json
import time
from collections.abc import Iterable

from pyspark.sql.streaming import StreamingQueryListener

from .common import get_logger

GROUP_PREFIX = "waba-spark-"
log = get_logger("waba.monitoring")


def group_id(query_name: str) -> str:
    return f"{GROUP_PREFIX}{query_name}"


def kafka_offsets(sources: Iterable) -> dict[str, dict[int, int]]:
    """Offsets traités (endOffset) des sources Kafka d'une progression : {topic: {partition: offset}}.

    endOffset Kafka = prochain offset à lire, soit exactement la sémantique d'un offset committé."""
    offsets: dict[str, dict[int, int]] = {}
    for s in sources:
        if not str(getattr(s, "description", "")).startswith("KafkaV2") or not getattr(s, "endOffset", None):
            continue
        end = json.loads(s.endOffset) if isinstance(s.endOffset, str) else s.endOffset
        for topic, parts in end.items():
            offsets.setdefault(topic, {}).update({int(p): int(o) for p, o in parts.items()})
    return offsets


class KafkaOffsetCommitter(StreamingQueryListener):
    """Publie la progression de chaque requête nommée dans Kafka (API Admin du client Kafka de la JVM Spark)."""

    def __init__(self, spark, bootstrap: str, timeout_s: int = 10):
        self._jvm, self._bootstrap, self._timeout = spark._jvm, bootstrap, timeout_s
        self._admin = None
        self._last_error = 0.0

    def _client(self):
        if self._admin is None:
            props = self._jvm.java.util.Properties()
            props.put("bootstrap.servers", self._bootstrap)
            props.put("client.id", "waba-offset-committer")
            self._admin = self._jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
        return self._admin

    def commit(self, group: str, offsets: dict[str, dict[int, int]]) -> None:
        jvm = self._jvm
        java_map = jvm.java.util.HashMap()
        for topic, parts in offsets.items():
            for partition, offset in parts.items():
                java_map.put(jvm.org.apache.kafka.common.TopicPartition(topic, partition),
                             jvm.org.apache.kafka.clients.consumer.OffsetAndMetadata(offset))
        (self._client().alterConsumerGroupOffsets(group, java_map).all()
         .get(self._timeout, jvm.java.util.concurrent.TimeUnit.SECONDS))

    def onQueryStarted(self, event) -> None:  # noqa: N802 (API PySpark)
        pass

    def onQueryProgress(self, event) -> None:  # noqa: N802
        progress = event.progress
        offsets = kafka_offsets(progress.sources)
        if not progress.name or not offsets:
            return
        try:
            self.commit(group_id(progress.name), offsets)
        except Exception as exc:  # noqa: BLE001 - la supervision ne doit jamais casser le flux
            self._admin = None                                  # client recréé au prochain micro-lot
            if time.monotonic() - self._last_error > 60:        # au plus un message par minute
                self._last_error = time.monotonic()
                log.warning("publication des offsets impossible", extra={"ctx": {
                    "event": "offset_commit_failed", "group": group_id(progress.name), "error": str(exc)[:300]}})

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        pass


def install(spark, bootstrap: str) -> KafkaOffsetCommitter:
    """À appeler une fois par application, avant le démarrage des requêtes."""
    listener = KafkaOffsetCommitter(spark, bootstrap)
    spark.streams.addListener(listener)
    return listener
