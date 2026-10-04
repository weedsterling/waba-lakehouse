"""Mode de génération continue : un thread d'arrière-plan émet des micro-lots
toutes les N secondes (10-60 s) pour simuler un flux temps réel."""
from __future__ import annotations

import logging
import threading
from collections import deque
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from .fraud import fraud_scenarios
from .referentials import Referentials
from .storage import LakeStorage
from .transactions import Pools, generate_transactions

log = logging.getLogger(__name__)


class ContinuousGenerator:
    def __init__(self, storage: LakeStorage, ref: Referentials):
        self.storage = storage
        self.ref = ref
        self.pools = Pools(ref)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.history: deque[str] = deque(maxlen=200)
        self.batches = 0
        self.rows = 0

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, datasets: list[str], countries: list[str], rows_per_batch: int,
              min_interval: int, max_interval: int, anomaly_rate: float = 0.0,
              fraud: bool = False) -> None:
        if self.running:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="waba-continuous", daemon=True,
            args=(datasets, countries, rows_per_batch, min_interval, max_interval, anomaly_rate, fraud))
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self, datasets, countries, rows_per_batch, min_iv, max_iv, anomaly_rate, fraud) -> None:
        rng = np.random.default_rng()
        lot = 0
        while not self._stop.is_set():
            lot += 1
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            try:
                # Scénarios de démonstration : fraude + AML à chaque micro-lot, ruée sur les retraits
                # au 1er micro-lot puis tous les 5 (démo déterministe de l'alerte de liquidité)
                extra = fraud_scenarios(self.pools, countries, now, rng, bank_run=lot % 5 == 1) if fraud else {}
                for ds in datasets:
                    frames = generate_transactions(
                        self.ref, ds, countries, rows_per_batch,
                        start=now - timedelta(seconds=max_iv), end=now,
                        anomaly_rate=anomaly_rate, seed=int(rng.integers(0, 2**31)), pools=self.pools)
                    for cc, df in extra.get(ds, {}).items():
                        frames[cc] = pd.concat([frames[cc], df], ignore_index=True) if cc in frames else df
                    keys = self.storage.upload_transactions(ds, frames)
                    self.batches += 1
                    self.rows += sum(len(f) for f in frames.values())
                    self.history.appendleft(f"{now:%H:%M:%S} · {ds} · {len(keys)} fichier(s)")
            except Exception as exc:  # noqa: BLE001 - le thread ne doit jamais mourir silencieusement
                log.exception("Erreur de génération continue")
                self.history.appendleft(f"{now:%H:%M:%S} · ERREUR · {exc}")
            self._stop.wait(int(rng.integers(min_iv, max_iv + 1)))
