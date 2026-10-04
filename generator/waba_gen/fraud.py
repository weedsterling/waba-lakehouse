"""Scénarios de fraude / AML / liquidité injectés dans le flux continu (démo déterministe du Level 3).

Chaque scénario est construit à partir des générateurs normaux (identifiants, comptes et clients réels
des référentiels : aucune clé orpheline), puis modifié pour franchir exactement une règle :

  LARGE_TXN_BURST   3 virements de 600 000 à 900 000 XOF depuis le même compte en moins de 2 min
  AML_THRESHOLD     1 virement au-dessus du seuil déclaratif (1 000 000 XOF / 5 000 GHS)
  UNUSUAL_COUNTRY   2 paiements mobile money d'un client dans un pays qui n'est pas le sien
  CLAIM_GT_3X       1 prime mensuelle puis 1 sinistre de 40 x cette prime (> 3 x la prime annuelle)
  LIQUIDITY_RUN     (optionnel) 60 retraits de 100 M XOF dans un même pays en moins de 2 min
"""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from . import config as C
from .transactions import (
    ISO_FMT,
    _Pools,
    gen_bank_transactions,
    gen_insurance_operations,
    gen_mobile_money,
)

AML_THRESHOLD = {"XOF": 1_000_000, "GHS": 5_000}


def _local(xof: float, currency: str) -> float:
    return round(xof / C.XOF_PER_GHS, 2) if currency == "GHS" else float(xof)


def _ts(now: datetime, seconds_ago: list[int]) -> list[str]:
    return [(now - timedelta(seconds=int(s))).strftime(ISO_FMT) for s in seconds_ago]


def fraud_scenarios(p: _Pools, countries: list[str], now: datetime, rng: np.random.Generator,
                    bank_run: bool = False) -> dict[str, dict[str, pd.DataFrame]]:
    """{dataset: {pays: lignes à ajouter au micro-lot}}."""
    out: dict[str, dict[str, pd.DataFrame]] = {}

    def add(ds: str, cc: str, df: pd.DataFrame) -> None:
        cur = out.setdefault(ds, {})
        cur[cc] = pd.concat([cur[cc], df], ignore_index=True) if cc in cur else df

    win = (now - timedelta(seconds=120), now)
    banks = [c for c in countries if c in p.bank_accounts]
    if banks:
        cc = str(rng.choice(banks))
        cur = C.CURRENCY_MAP[cc]
        burst = gen_bank_transactions(p, cc, 4, *win, rng)
        burst.loc[:2, "account_id"] = burst.at[0, "account_id"]
        burst.loc[:2, "entity_type"] = burst.at[0, "entity_type"]
        burst["transaction_type"] = "TRANSFER"
        burst["transaction_status"] = "SUCCESS"
        amounts = [_local(x, cur) for x in rng.uniform(600_000, 900_000, 3)] + [AML_THRESHOLD[cur] * 1.5]
        burst["amount"] = amounts
        burst["fee_amount"] = np.round(burst["amount"] * 0.001, 0)
        burst["timestamp"] = _ts(now, [100, 60, 20, 10])
        add("bank_transactions", cc, burst)
        if bank_run:
            run_cc = str(rng.choice(banks))
            run = gen_bank_transactions(p, run_cc, 60, *win, rng)
            run["transaction_type"] = "WITHDRAWAL"
            run["beneficiary_account"] = run["account_id"]
            run["transaction_status"] = "SUCCESS"
            run["amount"] = _local(100_000_000, C.CURRENCY_MAP[run_cc])
            run["fee_amount"] = np.round(run["amount"] * 0.001, 0)
            run["timestamp"] = _ts(now, list(rng.integers(0, 110, 60)))
            add("bank_transactions", run_cc, run)

    mm = [c for c in countries if c in p.mm_customers]
    if mm:
        cc = str(rng.choice(mm))
        others = [c for c in p.mm_customers if c != cc]
        if others:
            home = str(rng.choice(others))
            pay = gen_mobile_money(p, cc, 2, *win, rng)
            pay["sender_id"] = rng.choice(p.mm_customers[home], 2)    # client d'un autre pays
            pay["status"] = "SUCCESS"
            pay["timestamp"] = _ts(now, [40, 5])
            add("mobile_money_payments", cc, pay)

    ins = [c for c in countries if c in p.policies]
    if ins:
        cc = str(rng.choice(ins))
        ops = gen_insurance_operations(p, cc, 2, *win, rng)
        for col in ("customer_id", "account_id", "product_line"):
            ops.at[1, col] = ops.at[0, col]
        premium = float(ops.at[0, "amount"])
        ops["operation_type"] = ["PREMIUM_PAYMENT", "CLAIM_SUBMISSION"]
        ops["amount"] = [premium, round(premium * 40, 2)]   # > 3 x prime annuelle (12 mensualités)
        ops["claim_status"] = [None, "PENDING"]
        ops["processing_days"] = pd.array([pd.NA, 3], dtype="Int64")
        ops["timestamp"] = _ts(now, [90, 15])
        add("insurance_operations", cc, ops)
    return out
