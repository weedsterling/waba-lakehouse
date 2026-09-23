"""Génération vectorisée des flux transactionnels (4 types) à partir des référentiels.

Garanties :
* toutes les clés (account_id, customer_id, branch_id, sender/receiver_id...) sont
  tirées des référentiels -> aucune clé orpheline ;
* devise cohérente avec le pays (XOF zone UEMOA, GHS Ghana) ;
* distributions réalistes (NPL 3-8 %, loss ratio 50-85 % selon le pays) ;
* injection optionnelle et contrôlée d'anomalies (lignes malformées, doublons)
  pour démontrer la validation et l'idempotence côté Spark.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime

import numpy as np
import pandas as pd

from . import config as C
from .referentials import Referentials

log = logging.getLogger(__name__)

ISO_FMT = "%Y-%m-%dT%H:%M:%S"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _uuids(n: int, rng: np.random.Generator) -> list[str]:
    """UUID v4 reproductibles (dérivés du RNG) pour faciliter les tests."""
    raw = rng.integers(0, 2**63, size=(n, 2), dtype=np.int64)
    return [str(uuid.UUID(int=(int(a) << 64 | int(b)) & ((1 << 128) - 1), version=4)) for a, b in raw]


def _timestamps(start: datetime, end: datetime, n: int, rng: np.random.Generator) -> pd.Series:
    span = max(int((end - start).total_seconds()), 1)
    secs = rng.integers(0, span, size=n)
    return pd.Series(pd.Timestamp(start) + pd.to_timedelta(secs, unit="s")).dt.strftime(ISO_FMT)


def _amounts(n: int, currency: str, rng: np.random.Generator, mean: float, sigma: float,
             floor_xof: float) -> np.ndarray:
    xof = np.maximum(floor_xof, rng.lognormal(mean=mean, sigma=sigma, size=n))
    return np.round(xof / C.XOF_PER_GHS if currency == "GHS" else xof, 2)


def split_rows(n: int, countries: list[str], rng: np.random.Generator) -> dict[str, int]:
    """Répartit n lignes entre les pays sélectionnés au prorata de leur poids."""
    if not countries:
        return {}
    w = np.array([C.COUNTRY_WEIGHTS[c] for c in countries], dtype=float)
    return dict(zip(countries, rng.multinomial(n, w / w.sum()).tolist(), strict=True))


class _Pools:
    """Index pré-calculés des référentiels (évite de filtrer 800k lignes à chaque appel)."""

    def __init__(self, ref: Referentials):
        acc = ref.accounts
        active = acc[acc["status"].isin(["ACTIVE", "DORMANT"])]
        self.bank_accounts = {cc: g for cc, g in active[
            active["account_type"].isin(["CURRENT", "SAVINGS"])
            & active["entity_type"].isin(["BANK", "MICROFINANCE"])].groupby("country_code")}
        self.loan_accounts = {cc: g for cc, g in acc[
            (acc["account_type"] == "LOAN") & (acc["status"] != "CLOSED")].groupby("country_code")}
        self.policies = {cc: g for cc, g in acc[
            (acc["account_type"] == "INSURANCE_POLICY") & (acc["status"] != "CLOSED")].groupby("country_code")}
        cust = ref.customers
        self.mm_customers = {cc: g["customer_id"].to_numpy()
                             for cc, g in cust[cust["entity_type"] == "MOBILE_MONEY"].groupby("country_code")}
        self.all_customers = {cc: g["customer_id"].to_numpy() for cc, g in cust.groupby("country_code")}
        self.branches = {cc: g for cc, g in ref.branches[ref.branches["is_active"]].groupby("country_code")}


# --------------------------------------------------------------------------- #
# Générateurs par type
# --------------------------------------------------------------------------- #
def gen_bank_transactions(p: _Pools, cc: str, n: int, start: datetime, end: datetime,
                          rng: np.random.Generator) -> pd.DataFrame:
    accs = p.bank_accounts.get(cc)
    if accs is None or n == 0:
        return pd.DataFrame()
    cur = C.CURRENCY_MAP[cc]
    idx = rng.integers(0, len(accs), n)
    debit = accs.iloc[idx]
    ttype = rng.choice(C.TXN_TYPES, size=n, p=C.TXN_TYPE_PROBS)
    benef = accs["account_id"].to_numpy()[rng.integers(0, len(accs), n)]
    # Retrait / dépôt : le bénéficiaire est le compte lui-même
    benef = np.where(np.isin(ttype, ["WITHDRAWAL", "DEPOSIT"]), debit["account_id"].to_numpy(), benef)
    status = rng.choice(C.TXN_STATUSES, size=n, p=C.TXN_STATUS_PROBS)
    amount = _amounts(n, cur, rng, mean=12, sigma=1.5, floor_xof=500)
    amount = np.where(ttype == "INTERNATIONAL_WIRE", np.round(amount * 4, 2), amount)
    br = p.branches[cc]["branch_id"].to_numpy()
    return pd.DataFrame({
        "transaction_id": _uuids(n, rng),
        "timestamp": _timestamps(start, end, n, rng),
        "account_id": debit["account_id"].to_numpy(),
        "beneficiary_account": benef,
        "branch_id": br[rng.integers(0, len(br), n)],
        "country_code": cc,
        "transaction_type": ttype,
        "amount": amount,
        "currency": cur,
        "channel": rng.choice(C.CHANNELS, size=n, p=C.CHANNEL_PROBS),
        "transaction_status": status,
        "fee_amount": np.where(status == "SUCCESS", np.round(amount * 0.001, 0), 0.0),
        "entity_type": debit["entity_type"].to_numpy(),
    })


def gen_insurance_operations(p: _Pools, cc: str, n: int, start: datetime, end: datetime,
                             rng: np.random.Generator) -> pd.DataFrame:
    pol = p.policies.get(cc)
    if pol is None or n == 0:
        return pd.DataFrame()
    cur = C.CURRENCY_MAP[cc]
    sel = pol.iloc[rng.integers(0, len(pol), n)]
    lines = C.PRODUCT_LINES_GH if cc == "GH" else C.PRODUCT_LINES_UEMOA
    product_line = rng.choice(lines, size=n)
    op = rng.choice(C.INSURANCE_OP_TYPES, size=n, p=C.INSURANCE_OP_PROBS)

    amount = _amounts(n, cur, rng, mean=11.0, sigma=0.7, floor_xof=5_000)           # primes
    claim_est = _amounts(n, cur, rng, mean=12.3, sigma=1.0, floor_xof=20_000)       # sinistres
    amount = np.where(np.isin(op, ["CLAIM_SUBMISSION", "CLAIM_PAYMENT"]), claim_est, amount)
    amount = np.where(op == "POLICY_CANCELLATION", np.round(amount * 0.2, 2), amount)  # remboursement prorata

    claim_status = np.full(n, None, dtype=object)
    is_sub, is_pay = op == "CLAIM_SUBMISSION", op == "CLAIM_PAYMENT"
    claim_status[is_sub] = rng.choice(C.CLAIM_STATUSES_SUBMISSION, size=is_sub.sum(),
                                      p=C.CLAIM_STATUSES_SUBMISSION_PROBS)
    claim_status[is_pay] = "PAID"

    is_iard = np.char.startswith(product_line.astype(str), "IARD")
    days = np.where(is_iard, rng.poisson(9, n) + 2, rng.poisson(22, n) + 5)
    processing_days = pd.array(np.where(is_sub | is_pay, days, 0), dtype="Int64")
    processing_days[~(is_sub | is_pay)] = pd.NA

    df = pd.DataFrame({
        "operation_id": _uuids(n, rng),
        "timestamp": _timestamps(start, end, n, rng),
        "customer_id": sel["customer_id"].to_numpy(),
        "account_id": sel["account_id"].to_numpy(),
        "country_code": cc,
        "operation_type": op,
        "product_line": product_line,
        "amount": amount,
        "currency": cur,
        "claim_status": claim_status,
        "processing_days": processing_days,
        "entity_type": "INSURANCE",
    })
    return _calibrate_loss_ratio(df, cc, rng)


def _calibrate_loss_ratio(df: pd.DataFrame, cc: str, rng: np.random.Generator) -> pd.DataFrame:
    """Ajuste les sinistres payés pour que sinistres/primes ≈ cible pays (±8 %) par produit."""
    target = C.TARGET_LOSS_RATIO[cc]
    for _line, g in df.groupby("product_line"):
        premiums = g.loc[g["operation_type"].isin(["PREMIUM_PAYMENT", "POLICY_RENEWAL"]), "amount"].sum()
        pay_idx = g.index[g["operation_type"] == "CLAIM_PAYMENT"]
        paid = df.loc[pay_idx, "amount"].sum()
        if premiums > 0 and paid > 0:
            lr = float(np.clip(target * rng.uniform(0.92, 1.08), 0.5, 0.85))
            df.loc[pay_idx, "amount"] = np.round(df.loc[pay_idx, "amount"] * (lr * premiums / paid), 2)
    return df


def gen_mobile_money(p: _Pools, cc: str, n: int, start: datetime, end: datetime,
                     rng: np.random.Generator) -> pd.DataFrame:
    senders = p.mm_customers.get(cc)
    if senders is None or n == 0:
        if n:
            log.warning("Pas d'activité Mobile Money en %s : %s lignes ignorées", cc, n)
        return pd.DataFrame()
    cur = C.CURRENCY_MAP[cc]
    ptype = rng.choice(C.MM_PAYMENT_TYPES, size=n, p=C.MM_PAYMENT_PROBS)
    if cc not in C.CROSS_BORDER_CORRIDORS:
        ptype = np.where(ptype == "CROSS_BORDER_TRANSFER", "P2P", ptype)

    receiver_country = np.full(n, cc, dtype=object)
    xb = ptype == "CROSS_BORDER_TRANSFER"
    if xb.any():
        receiver_country[xb] = rng.choice(C.CROSS_BORDER_CORRIDORS[cc], size=xb.sum())

    receiver_id = np.empty(n, dtype=object)
    for rc in np.unique(receiver_country):
        m = receiver_country == rc
        pool = p.mm_customers.get(rc, p.all_customers[rc])
        receiver_id[m] = pool[rng.integers(0, len(pool), m.sum())]

    amount = _amounts(n, cur, rng, mean=9.6, sigma=1.2, floor_xof=100)
    amount = np.where(xb, np.round(amount * 5, 2), amount)
    fee_rate = np.select([xb, ptype == "P2P"], [0.015, 0.005], default=0.0)
    return pd.DataFrame({
        "payment_id": _uuids(n, rng),
        "timestamp": _timestamps(start, end, n, rng),
        "sender_id": senders[rng.integers(0, len(senders), n)],
        "receiver_id": receiver_id,
        "sender_country": cc,
        "receiver_country": receiver_country,
        "amount": amount,
        "currency": cur,
        "payment_type": ptype,
        "operator": rng.choice(C.MM_OPERATORS, size=n, p=C.MM_OPERATOR_PROBS),
        "status": rng.choice(C.MM_STATUSES, size=n, p=C.MM_STATUS_PROBS),
        "fee_amount": np.round(amount * fee_rate, 2),
        "entity_type": "MOBILE_MONEY",
        "country_code": cc,  # champ commun obligatoire (= pays émetteur)
    })


def gen_loan_repayments(p: _Pools, cc: str, n: int, start: datetime, end: datetime,
                        rng: np.random.Generator) -> pd.DataFrame:
    loans = p.loan_accounts.get(cc)
    if loans is None or n == 0:
        return pd.DataFrame()
    cur = C.CURRENCY_MAP[cc]
    sel = loans.iloc[rng.integers(0, len(loans), n)]
    ent = sel["entity_type"].to_numpy()
    loan_type = np.where(
        ent == "MICROFINANCE",
        rng.choice(C.LOAN_TYPES_MFI, size=n, p=C.LOAN_TYPES_MFI_PROBS),
        rng.choice(C.LOAN_TYPES_BANK, size=n, p=C.LOAN_TYPES_BANK_PROBS),
    )
    d = C.TARGET_DEFAULT_RATE[cc]
    status = rng.choice(["ON_TIME", "LATE", "DEFAULT"], size=n, p=[1 - d - C.LATE_RATE, C.LATE_RATE, d])

    event_ts = pd.to_datetime(_timestamps(start, end, n, rng))
    event_day = event_ts.dt.normalize()
    overdue = np.select([status == "LATE", status == "DEFAULT"],
                        [rng.integers(1, 90, n), rng.integers(90, 361, n)], default=0)
    early = rng.integers(0, 6, n)
    due_date = np.where(status == "ON_TIME", event_day + pd.to_timedelta(early, "D"),
                        event_day - pd.to_timedelta(overdue, "D"))

    # Échéance mensuelle ≈ plafond / durée (12 à 60 mois)
    amount_due = np.round(sel["credit_limit"].to_numpy() / rng.integers(12, 61, n), 2)
    amount_due = np.maximum(amount_due, 1_000 / (C.XOF_PER_GHS if cur == "GHS" else 1))
    partial = rng.random(n) < 0.3
    amount_paid = np.select(
        [status == "ON_TIME", status == "LATE", status == "DEFAULT"],
        [amount_due, np.where(partial, amount_due * rng.uniform(0.3, 0.9, n), amount_due),
         np.where(partial, amount_due * rng.uniform(0.0, 0.3, n), 0.0)],
    ).round(2)
    payment_date = pd.Series(event_day.dt.strftime("%Y-%m-%d"))
    payment_date[amount_paid == 0] = None

    return pd.DataFrame({
        "repayment_id": _uuids(n, rng),
        "timestamp": event_ts.dt.strftime(ISO_FMT),
        "loan_account_id": sel["account_id"].to_numpy(),
        "customer_id": sel["customer_id"].to_numpy(),
        "country_code": cc,
        "amount_due": amount_due,
        "amount_paid": amount_paid,
        "currency": cur,
        "due_date": pd.to_datetime(due_date).strftime("%Y-%m-%d"),
        "payment_date": payment_date.to_numpy(),
        "days_overdue": overdue,
        "loan_type": loan_type,
        "repayment_status": status,
        "entity_type": ent,
    })


GENERATORS = {
    "bank_transactions": gen_bank_transactions,
    "insurance_operations": gen_insurance_operations,
    "mobile_money_payments": gen_mobile_money,
    "loan_repayments": gen_loan_repayments,
}
ID_COLUMN = {
    "bank_transactions": "transaction_id",
    "insurance_operations": "operation_id",
    "mobile_money_payments": "payment_id",
    "loan_repayments": "repayment_id",
}


# --------------------------------------------------------------------------- #
# Injection d'anomalies (qualité de données)
# --------------------------------------------------------------------------- #
def inject_anomalies(df: pd.DataFrame, dataset: str, rate: float, rng: np.random.Generator) -> pd.DataFrame:
    """Corrompt ~rate des lignes (id vide, devise invalide, montant négatif ou non
    numérique, timestamp illisible) et duplique ~rate/2 lignes. Sert à prouver que
    la couche d'ingestion rejette/déduplique correctement."""
    if rate <= 0 or df.empty:
        return df
    df = df.astype(object)
    n = len(df)
    k = max(1, int(n * rate))
    idx = rng.choice(n, size=min(k, n), replace=False)
    amount_col = "amount_due" if dataset == "loan_repayments" else "amount"
    kinds = rng.integers(0, 5, len(idx))
    for i, kind in zip(idx, kinds, strict=True):
        if kind == 0:
            df.iat[i, df.columns.get_loc(ID_COLUMN[dataset])] = ""
        elif kind == 1:
            df.iat[i, df.columns.get_loc("currency")] = "EUR"
        elif kind == 2:
            df.iat[i, df.columns.get_loc(amount_col)] = -abs(float(df.iat[i, df.columns.get_loc(amount_col)]))
        elif kind == 3:
            df.iat[i, df.columns.get_loc(amount_col)] = "N/A"
        else:
            df.iat[i, df.columns.get_loc("timestamp")] = "31/02/2026 25:61"
    dups = df.iloc[rng.choice(n, size=max(1, k // 2), replace=False)]
    return pd.concat([df, dups], ignore_index=True)


# --------------------------------------------------------------------------- #
# Point d'entrée
# --------------------------------------------------------------------------- #
def generate_transactions(ref: Referentials, dataset: str, countries: list[str], n_rows: int,
                          start: datetime, end: datetime, anomaly_rate: float = 0.0,
                          seed: int | None = None, pools: _Pools | None = None) -> dict[str, pd.DataFrame]:
    """Retourne {country_code: DataFrame} pour le type demandé."""
    if dataset not in GENERATORS:
        raise ValueError(f"Type inconnu: {dataset}")
    if end <= start:
        raise ValueError("La date de fin doit être postérieure à la date de début")
    rng = np.random.default_rng(seed)
    pools = pools or _Pools(ref)
    eligible = countries
    if dataset == "mobile_money_payments":
        eligible = [c for c in countries if c in C.ENTITY_COUNTRIES["MOBILE_MONEY"]]
    out: dict[str, pd.DataFrame] = {}
    for cc, n in split_rows(n_rows, eligible, rng).items():
        df = GENERATORS[dataset](pools, cc, n, start, end, rng)
        if not df.empty:
            out[cc] = inject_anomalies(df, dataset, anomaly_rate, rng)
            log.info("%s %s: %s lignes", dataset, cc, len(out[cc]))
    return out


Pools = _Pools
