"""Génération vectorisée (numpy/pandas) des référentiels du WABA Group.

Ordre imposé : branches -> customers -> accounts -> products.
Les transactions ne sont générées qu'à partir de ces référentiels, ce qui
garantit l'absence de clé orpheline (account_id, customer_id, branch_id).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd

from . import config as C

log = logging.getLogger(__name__)


@dataclass
class Referentials:
    customers: pd.DataFrame
    accounts: pd.DataFrame
    branches: pd.DataFrame
    products: pd.DataFrame

    def as_dict(self) -> dict[str, pd.DataFrame]:
        return {"customers": self.customers, "accounts": self.accounts,
                "branches": self.branches, "products": self.products}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _split_by_weight(n: int, countries: list[str], rng: np.random.Generator) -> dict[str, int]:
    """Répartit n lignes entre pays selon COUNTRY_WEIGHTS (somme exacte = n)."""
    w = np.array([C.COUNTRY_WEIGHTS[c] for c in countries], dtype=float)
    w /= w.sum()
    counts = rng.multinomial(n, w)
    return dict(zip(countries, counts.tolist(), strict=True))


def _pick_cities(cc: str, n: int, rng: np.random.Generator) -> np.ndarray:
    cities = list(C.CITIES[cc])
    rest = (1 - C.CITY_PROBS_HEAD) / (len(cities) - 1)
    p = [C.CITY_PROBS_HEAD] + [rest] * (len(cities) - 1)
    return rng.choice(cities, size=n, p=p)


def _random_dates(start: date, end: date, n: int, rng: np.random.Generator) -> np.ndarray:
    s, e = np.datetime64(start, "D"), np.datetime64(end, "D")
    offsets = rng.integers(0, (e - s).astype(int) + 1, size=n)
    return (s + offsets).astype("datetime64[D]")


def _entity_probs_for(cc: str) -> tuple[list[str], np.ndarray]:
    """Probabilités d'entité renormalisées sur les entités présentes dans le pays."""
    ents = [e for e in C.ENTITY_TYPES if cc in C.ENTITY_COUNTRIES[e]]
    p = np.array([C.ENTITY_PROBS[C.ENTITY_TYPES.index(e)] for e in ents])
    return ents, p / p.sum()


def _fake_iban(cc: str, account_seq: np.ndarray) -> np.ndarray:
    """IBAN fictif (format BCEAO simplifié). Donnée sensible : masquée dès l'ingestion."""
    bank_code = {"GH": "GH0WABA"}.get(cc, f"{cc}0WA")
    return np.char.add(f"{cc}76 {bank_code} ", np.char.zfill(account_seq.astype(str), 12))


# --------------------------------------------------------------------------- #
# Branches
# --------------------------------------------------------------------------- #
def generate_branches(n: int, rng: np.random.Generator) -> pd.DataFrame:
    frames = []
    for cc, k in _split_by_weight(n, C.COUNTRIES, rng).items():
        k = max(k, 3)  # au moins 3 agences par pays
        cities = _pick_cities(cc, k, rng)
        ents, p = _entity_probs_for(cc)
        frames.append(pd.DataFrame({
            "branch_id": [f"WABA-{cc}-B-{i:03d}" for i in range(1, k + 1)],
            "country_code": cc,
            "entity_type": rng.choice(ents, size=k, p=p),
            "city": cities,
            "region": [C.CITIES[cc][c] for c in cities],
            "branch_type": rng.choice(["FULL_SERVICE", "DIGITAL_ONLY", "AGENCY_BANKING", "ATM_POINT"],
                                      size=k, p=[0.45, 0.10, 0.30, 0.15]),
            "is_active": rng.random(k) > 0.04,
        }))
    df = pd.concat(frames, ignore_index=True)
    log.info("branches générées: %s lignes", len(df))
    return df


# --------------------------------------------------------------------------- #
# Customers
# --------------------------------------------------------------------------- #
def generate_customers(n: int, rng: np.random.Generator) -> pd.DataFrame:
    frames = []
    for cc, k in _split_by_weight(n, C.COUNTRIES, rng).items():
        ents, p = _entity_probs_for(cc)
        frames.append(pd.DataFrame({
            "customer_id": [f"WABA-{cc}-C-{i:06d}" for i in range(1, k + 1)],
            "country_code": cc,
            "entity_type": rng.choice(ents, size=k, p=p),
            "segment": rng.choice(C.SEGMENTS, size=k, p=C.SEGMENT_PROBS),
            "kyc_level": rng.choice(C.KYC_LEVELS, size=k, p=C.KYC_PROBS),
            "onboarding_date": _random_dates(date(2010, 1, 1), date(2025, 12, 31), k, rng),
            "region": _pick_cities(cc, k, rng),
            "is_active": rng.random(k) > 0.08,
        }))
    df = pd.concat(frames, ignore_index=True)
    log.info("customers générés: %s lignes", len(df))
    return df


# --------------------------------------------------------------------------- #
# Accounts
# --------------------------------------------------------------------------- #
def generate_accounts(customers: pd.DataFrame, n: int, rng: np.random.Generator) -> pd.DataFrame:
    """Chaque client a au moins un compte ; les comptes supplémentaires sont
    attribués aux clients BANK / MICROFINANCE / INSURANCE (multi-détention)."""
    n_cust = len(customers)
    if n < n_cust:
        raise ValueError(f"accounts ({n}) doit être >= customers ({n_cust}) : 1 compte minimum par client")

    extra_pool = customers.index[customers["entity_type"] != "MOBILE_MONEY"].to_numpy()
    owner_idx = np.concatenate([customers.index.to_numpy(), rng.choice(extra_pool, size=n - n_cust)])
    owners = customers.loc[owner_idx, ["customer_id", "country_code", "entity_type", "segment",
                                       "onboarding_date"]].reset_index(drop=True)

    # Type de compte selon l'entité du client
    account_type = np.empty(n, dtype=object)
    for ent, (types, probs) in C.ACCOUNT_TYPES_BY_ENTITY.items():
        mask = (owners["entity_type"] == ent).to_numpy()
        account_type[mask] = rng.choice(types, size=mask.sum(), p=probs)

    currency = owners["country_code"].map(C.CURRENCY_MAP).to_numpy()
    fx = np.where(currency == "GHS", 1 / C.XOF_PER_GHS, 1.0)
    seg_mult = owners["segment"].map({"RETAIL": 1, "SME": 6, "CORPORATE": 40, "PREMIUM": 15}).to_numpy()

    balance = np.round(rng.lognormal(mean=12.2, sigma=1.3, size=n) * seg_mult * fx, 2)
    is_loan = account_type == "LOAN"
    # LOAN : plafond = montant accordé (> encours) ; CURRENT : découvert autorisé pour ~30 %
    overdraft = np.where(rng.random(n) < 0.3, balance * 0.5, 0.0)
    credit_limit = np.round(np.select(
        [is_loan, account_type == "CURRENT"],
        [balance * rng.uniform(1.1, 2.5, n), overdraft],
        default=0.0,
    ), 2)

    # Numérotation séquentielle par pays : WABA-CI-A-0000001
    seq = owners.groupby("country_code").cumcount().to_numpy() + 1
    account_id = ("WABA-" + owners["country_code"] + "-A-" + pd.Series(seq).astype(str).str.zfill(7)).to_numpy()

    iban = np.empty(n, dtype=object)
    for cc in C.COUNTRIES:
        m = (owners["country_code"] == cc).to_numpy()
        iban[m] = _fake_iban(cc, seq[m])

    onboarding = owners["onboarding_date"].to_numpy().astype("datetime64[D]")
    opened = onboarding + rng.integers(0, 365 * 3, n).astype("timedelta64[D]")
    opened = np.minimum(opened, np.datetime64("2025-12-31"))

    df = pd.DataFrame({
        "account_id": account_id,
        "customer_id": owners["customer_id"].to_numpy(),
        "country_code": owners["country_code"].to_numpy(),
        "account_type": account_type,
        "currency": currency,
        "balance": balance,
        "credit_limit": credit_limit,
        "opened_date": opened,
        "status": rng.choice(C.ACCOUNT_STATUS, size=n, p=C.ACCOUNT_STATUS_PROBS),
        "entity_type": owners["entity_type"].to_numpy(),
        "iban": iban,  # colonne additionnelle (PII) -> masquée/pseudonymisée à l'ingestion
    })
    log.info("accounts générés: %s lignes", len(df))
    return df


# --------------------------------------------------------------------------- #
# Products (catalogue 50 lignes, schéma non imposé par l'énoncé)
# --------------------------------------------------------------------------- #
_PRODUCT_TEMPLATES = [
    # (product_code, libellé, catégorie, entité, pays éligibles)
    ("CURRENT", "Compte Courant", "ACCOUNT", "BANK", C.COUNTRIES),
    ("SAVINGS", "Compte Épargne", "ACCOUNT", "BANK", C.COUNTRIES),
    ("CONSUMER", "Crédit Consommation", "LOAN", "BANK", C.COUNTRIES),
    ("MORTGAGE", "Crédit Immobilier", "LOAN", "BANK", C.COUNTRIES),
    ("SME", "Crédit PME", "LOAN", "BANK", C.COUNTRIES),
    ("AGRICULTURAL", "Crédit Agricole", "LOAN", "MICROFINANCE", ["ML", "GN", "BF"]),
    ("MICROCREDIT", "Microcrédit / Tontine digitale", "LOAN", "MICROFINANCE", ["ML", "GN", "BF"]),
    ("VIE", "Assurance Vie Épargne", "INSURANCE", "INSURANCE", C.UEMOA),
    ("PREVOYANCE", "Prévoyance Famille", "INSURANCE", "INSURANCE", C.UEMOA),
    ("IARD_AUTO", "Assurance Auto", "INSURANCE", "INSURANCE", C.COUNTRIES),
    ("IARD_HABITATION", "Assurance Habitation", "INSURANCE", "INSURANCE", C.COUNTRIES),
    ("IARD_SANTE", "Assurance Santé", "INSURANCE", "INSURANCE", C.COUNTRIES),
    ("MOBILE_WALLET", "Portefeuille WABA Pay", "WALLET", "MOBILE_MONEY", ["CI", "SN", "BF", "GH"]),
]


def generate_products(n: int, rng: np.random.Generator) -> pd.DataFrame:
    """Catalogue produits par pays. Les couples (pays, product_code) sont triés par
    priorité commerciale : les n premiers sont retenus (n=50 par défaut)."""
    rows = []
    for code, label, cat, ent, countries in _PRODUCT_TEMPLATES:
        for cc in countries:
            rows.append((cc, code, label, cat, ent))
    rng.shuffle(rows)
    # Priorité : grands pays et produits cœur d'abord -> couverture maximale
    rows.sort(key=lambda r: (-C.COUNTRY_WEIGHTS[r[0]] * (2 if r[3] in ("ACCOUNT", "INSURANCE") else 1)))
    rows = rows[:n]
    df = pd.DataFrame(rows, columns=["country_code", "product_code", "product_name",
                                     "product_category", "entity_type"])
    df.insert(0, "product_id", [f"WABA-P-{i:03d}" for i in range(1, len(df) + 1)])
    df["product_name"] = df["product_name"] + " " + df["country_code"]
    df["currency"] = df["country_code"].map(C.CURRENCY_MAP)
    df["commission_rate"] = np.round(rng.uniform(0.002, 0.02, len(df)), 4)
    df["interest_rate"] = np.where(df["product_category"] == "LOAN",
                                   np.round(rng.uniform(0.07, 0.24, len(df)), 4), 0.0)
    df["launch_date"] = _random_dates(date(2012, 1, 1), date(2024, 12, 31), len(df), rng)
    df["is_active"] = True
    log.info("products générés: %s lignes", len(df))
    return df


def generate_referentials(sizes: dict[str, int] | None = None, seed: int = 42) -> Referentials:
    sizes = {**{k: C.DEFAULT_ROWS[k] for k in C.REFERENTIAL_DATASETS}, **(sizes or {})}
    rng = np.random.default_rng(seed)
    branches = generate_branches(sizes["branches"], rng)
    customers = generate_customers(sizes["customers"], rng)
    accounts = generate_accounts(customers, sizes["accounts"], rng)
    products = generate_products(sizes["products"], rng)
    return Referentials(customers, accounts, branches, products)
