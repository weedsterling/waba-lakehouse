"""Configuration métier du WABA Group : pays, entités, devises, distributions.

Toutes les constantes métier sont centralisées ici afin que le générateur,
les tests et (plus tard) les jobs Spark partagent le même référentiel.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

# --------------------------------------------------------------------------- #
# Périmètre géographique
# --------------------------------------------------------------------------- #
COUNTRIES: list[str] = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
UEMOA: list[str] = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ"]  # périmètre "zone XOF" du challenge
CURRENCY_MAP: dict[str, str] = {c: "XOF" for c in UEMOA} | {"GH": "GHS"}

# Poids relatifs des pays dans l'activité du groupe (somme = 1)
COUNTRY_WEIGHTS: dict[str, float] = {
    "CI": 0.26, "SN": 0.15, "ML": 0.10, "BF": 0.10,
    "GN": 0.08, "TG": 0.08, "BJ": 0.09, "GH": 0.14,
}

# Ordre de grandeur XOF -> GHS (montants générés de façon réaliste en devise locale)
XOF_PER_GHS: float = 45.0

# --------------------------------------------------------------------------- #
# Entités et périmètre d'activité (cf. tableau "Contexte & Mission")
# --------------------------------------------------------------------------- #
ENTITY_TYPES: list[str] = ["BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE"]
ENTITY_PROBS: list[float] = [0.55, 0.20, 0.18, 0.07]

ENTITY_COUNTRIES: dict[str, list[str]] = {
    "BANK": COUNTRIES,                           # Retail UEMOA + Corporate UEMOA+GH
    "INSURANCE": COUNTRIES,                      # Vie UEMOA + IARD UEMOA+GH
    "MOBILE_MONEY": ["CI", "SN", "BF", "GH"],
    "MICROFINANCE": ["ML", "GN", "BF"],
}

SEGMENTS = ["RETAIL", "SME", "CORPORATE", "PREMIUM"]
SEGMENT_PROBS = [0.65, 0.20, 0.10, 0.05]
KYC_LEVELS = ["BASIC", "STANDARD", "ENHANCED"]
KYC_PROBS = [0.3, 0.5, 0.2]

# --------------------------------------------------------------------------- #
# Géographie (villes / régions administratives)
# --------------------------------------------------------------------------- #
CITIES: dict[str, dict[str, str]] = {  # pays -> {ville: région}
    "CI": {"Abidjan": "District d'Abidjan", "Bouaké": "Gbêkê", "Yamoussoukro": "District de Yamoussoukro",
           "San Pedro": "San-Pédro", "Korhogo": "Poro"},
    "SN": {"Dakar": "Dakar", "Thiès": "Thiès", "Ziguinchor": "Ziguinchor",
           "Saint-Louis": "Saint-Louis", "Kaolack": "Kaolack"},
    "ML": {"Bamako": "District de Bamako", "Sikasso": "Sikasso", "Ségou": "Ségou",
           "Mopti": "Mopti", "Tombouctou": "Tombouctou"},
    "BF": {"Ouagadougou": "Centre", "Bobo-Dioulasso": "Hauts-Bassins",
           "Koudougou": "Centre-Ouest", "Banfora": "Cascades"},
    "GN": {"Conakry": "Conakry", "Nzérékoré": "Nzérékoré", "Kindia": "Kindia", "Kankan": "Kankan"},
    "TG": {"Lomé": "Maritime", "Sokodé": "Centrale", "Kara": "Kara", "Atakpamé": "Plateaux"},
    "BJ": {"Cotonou": "Littoral", "Porto-Novo": "Ouémé", "Parakou": "Borgou", "Abomey-Calavi": "Atlantique"},
    "GH": {"Accra": "Greater Accra", "Kumasi": "Ashanti", "Tamale": "Northern",
           "Cape Coast": "Central", "Sunyani": "Bono"},
}
CITY_PROBS_HEAD = 0.55  # la capitale économique concentre ~55 % de l'activité

# --------------------------------------------------------------------------- #
# Comptes
# --------------------------------------------------------------------------- #
ACCOUNT_TYPES_BY_ENTITY: dict[str, tuple[list[str], list[float]]] = {
    "BANK": (["CURRENT", "SAVINGS", "LOAN"], [0.55, 0.25, 0.20]),
    "MICROFINANCE": (["SAVINGS", "LOAN"], [0.40, 0.60]),
    "INSURANCE": (["INSURANCE_POLICY"], [1.0]),
    "MOBILE_MONEY": (["MOBILE_WALLET"], [1.0]),
}
ACCOUNT_STATUS = ["ACTIVE", "DORMANT", "FROZEN", "CLOSED"]
ACCOUNT_STATUS_PROBS = [0.86, 0.07, 0.03, 0.04]

# --------------------------------------------------------------------------- #
# Transactions bancaires
# --------------------------------------------------------------------------- #
TXN_TYPES = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "INTERNATIONAL_WIRE"]
TXN_TYPE_PROBS = [0.35, 0.30, 0.15, 0.15, 0.05]
CHANNELS = ["BRANCH", "ATM", "MOBILE_APP", "INTERNET_BANKING", "USSD"]
CHANNEL_PROBS = [0.20, 0.15, 0.35, 0.20, 0.10]
TXN_STATUSES = ["SUCCESS", "FAILED", "REVERSED"]
TXN_STATUS_PROBS = [0.92, 0.05, 0.03]

# --------------------------------------------------------------------------- #
# Assurance
# --------------------------------------------------------------------------- #
INSURANCE_OP_TYPES = ["PREMIUM_PAYMENT", "CLAIM_SUBMISSION", "CLAIM_PAYMENT",
                      "POLICY_RENEWAL", "POLICY_CANCELLATION"]
INSURANCE_OP_PROBS = [0.58, 0.14, 0.11, 0.12, 0.05]
PRODUCT_LINES_UEMOA = ["VIE", "IARD_AUTO", "IARD_HABITATION", "IARD_SANTE", "PREVOYANCE"]
PRODUCT_LINES_GH = ["IARD_AUTO", "IARD_HABITATION", "IARD_SANTE"]  # pas d'Assurance Vie au Ghana
CLAIM_STATUSES_SUBMISSION = ["PENDING", "APPROVED", "REJECTED"]
CLAIM_STATUSES_SUBMISSION_PROBS = [0.45, 0.40, 0.15]

# Loss ratio cible par pays (note réglementaire : 50 % – 85 %)
TARGET_LOSS_RATIO: dict[str, float] = {
    "CI": 0.62, "SN": 0.58, "ML": 0.74, "BF": 0.78,
    "GN": 0.81, "TG": 0.66, "BJ": 0.55, "GH": 0.70,
}

# --------------------------------------------------------------------------- #
# Mobile money
# --------------------------------------------------------------------------- #
MM_PAYMENT_TYPES = ["P2P", "MERCHANT_PAYMENT", "BILL_PAYMENT", "AIRTIME", "CROSS_BORDER_TRANSFER"]
MM_PAYMENT_PROBS = [0.38, 0.25, 0.15, 0.14, 0.08]
MM_OPERATORS = ["WABA_PAY", "ORANGE_MONEY_PARTNER", "MTN_PARTNER"]
MM_OPERATOR_PROBS = [0.50, 0.30, 0.20]
MM_STATUSES = ["SUCCESS", "FAILED", "PENDING"]
MM_STATUS_PROBS = [0.94, 0.04, 0.02]
# Corridors transfrontaliers privilégiés (émetteur -> destinataires)
CROSS_BORDER_CORRIDORS: dict[str, list[str]] = {
    "CI": ["SN", "ML", "BF", "GN", "TG", "BJ"],
    "SN": ["CI", "ML", "GN"],
    "BF": ["CI", "ML", "TG"],
    "GH": ["CI", "TG", "BJ"],
}

# --------------------------------------------------------------------------- #
# Crédits
# --------------------------------------------------------------------------- #
LOAN_TYPES_BANK = ["CONSUMER", "MORTGAGE", "SME", "AGRICULTURAL"]
LOAN_TYPES_BANK_PROBS = [0.45, 0.15, 0.25, 0.15]
LOAN_TYPES_MFI = ["MICROCREDIT", "AGRICULTURAL", "SME"]
LOAN_TYPES_MFI_PROBS = [0.65, 0.25, 0.10]
# Taux de défaut cible par pays (NPL réaliste 3 % – 8 %)
TARGET_DEFAULT_RATE: dict[str, float] = {
    "CI": 0.040, "SN": 0.045, "ML": 0.072, "BF": 0.068,
    "GN": 0.078, "TG": 0.055, "BJ": 0.042, "GH": 0.060,
}
LATE_RATE = 0.12

# --------------------------------------------------------------------------- #
# Volumétries par défaut (cf. énoncé)
# --------------------------------------------------------------------------- #
DEFAULT_ROWS = {
    "customers": 500_000,
    "accounts": 800_000,
    "branches": 200,
    "products": 50,
    "bank_transactions": 10_000,
    "insurance_operations": 5_000,
    "mobile_money_payments": 20_000,
    "loan_repayments": 5_000,
}

# Préfixes de fichiers (nomenclature imposée)
FILE_PREFIX = {
    "bank_transactions": "bank_txn",
    "insurance_operations": "insurance_ops",
    "mobile_money_payments": "mobile_money",
    "loan_repayments": "loan_repayments",
}
TRANSACTIONAL_DATASETS = list(FILE_PREFIX)
REFERENTIAL_DATASETS = ["customers", "accounts", "branches", "products"]


@dataclass(frozen=True)
class StorageSettings:
    """Paramètres d'accès MinIO, lus exclusivement depuis l'environnement."""

    endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    raw_bucket: str = os.getenv("RAW_BUCKET", "raw-landing")
    cache_dir: str = os.getenv("GENERATOR_CACHE_DIR", "/data/referentials")
