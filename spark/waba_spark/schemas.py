"""Contrats de données (schémas explicites + règles de validation) des tables raw.*

Un `DatasetSpec` décrit, pour chaque jeu de données :
  * le schéma CSV explicite (aucune inférence) ;
  * la clé d'idempotence ;
  * la colonne temporelle de partitionnement (None pour les référentiels) ;
  * les domaines de valeurs autorisés (enums) et colonnes obligatoires ;
  * les colonnes montant devant être >= 0.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
CURRENCY_BY_COUNTRY = {**{c: "XOF" for c in COUNTRIES if c != "GH"}, "GH": "GHS"}
ENTITY_TYPES = ["BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE"]
CORRUPT_COL = "_corrupt_record"


def _s(name, dtype=None, nullable=True):
    return StructField(name, dtype or StringType(), nullable)


@dataclass(frozen=True)
class DatasetSpec:
    name: str                      # nom logique = dossier raw-landing = table raw.<name>
    schema: StructType
    id_col: str
    ts_col: str | None = None      # partition days(ts_col) ; None -> partition country_code seule
    required: tuple[str, ...] = ()
    enums: dict[str, list[str]] = field(default_factory=dict)
    non_negative: tuple[str, ...] = ()
    check_currency: bool = False
    is_referential: bool = False
    pii_columns: tuple[str, ...] = ()  # colonnes masquées + pseudonymisées à l'ingestion

    @property
    def table(self) -> str:
        return f"raw.{self.name}"


SPECS: dict[str, DatasetSpec] = {}


def _register(spec: DatasetSpec) -> None:
    SPECS[spec.name] = spec


# --------------------------------------------------------------------------- #
# Référentiels
# --------------------------------------------------------------------------- #
_register(DatasetSpec(
    name="customers",
    schema=StructType([_s("customer_id"), _s("country_code"), _s("entity_type"), _s("segment"),
                       _s("kyc_level"), _s("onboarding_date", DateType()), _s("region"),
                       _s("is_active", BooleanType())]),
    id_col="customer_id", is_referential=True,
    required=("customer_id", "country_code", "entity_type"),
    enums={"segment": ["RETAIL", "SME", "CORPORATE", "PREMIUM"],
           "kyc_level": ["BASIC", "STANDARD", "ENHANCED"]},
))
_register(DatasetSpec(
    name="accounts",
    schema=StructType([_s("account_id"), _s("customer_id"), _s("country_code"), _s("account_type"),
                       _s("currency"), _s("balance", DoubleType()), _s("credit_limit", DoubleType()),
                       _s("opened_date", DateType()), _s("status"), _s("entity_type"), _s("iban")]),
    id_col="account_id", is_referential=True, check_currency=True,
    required=("account_id", "customer_id", "country_code", "entity_type", "account_type"),
    enums={"account_type": ["CURRENT", "SAVINGS", "LOAN", "MOBILE_WALLET", "INSURANCE_POLICY"],
           "status": ["ACTIVE", "FROZEN", "CLOSED", "DORMANT"]},
    non_negative=("credit_limit",),
    pii_columns=("iban",),
))
_register(DatasetSpec(
    name="branches",
    schema=StructType([_s("branch_id"), _s("country_code"), _s("entity_type"), _s("city"), _s("region"),
                       _s("branch_type"), _s("is_active", BooleanType())]),
    id_col="branch_id", is_referential=True,
    required=("branch_id", "country_code", "entity_type"),
    enums={"branch_type": ["FULL_SERVICE", "DIGITAL_ONLY", "AGENCY_BANKING", "ATM_POINT"]},
))
_register(DatasetSpec(
    name="products",
    schema=StructType([_s("product_id"), _s("country_code"), _s("product_code"), _s("product_name"),
                       _s("product_category"), _s("entity_type"), _s("currency"),
                       _s("commission_rate", DoubleType()), _s("interest_rate", DoubleType()),
                       _s("launch_date", DateType()), _s("is_active", BooleanType())]),
    id_col="product_id", is_referential=True, check_currency=True,
    required=("product_id", "country_code", "entity_type", "product_code"),
    non_negative=("commission_rate", "interest_rate"),
))

# --------------------------------------------------------------------------- #
# Transactions
# --------------------------------------------------------------------------- #
_register(DatasetSpec(
    name="bank_transactions",
    schema=StructType([_s("transaction_id"), _s("timestamp", TimestampType()), _s("account_id"),
                       _s("beneficiary_account"), _s("branch_id"), _s("country_code"),
                       _s("transaction_type"), _s("amount", DoubleType()), _s("currency"), _s("channel"),
                       _s("transaction_status"), _s("fee_amount", DoubleType()), _s("entity_type")]),
    id_col="transaction_id", ts_col="timestamp", check_currency=True,
    required=("transaction_id", "timestamp", "account_id", "branch_id", "country_code", "amount",
              "currency", "transaction_status", "entity_type"),
    enums={"transaction_type": ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT", "INTERNATIONAL_WIRE"],
           "channel": ["BRANCH", "ATM", "MOBILE_APP", "INTERNET_BANKING", "USSD"],
           "transaction_status": ["SUCCESS", "FAILED", "REVERSED"],
           "entity_type": ["BANK", "MICROFINANCE"]},
    non_negative=("amount", "fee_amount"),
))
_register(DatasetSpec(
    name="insurance_operations",
    schema=StructType([_s("operation_id"), _s("timestamp", TimestampType()), _s("customer_id"),
                       _s("account_id"), _s("country_code"), _s("operation_type"), _s("product_line"),
                       _s("amount", DoubleType()), _s("currency"), _s("claim_status"),
                       _s("processing_days", IntegerType()), _s("entity_type")]),
    id_col="operation_id", ts_col="timestamp", check_currency=True,
    required=("operation_id", "timestamp", "customer_id", "account_id", "country_code", "amount",
              "currency", "operation_type", "entity_type"),
    enums={"operation_type": ["PREMIUM_PAYMENT", "CLAIM_SUBMISSION", "CLAIM_PAYMENT",
                              "POLICY_RENEWAL", "POLICY_CANCELLATION"],
           "product_line": ["VIE", "IARD_AUTO", "IARD_HABITATION", "IARD_SANTE", "PREVOYANCE"],
           "claim_status": ["PENDING", "APPROVED", "REJECTED", "PAID"],
           "entity_type": ["INSURANCE"]},
    non_negative=("amount", "processing_days"),
))
_register(DatasetSpec(
    name="mobile_money_payments",
    schema=StructType([_s("payment_id"), _s("timestamp", TimestampType()), _s("sender_id"),
                       _s("receiver_id"), _s("sender_country"), _s("receiver_country"),
                       _s("amount", DoubleType()), _s("currency"), _s("payment_type"), _s("operator"),
                       _s("status"), _s("fee_amount", DoubleType()), _s("entity_type"), _s("country_code")]),
    id_col="payment_id", ts_col="timestamp", check_currency=True,
    required=("payment_id", "timestamp", "sender_id", "receiver_id", "country_code", "amount",
              "currency", "status", "entity_type"),
    enums={"payment_type": ["P2P", "MERCHANT_PAYMENT", "BILL_PAYMENT", "AIRTIME", "CROSS_BORDER_TRANSFER"],
           "operator": ["WABA_PAY", "ORANGE_MONEY_PARTNER", "MTN_PARTNER"],
           "status": ["SUCCESS", "FAILED", "PENDING"],
           "receiver_country": COUNTRIES,
           "entity_type": ["MOBILE_MONEY"]},
    non_negative=("amount", "fee_amount"),
))
_register(DatasetSpec(
    name="loan_repayments",
    schema=StructType([_s("repayment_id"), _s("timestamp", TimestampType()), _s("loan_account_id"),
                       _s("customer_id"), _s("country_code"), _s("amount_due", DoubleType()),
                       _s("amount_paid", DoubleType()), _s("currency"), _s("due_date", DateType()),
                       _s("payment_date", DateType()), _s("days_overdue", IntegerType()), _s("loan_type"),
                       _s("repayment_status"), _s("entity_type")]),
    id_col="repayment_id", ts_col="timestamp", check_currency=True,
    required=("repayment_id", "timestamp", "loan_account_id", "customer_id", "country_code",
              "amount_due", "amount_paid", "currency", "repayment_status", "entity_type"),
    enums={"loan_type": ["CONSUMER", "MORTGAGE", "SME", "AGRICULTURAL", "MICROCREDIT"],
           "repayment_status": ["ON_TIME", "LATE", "DEFAULT"],
           "entity_type": ["BANK", "MICROFINANCE"]},
    non_negative=("amount_due", "amount_paid", "days_overdue"),
))

REFERENTIALS = [n for n, s in SPECS.items() if s.is_referential]
TRANSACTIONS = [n for n, s in SPECS.items() if not s.is_referential]
