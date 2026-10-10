"""Catalogue de données « as code » (OpenMetadata) : découverte Trino, documentation Gold, PII, lineage.

Exécuté par le CronJob openmetadata-catalog (quotidien) ou à la demande (scripts/k8s/governance.sh catalog).
Idempotent : chaque exécution remet le catalogue dans l'état déclaré ici.

  1. connexion administrateur (mot de passe de .env ; au premier passage, remplace le mot de passe par défaut) ;
  2. découverte automatique des schémas bronze / silver / gold / reporting via le connecteur Trino ;
  3. référentiels : classification « Reglementaire » (BCEAO, CIMA, AML), équipe propriétaire « WABA Group »,
     glossaire « WABA Finance » ;
  4. documentation des 7 tables Gold (description métier, propriétaire, tags réglementaires, colonnes clés) ;
  5. tags PII.Sensitive sur les identifiants personnels (customer_id, comptes, IBAN, émetteurs/bénéficiaires) ;
  6. lineage raw (MinIO) -> bronze -> silver -> gold -> reporting, chaque arête portant le pipeline (DAG Airflow)
     qui la produit. Déclaré depuis le code des pipelines, pas saisi à la main ; évolution prévue : collecte
     automatique par l'agent OpenLineage de Spark.

  python catalog.py              (environnement : OM_URL, OM_ADMIN_EMAIL, OM_ADMIN_PASSWORD, TRINO_HOSTPORT)
  python catalog.py --dry-run    (construit et valide toutes les requêtes, sans serveur)
"""
from __future__ import annotations

import argparse
import base64
import os
import sys
from dataclasses import dataclass, field

SERVICE = "waba_trino"                 # service de base de données OpenMetadata (connecteur Trino)
CATALOG = "lakehouse"
STORAGE = "minio_raw_landing"          # zone d'atterrissage des CSV (MinIO)
PIPELINES_SERVICE = "waba_pipelines"
TEAM = "waba-group"
CLASSIFICATION = "Reglementaire"
GLOSSARY = "WABA_Finance"
SCHEMAS = ["bronze", "silver", "gold", "reporting"]
TRANSACTIONS = ["bank_transactions", "insurance_operations", "mobile_money_payments", "loan_repayments"]
REFERENTIALS = ["customers", "accounts", "branches", "products"]
AIRFLOW_URL = "http://airflow.waba.local/dags"


def fqn(schema: str, table: str) -> str:
    return f"{SERVICE}.{CATALOG}.{schema}.{table}"


@dataclass
class GoldTable:
    name: str
    description: str
    tags: list[str]
    sources: list[str]                                    # tables silver d'origine
    columns: dict[str, str] = field(default_factory=dict)


GOLD = [
    GoldTable("daily_transaction_volume",
              "Volume journalier des transactions par pays, entité, flux (banque, mobile money, assurance, "
              "remboursements) et type d'opération. Montants en EUR, transactions échouées comptées mais exclues "
              "des montants.", [],
              TRANSACTIONS,
              {"txn_count": "Nombre d'opérations (réussies et échouées)",
               "total_amount_eur": "Montant des opérations réussies (EUR)",
               "outlier_count": "Opérations au-delà de la borne de Tukey du pays (contrôle qualité)"}),
    GoldTable("npl_ratio_by_country",
              "Taux de créances en souffrance (NPL) BCEAO : encours en défaut (statut DEFAULT ou impayé > 90 jours) "
              "/ encours total, photographié à chaque fin de mois par pays et entité. Seuil d'alerte BCEAO : 5 %.",
              ["BCEAO"], ["loan_repayments"],
              {"npl_ratio": "Encours en souffrance / encours total (0-1)",
               "is_above_threshold": "Vrai si le ratio dépasse le seuil BCEAO (5 %)",
               "bceao_threshold": "Seuil réglementaire appliqué (traçabilité)"}),
    GoldTable("customer_arpu_monthly",
              "Revenu moyen par client (ARPC/ARPU) mensuel : commissions + intérêts perçus / clients actifs, "
              "par pays, entité et segment.", [],
              ["bank_transactions", "mobile_money_payments", "loan_repayments"],
              {"arpu_eur": "Revenu moyen par client actif (EUR)",
               "active_customers": "Clients distincts ayant au moins une opération dans le mois"}),
    GoldTable("loss_ratio_by_product",
              "Ratio sinistres / primes (loss ratio) CIMA par produit, pays et mois, avec cumul annuel. "
              "Seuil d'alerte CIMA : 70 %.", ["CIMA"], ["insurance_operations"],
              {"loss_ratio": "Sinistres payés / primes encaissées du mois",
               "loss_ratio_ytd": "Ratio cumulé depuis le début de l'année",
               "is_above_threshold": "Vrai si le ratio du mois dépasse 70 %"}),
    GoldTable("claims_processing_time",
              "Délai de traitement des sinistres clos (jours ouvrés et calendaires) par pays et branche "
              "(IARD / VIE) : moyenne, médiane, 90e centile.", ["CIMA"], ["insurance_operations"],
              {"avg_working_days": "Délai moyen en jours ouvrés (lundi-vendredi)",
               "p90_working_days": "90e centile du délai (jours ouvrés)"}),
    GoldTable("mobile_money_daily_flow",
              "Flux mobile money journaliers par pays et opérateur : volume, montant, taux d'échec, "
              "utilisateurs actifs.", ["AML"], ["mobile_money_payments"],
              {"failure_rate": "Transactions échouées / transactions",
               "active_users": "Émetteurs ou bénéficiaires distincts"}),
    GoldTable("cross_border_transfers",
              "Transferts transfrontaliers par corridor (pays émetteur -> bénéficiaire) et semaine, avec "
              "évolution par rapport à la semaine précédente ; corridors UEMOA identifiés.", ["AML"],
              ["mobile_money_payments"],
              {"corridor": "Pays émetteur - pays bénéficiaire",
               "wow_amount_change_pct": "Évolution du montant vs semaine précédente (%)"}),
]

REPORTING = {"bceao_prudential": ["npl_ratio_by_country"],
             "cima_technical": ["loss_ratio_by_product", "claims_processing_time"]}

# Données personnelles (RGPD / lois locales de protection des données) : identifiants directs ou indirects
PII = {
    "customers": ["customer_id"],
    "accounts": ["account_id", "customer_id", "iban_masked", "iban_hash"],
    "bank_transactions": ["account_id", "beneficiary_account", "customer_id"],
    "insurance_operations": ["customer_id", "account_id"],
    "mobile_money_payments": ["sender_id", "receiver_id"],
    "loan_repayments": ["loan_account_id", "customer_id"],
}

TAGS = {"BCEAO": "Soumis au reporting prudentiel BCEAO (UEMOA)",
        "CIMA": "Soumis au contrôle technique CIMA (assurance)",
        "AML": "Utile à la lutte anti-blanchiment (seuils déclaratifs)"}

GLOSSARY_TERMS = {
    "NPL": "Non-Performing Loan : créance impayée depuis plus de 90 jours ou en défaut.",
    "LossRatio": "Ratio sinistres payés / primes encaissées (CIMA).",
    "ARPC": "Revenu moyen par client actif sur la période.",
    "Corridor": "Couple pays émetteur -> pays bénéficiaire d'un transfert.",
    "AML": "Anti Money Laundering : détection des opérations au-delà des seuils déclaratifs.",
}

PIPELINES = {
    "dag_ingest_raw": "Ingestion des CSV de raw-landing (MinIO) vers la couche Bronze Iceberg",
    "dag_bronze_to_silver": "Nettoyage, dédoublonnage, conversion EUR et enrichissement Bronze -> Silver",
    "dag_silver_to_gold": "Calcul des 7 KPIs Gold",
    "dag_regulatory_report": "Rapports réglementaires quotidiens BCEAO / CIMA",
}


def lineage_edges() -> list[tuple[str, str, str, str, str]]:
    """(type source, fqn source, type cible, fqn cible, pipeline)."""
    edges = []
    for ds in TRANSACTIONS + REFERENTIALS:
        edges.append(("container", f"{STORAGE}.{ds}", "table", fqn("bronze", ds), "dag_ingest_raw"))
        edges.append(("table", fqn("bronze", ds), "table", fqn("silver", ds), "dag_bronze_to_silver"))
    for g in GOLD:
        edges += [("table", fqn("silver", s), "table", fqn("gold", g.name), "dag_silver_to_gold") for s in g.sources]
    for report, sources in REPORTING.items():
        edges += [("table", fqn("gold", s), "table", fqn("reporting", report), "dag_regulatory_report")
                  for s in sources]
    return edges


# --------------------------------------------------------------------------- #
# Requêtes (construites et validées par les modèles pydantic du SDK, y compris en --dry-run)
# --------------------------------------------------------------------------- #
def ingestion_config(token: str) -> dict:
    return {
        "source": {
            "type": "trino", "serviceName": SERVICE,
            "serviceConnection": {"config": {
                "type": "Trino", "hostPort": os.environ.get("TRINO_HOSTPORT", "trino.serving.svc.cluster.local:8080"),
                "username": "openmetadata", "catalog": CATALOG}},
            "sourceConfig": {"config": {
                "type": "DatabaseMetadata", "includeViews": False, "includeTags": False,
                "schemaFilterPattern": {"includes": [f"^{s}$" for s in SCHEMAS]}}},
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "INFO", "openMetadataServerConfig": {
            "hostPort": f"{os.environ.get('OM_URL', 'http://openmetadata:8585')}/api",
            "authProvider": "openmetadata", "securityConfig": {"jwtToken": token}}},
    }


def reference_requests() -> list:
    from metadata.generated.schema.api.classification.createClassification import CreateClassificationRequest
    from metadata.generated.schema.api.classification.createTag import CreateTagRequest
    from metadata.generated.schema.api.data.createContainer import CreateContainerRequest
    from metadata.generated.schema.api.data.createGlossary import CreateGlossaryRequest
    from metadata.generated.schema.api.data.createGlossaryTerm import CreateGlossaryTermRequest
    from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
    from metadata.generated.schema.api.services.createPipelineService import CreatePipelineServiceRequest
    from metadata.generated.schema.api.services.createStorageService import CreateStorageServiceRequest
    from metadata.generated.schema.api.teams.createTeam import CreateTeamRequest
    from metadata.generated.schema.entity.services.connections.pipeline.customPipelineConnection import (
        CustomPipelineConnection,
    )
    from metadata.generated.schema.entity.services.connections.storage.customStorageConnection import (
        CustomStorageConnection,
    )
    from metadata.generated.schema.entity.services.pipelineService import PipelineConnection, PipelineServiceType
    from metadata.generated.schema.entity.services.storageService import StorageConnection, StorageServiceType
    from metadata.generated.schema.entity.teams.team import TeamType

    reqs = [
        CreateTeamRequest(name=TEAM, displayName="WABA Group", teamType=TeamType.Group,
                          description="Entité propriétaire des données du lakehouse (Data Office groupe)."),
        CreateClassificationRequest(name=CLASSIFICATION, description="Rattachement réglementaire des données."),
        *[CreateTagRequest(classification=CLASSIFICATION, name=t, description=d) for t, d in TAGS.items()],
        CreateGlossaryRequest(name=GLOSSARY, displayName="WABA Finance",
                              description="Vocabulaire métier banque / assurance / mobile money du groupe."),
        *[CreateGlossaryTermRequest(glossary=GLOSSARY, name=t, description=d) for t, d in GLOSSARY_TERMS.items()],
        CreateStorageServiceRequest(name=STORAGE, serviceType=StorageServiceType.CustomStorage,
                                    description="MinIO, bucket raw-landing : CSV déposés par les entités.",
                                    connection=StorageConnection(config=CustomStorageConnection(type="CustomStorage"))),
        *[CreateContainerRequest(name=ds, service=STORAGE, prefix=f"raw-landing/{ds}/",
                                 description=f"Fichiers CSV bruts « {ds} » (zone d'atterrissage, avant Bronze).")
          for ds in TRANSACTIONS + REFERENTIALS],
        CreatePipelineServiceRequest(name=PIPELINES_SERVICE, serviceType=PipelineServiceType.CustomPipeline,
                                     description="DAGs Airflow de la plateforme WABA (Spark sur Kubernetes).",
                                     connection=PipelineConnection(config=CustomPipelineConnection(type="CustomPipeline"))),
        *[CreatePipelineRequest(name=p, service=PIPELINES_SERVICE, description=d, sourceUrl=f"{AIRFLOW_URL}/{p}")
          for p, d in PIPELINES.items()],
    ]
    return reqs


def tag_label(tag_fqn: str):
    from metadata.generated.schema.type.tagLabel import LabelType, State, TagLabel, TagSource

    return TagLabel(tagFQN=tag_fqn, source=TagSource.Classification, labelType=LabelType.Manual,
                    state=State.Confirmed)


# --------------------------------------------------------------------------- #
# Exécution contre le serveur
# --------------------------------------------------------------------------- #
def admin_token(base: str) -> str:
    import requests

    email = os.environ.get("OM_ADMIN_EMAIL", "admin@open-metadata.org")
    wanted = os.environ["OM_ADMIN_PASSWORD"]

    def login(pw: str):
        r = requests.post(f"{base}/api/v1/users/login", timeout=60,
                          json={"email": email, "password": base64.b64encode(pw.encode()).decode()})
        return r.json()["accessToken"] if r.ok else None

    token = login(wanted)
    if token:
        return token
    token = login("admin")                                 # premier passage : mot de passe par défaut
    if not token:
        raise SystemExit("connexion administrateur OpenMetadata impossible (mot de passe .env et défaut refusés)")
    r = requests.put(f"{base}/api/v1/users/changePassword", timeout=60,
                     headers={"Authorization": f"Bearer {token}"},
                     json={"username": email.split("@")[0], "oldPassword": "admin", "newPassword": wanted,
                           "confirmPassword": wanted, "requestType": "SELF"})
    print("mot de passe administrateur par défaut remplacé" if r.ok
          else f"⚠ mot de passe par défaut conservé ({r.status_code}) : à changer dans l'UI", flush=True)
    return login(wanted) or token


def run(dry_run: bool) -> int:
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.entity.data.container import Container
    from metadata.generated.schema.entity.data.pipeline import Pipeline
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.entity.teams.team import Team
    from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
    from metadata.generated.schema.type.entityLineage import Source as LineageSource
    from metadata.generated.schema.type.entityReference import EntityReference
    from metadata.generated.schema.type.entityReferenceList import EntityReferenceList
    from metadata.ingestion.models.table_metadata import ColumnDescription, ColumnTag

    refs = reference_requests()
    edges = lineage_edges()
    if dry_run:
        from metadata.workflow.metadata import MetadataWorkflow  # noqa: F401  (import du connecteur)

        tags = [tag_label(f"{CLASSIFICATION}.{t}") for t in TAGS] + [tag_label("PII.Sensitive")]
        print(f"dry-run OK : {len(refs)} objets de référence, {len(GOLD)} tables Gold documentées, "
              f"{sum(map(len, PII.values()))} colonnes PII x2 couches, {len(edges)} arêtes de lineage, "
              f"{len(tags)} tags, configuration d'ingestion {sorted(ingestion_config('x')['source'])}")
        return 0

    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
    from metadata.ingestion.ometa.ometa_api import OpenMetadata
    from metadata.workflow.metadata import MetadataWorkflow

    base = os.environ.get("OM_URL", "http://openmetadata:8585")
    token = admin_token(base)
    om = OpenMetadata(OpenMetadataConnection(hostPort=f"{base}/api", authProvider="openmetadata",
                                             securityConfig=OpenMetadataJWTClientConfig(jwtToken=token)))

    # 1. Découverte des tables via Trino
    wf = MetadataWorkflow.create(ingestion_config(token))
    wf.execute()
    wf.print_status()
    wf.raise_from_status()
    wf.stop()

    # 2. Référentiels
    for req in refs:
        om.create_or_update(req)
    team = om.get_by_name(Team, TEAM)
    owners = EntityReferenceList(root=[EntityReference(id=team.id, type="team")])
    missing = []

    def table(schema: str, name: str):
        t = om.get_by_name(Table, fqn(schema, name), fields=["columns", "tags", "owners"])
        if t is None:
            missing.append(f"{schema}.{name}")
        return t

    # 3. Tables Gold : description, propriétaire, tags, colonnes
    for g in GOLD:
        t = table("gold", g.name)
        if t is None:
            continue
        om.patch_description(Table, t, g.description, force=True)
        om.patch_owner(Table, t, owners, force=True)
        if g.tags:
            om.patch_tags(Table, t, [tag_label(f"{CLASSIFICATION}.{x}") for x in g.tags])
        cols = {c.name.root for c in t.columns}
        om.patch_column_descriptions(t, [ColumnDescription(column_fqn=f"{t.fullyQualifiedName.root}.{c}",
                                                           description=d)
                                         for c, d in g.columns.items() if c in cols], force=True)

    # 4. PII sur Bronze et Silver
    for schema in ("bronze", "silver"):
        for name, columns in PII.items():
            t = table(schema, name)
            if t is None:
                continue
            cols = {c.name.root for c in t.columns}
            om.patch_column_tags(t, [ColumnTag(column_fqn=f"{t.fullyQualifiedName.root}.{c}",
                                               tag_label=tag_label("PII.Sensitive")) for c in columns if c in cols])

    # 5. Lineage avec le pipeline producteur sur chaque arête
    entity_type = {"table": Table, "container": Container}
    added = 0
    for src_type, src, dst_type, dst, pipeline in edges:
        a = om.get_by_name(entity_type[src_type], src)
        b = om.get_by_name(entity_type[dst_type], dst)
        p = om.get_by_name(Pipeline, f"{PIPELINES_SERVICE}.{pipeline}")
        if not (a and b and p):
            missing.append(f"lineage {src} -> {dst}")
            continue
        om.add_lineage(AddLineageRequest(edge=EntitiesEdge(
            fromEntity=EntityReference(id=a.id, type=src_type), toEntity=EntityReference(id=b.id, type=dst_type),
            lineageDetails=LineageDetails(pipeline=EntityReference(id=p.id, type="pipeline"),
                                          source=LineageSource.PipelineLineage,
                                          description=PIPELINES[pipeline]))))
        added += 1
    print(f"catalogue appliqué : {len(GOLD)} tables Gold documentées, {added}/{len(edges)} arêtes de lineage",
          flush=True)
    if missing:
        print("⚠ absents du catalogue (tables non encore créées par les pipelines ?) :", sorted(set(missing)))
    return 0


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true")
    return run(p.parse_args(argv).dry_run)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
