# WABA Group — Plateforme Lakehouse financière multi-pays

Challenge Data Engineer Artefact · Banque & Assurance · Afrique de l'Ouest

Plateforme Lakehouse (pattern Lambda) qui ingère, historise et expose les données
de WestAfrica BancAssur Group (8 pays, 4 lignes métier) :
**MinIO (S3) + Apache Iceberg + Spark + Trino**, orchestrée par Airflow (Level 2),
étendue au streaming NiFi/Kafka (Level 3) puis déployée sur Kubernetes (Level 4).

> **État actuel : Level 1 livré.** Le guide d'installation et la feuille de route des niveaux 2 à 4 sont dans
> [`docs/ROADMAP.md`](docs/ROADMAP.md).

---

## 1. Architecture (Level 1)

```
┌──────────────┐  CSV   ┌──────────────────────── MinIO ─────────────────────────┐
│  Streamlit   │──────▶ │ raw-landing/<type>/<pays>/bank_txn_CI_20260923_01.csv  │
│  (générateur)│        │ lakehouse/  (tables Iceberg : raw.*, audit.*)          │
└──────────────┘        │ archive/    (fichiers traités, versionnés)             │
                        └───────▲──────────────────────────┬─────────────────────┘
                     S3A (read) │                          │ S3FileIO (Parquet)
                        ┌───────┴────────┐   REST   ┌──────▼────────┐
                        │ Spark 3.5      │◀────────▶│ Iceberg REST  │
                        │ ingest_raw.py  │          │ catalog       │
                        └────────────────┘          └──────▲────────┘
                                                           │ REST
                                                    ┌──────┴────────┐
                                                    │ Trino (SQL)   │
                                                    └───────────────┘
```

| Service | Rôle | URL locale |
|---|---|---|
| `generator` | Application Streamlit de génération des données | http://localhost:8501 |
| `minio` | Object storage S3 (console) | http://localhost:9001 |
| `minio-init` | Création idempotente des buckets + compte de service | — (one-shot) |
| `iceberg-rest` | Catalogue Iceberg REST (métadonnées persistées) | http://localhost:8181/v1/config |
| `spark-master` / `spark-worker` | Cluster Spark standalone (jobs PySpark) | http://localhost:8080 |
| `trino` | Moteur SQL de consommation | http://localhost:8088 |

## 2. Prérequis

| Outil | Version testée | Remarque |
|---|---|---|
| Docker Desktop (Windows : backend **WSL2**) | 4.3x+ / Engine 27+ | Allouer **≥ 12 Go RAM** et 6 CPU à WSL2 (cf. `docs/ROADMAP.md`) |
| Docker Compose | v2.24+ | Inclus dans Docker Desktop |
| Git | 2.40+ | |
| Python (optionnel, tests locaux) | 3.11 / 3.12 | + Java 17 pour les tests Spark |

Ports utilisés : 8501, 9000, 9001, 8181, 8080, 8081, 7077, 4040, 8088.

> **Installation dans une machine virtuelle** (Hyper-V / VMware) : voir `docs/ROADMAP.md` Partie A-bis et le script
> `scripts/vm-setup.sh`, qui prépare une VM Ubuntu Server 24.04 en une commande.

## 3. Démarrage from scratch

```powershell
git clone <url-du-depot> waba-lakehouse
cd waba-lakehouse
Copy-Item .env.example .env        # bash : cp .env.example .env
# éditer .env : remplacer toutes les valeurs "change-me"

./scripts/download-jars.sh         # JARs Spark (~330 Mo), reprise auto + vérification SHA-1
docker compose up -d --build       # 1er build : ~5-15 min selon la connexion
docker compose ps                  # minio-init doit être "Exited (0)", les autres "running"
```

### Étape 1 — Générer les données (Streamlit)

Ouvrir http://localhost:8501 :

1. **Onglet Référentiels** → *Générer et envoyer vers MinIO* (500 k clients, 800 k comptes, 200 agences, 50 produits ; ~30 s).
2. **Onglet One-time** → choisir types, pays, lignes métier, période (défaut : dernier trimestre) → *Générer*.
3. **Onglet Flux continu** (optionnel) → micro-lots toutes les 10 à 60 s.

Vérifier dans la console MinIO (http://localhost:9001) : `raw-landing/bank_transactions/CI/bank_txn_CI_YYYYMMDD_01.csv`, etc.

### Étape 2 — Ingestion Spark → Iceberg

```powershell
.\scripts\ingest.ps1                                        # bash : ./scripts/ingest.sh
.\scripts\ingest.ps1 --datasets bank_transactions --countries CI,SN
```

Suivi : http://localhost:8080 (Spark UI). Les logs sont en JSON structuré.

### Étape 3 — Interroger avec Trino

```powershell
docker compose exec -it trino trino --catalog lakehouse     # shell SQL interactif
Get-Content sql\level1_checks.sql | docker compose exec -T trino trino --catalog lakehouse
```

```sql
SHOW TABLES FROM lakehouse.raw;
SELECT country_code, count(*), round(sum(balance)) FROM lakehouse.raw.accounts GROUP BY 1;
```

### Étape 4 — Prouver l'idempotence

```powershell
.\scripts\ingest.ps1 --source archive --no-archive          # rejoue TOUS les fichiers déjà ingérés
```
```sql
SELECT transaction_id, count(*) FROM lakehouse.raw.bank_transactions
GROUP BY 1 HAVING count(*) > 1;                               -- 0 ligne
```
Le log du job affiche `"inserted": 0` pour chaque dataset rejoué.

## 4. Choix techniques clés

| Sujet | Choix | Justification |
|---|---|---|
| Catalogue | Iceberg **REST** (tabulario, SQLite persistée) | Standard ouvert, partagé par Spark et Trino, sans Hive Metastore. En production : Polaris / Lakekeeper / Nessie sur PostgreSQL. |
| Partitionnement | `country_code` + `days(timestamp)` (partition cachée Iceberg) | Élagage par pays/date sans colonne technique à maintenir ; référentiels partitionnés par pays. |
| Idempotence | 3 niveaux : dédoublonnage intra-lot, `MERGE INTO … ON id AND country_code`, fichiers archivés | Rejouer un fichier n'insère rien ; `country_code` dans la condition permet l'élagage de partitions. |
| Validation | Schéma explicite, mode `PERMISSIVE` + `_corrupt_record`, règles déclaratives (`schemas.py`) | Aucune ligne perdue silencieusement : chaque rejet est tracé avec **tous** ses motifs dans `audit.rejected_records`. |
| PII | IBAN → `iban_masked` + `iban_hash` (SHA-256 salé, sel dans `.env`) | Jamais de donnée sensible en clair dans le lakehouse ; le hash reste joignable. |
| Sécurité | Compte de service MinIO dédié, secrets uniquement via `.env` | Le compte root MinIO n'est utilisé que par `minio-init`. |
| Performance | Génération vectorisée numpy (800 k comptes en ~5 s), aucune UDF Python dans Spark | Transformations 100 % JVM. |
| Traçabilité | `audit.ingestion_log` (fichier, ETag, lignes lues/valides/rejetées, statut) | Base de l'observabilité (Level 4). |

## 5. Structure du dépôt

```
├── docker-compose.yml          # stack complète Level 1
├── .env.example                # variables d'environnement (valeurs d'exemple)
├── generator/                  # Application Streamlit
│   ├── app.py
│   └── waba_gen/               # config métier, référentiels, transactions, stockage, flux continu
├── spark/
│   ├── Dockerfile              # Spark 3.5.3 + Iceberg 1.6.1 + S3A
│   ├── conf/spark-defaults.conf
│   ├── waba_spark/             # schémas (contrats), validation, Iceberg, utilitaires
│   └── jobs/ingest_raw.py      # job raw-landing -> raw.*
├── trino/catalog/lakehouse.properties
├── sql/level1_checks.sql       # requêtes de contrôle et d'analyse
├── scripts/                    # minio-init.sh, ingest.ps1, ingest.sh
├── tests/                      # pytest : générateur, validation, Silver, Gold, DAGs
└── docs/                       # ROADMAP (installation + niveaux 2-4), ARCHITECTURE (write-up)
```

## 6. Tests

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
pytest -q            # pytest.ini fixe le PYTHONPATH (generator, spark)
```
Les tests Spark nécessitent Java 17 (`JAVA_HOME`) ; ils sont ignorés si PySpark est absent.

## 7. Modèle de données (tables Iceberg)

| Table | Clé | Partitionnement | Colonnes ajoutées |
|---|---|---|---|
| `raw.customers`, `raw.branches`, `raw.products` | `*_id` | `country_code` | `_source_file`, `_ingestion_ts`, `_batch_id` |
| `raw.accounts` | `account_id` | `country_code` | + `iban_masked`, `iban_hash` (IBAN en clair supprimé) |
| `raw.bank_transactions` | `transaction_id` | `country_code`, `days(timestamp)` | colonnes techniques |
| `raw.insurance_operations` | `operation_id` | idem | idem |
| `raw.mobile_money_payments` | `payment_id` | idem | idem |
| `raw.loan_repayments` | `repayment_id` | idem | idem |
| `audit.ingestion_log`, `audit.rejected_records` | — | `dataset`, jour | tables techniques |

## 8. Hypothèses et limites connues

* **Guinée (GN) en XOF** : l'énoncé classe GN en zone UEMOA/XOF ; en réalité la Guinée utilise le franc guinéen (GNF) et n'est pas membre de l'UEMOA. L'hypothèse de l'énoncé est respectée et centralisée dans `config.py` (`CURRENCY_MAP`).
* **Catalogue produits** : schéma non fourni par l'énoncé ; modélisé par pays (`product_code` = type de compte / de prêt / ligne d'assurance) pour permettre les jointures Silver au Level 2.
* **Mobile money** : `country_code` (= pays émetteur) ajouté pour respecter la contrainte « toutes les tables ».
* **IBAN** : colonne ajoutée au référentiel comptes pour démontrer le masquage exigé par les contraintes.
* **Séquence `NN`** : sur 2 chiffres minimum, elle peut dépasser 99 en mode continu (plusieurs milliers de micro-lots/jour).
* **MinIO** : MinIO a cessé de publier ses images communautaires (Docker Hub, puis quay.io depuis 2026). Le serveur provient d'un build communautaire open source épinglé (`ghcr.io/coollabsio/minio`), le client `mc` de l'image figée `bitnamilegacy/minio-client`. Alternatives S3 compatibles si ces miroirs disparaissent : SeaweedFS, Garage.
* Le catalogue REST (SQLite) est mono-instance : adapté au développement, pas à la haute disponibilité.

## 9. Dépannage

| Symptôme | Cause / solution |
|---|---|
| `/bin/sh^M: bad interpreter` dans `minio-init` | Fins de ligne CRLF : `git config core.autocrlf input` puis re-cloner (le `.gitattributes` force LF). |
| Spark `OutOfMemoryError` sur les référentiels | Augmenter `SPARK_WORKER_MEMORY` et la RAM allouée à WSL2 (`.wslconfig`). |
| Trino : `Table not found` | Lancer l'ingestion d'abord ; vérifier `curl http://localhost:8181/v1/namespaces`. |
| Port déjà utilisé | Modifier le port publié côté hôte dans `docker-compose.yml`. |
| Réinitialisation complète | `docker compose down -v` (⚠️ supprime les volumes : données et catalogue). |

---

## Level 2 — Orchestration Airflow & architecture médaillon (en cours)

| Service | Rôle | URL |
|---|---|---|
| `airflow-apiserver` | UI + API Airflow 3 (utilisateur `admin`, mot de passe `AIRFLOW_ADMIN_PASSWORD`) | http://localhost:8090 |
| `airflow-scheduler` | Planifie et exécute les tâches (LocalExecutor) ; héberge le driver Spark en mode client | — |
| `airflow-dag-processor` | Analyse les fichiers de DAGs | — |
| `airflow-postgres` | Base de métadonnées Airflow | — |

```bash
./scripts/gen-secrets.sh          # complète .env (secrets Airflow) sans toucher aux secrets existants
docker compose up -d --build
```

| DAG | Déclenchement | Rôle |
|---|---|---|
| `dag_ingest_raw` | toutes les 15 min + capteur de nouveaux fichiers MinIO | raw-landing → `bronze.*` ; publie l'asset `bronze` |
| `dag_bronze_to_silver` | asset `bronze` (data-aware) | `silver.*` : dédoublonnage, conversion EUR (`silver.fx_rates`), jointures référentiels, indicateurs `is_orphan_*` / `is_outlier`, métriques `audit.dq_metrics` |
| `dag_silver_to_gold` | asset `silver` (data-aware) | 7 KPIs `gold.*` (voir ci-dessous) ; publie l'asset `gold` |
| `dag_regulatory_report` | tous les jours à 00h30 UTC | `reporting.bceao_prudential` (NPL, seuil 5 %) et `reporting.cima_technical` (loss ratio cumulé, seuil 70 %), exports CSV par régulateur dans `s3://lakehouse/exports/regulatory/`, alerte structurée par dépassement |

Tous les jobs Spark passent par le pool Airflow **`spark` (1 emplacement)** : les runs déclenchés en
rafale par les assets font la queue au lieu de se disputer les 6 Go du worker.

| Table Gold | Grain | Formule |
|---|---|---|
| `daily_transaction_volume` | jour × pays × entité × flux × type | nb, échecs, montant EUR (hors échecs) |
| `npl_ratio_by_country` | mois × pays × entité | encours des prêts en défaut (> 90 j) / encours total, photo fin de mois, seuil BCEAO 5 % |
| `customer_arpu_monthly` | mois × pays × entité × segment | (commissions + intérêts) / clients actifs distincts |
| `loss_ratio_by_product` | mois × pays × produit | sinistres payés / primes (+ cumul annuel), seuil CIMA 70 % |
| `claims_processing_time` | mois × pays × IARD/Vie | jours ouvrés moyens, médiane, p90 (sinistres clos) |
| `mobile_money_daily_flow` | jour × pays × opérateur | volume, montant, taux d'échec, utilisateurs actifs |
| `cross_border_transfers` | semaine × corridor | nb, montant total/moyen, évolution S/S-1, corridor UEMOA |

Choix : **Airflow 3.3** (branche 2.x en fin de vie), image construite en copiant le client Spark et le JRE
depuis l'image Spark (versions identiques driver/executors), Connections injectées par variables
d'environnement (`AIRFLOW_CONN_*`), enchaînement des DAGs par **assets** (data-aware scheduling).

## Level 3 — Speed layer (Lambda)

| Service | URL | Rôle |
|---|---|---|
| `kafka` (KRaft) | `kafka:9092` (interne), `localhost:9094` (VM) | bus d'événements, 8 partitions par topic, clé = `country_code` |
| `kafka-init` | — | crée les 11 topics (raw, silver, gold, DLQ), idempotent |
| `kafka-ui` | http://IP-VM:8085 | exploration des topics (démo) |
| `nifi` | https://IP-VM:8443/nifi | ListS3 → FetchS3Object → UpdateRecord (CSV→JSON + `ingestion_timestamp`, `source_file`) → PublishKafkaRecord |
| `nifi-init` | — | construit le flux NiFi par l'API REST (`nifi/provision_flow.py`, flow-as-code) |
| `stream-raw-silver` | — | Job 1 Spark Streaming : raw-* → validation (DLQ `dlq-financial-events`) → Silver (EUR, enrichissement) → topics `silver-*` + tables Iceberg `silver.rt_*` |

| `stream-silver-gold` | — | Job 2 : fraude (`gold-fraud-alerts`), AML (`gold-aml-events`), liquidité (`gold-liquidity-alerts`) + tables Iceberg `gold.rt_*` |

**Job 2 — règles** (seuils surchargeables par variables d'environnement) :

| Règle | Définition |
|---|---|
| `LARGE_TXN_BURST` | ≥ 3 transactions > 500 000 XOF (équiv. EUR) d'un même compte, fenêtre glissante 5 min / 1 min |
| `UNUSUAL_COUNTRY` | paiement mobile money depuis un pays absent du profil client (résidence + historique Silver) |
| `CLAIM_GT_3X_PREMIUM` | sinistre > 3 × prime annuelle (primes 12 mois, mensualités annualisées) |
| AML | virement > 1 000 000 XOF (UEMOA) / 5 000 GHS (Ghana), banque et mobile money |
| `LIQUIDITY_COVERAGE` | sorties nettes d'un pays sur 5 min > 1 % des dépôts (comptes courants + épargne) |

Le générateur (onglet « Flux continu ») peut injecter un scénario par règle à chaque micro-lot
(case « Injecter des scénarios de fraude ») : démonstration déterministe.

**Job 1 — choix.** Chaque micro-lot réutilise les contrats, la validation et les transformations Silver du
batch (une seule définition des règles). Dédoublonnage par identifiant dans une fenêtre de 10 min
(watermark sur l'horodatage Kafka). Tables `silver.rt_*` séparées des tables batch : le batch reste la
source de vérité et réécrit ses partitions sans conflit d’écriture avec le flux. Append exactement-une-fois
(numéro de micro-lot gravé dans le snapshot Iceberg, sans MERGE) +
checkpoint MinIO : un redémarrage ne perd ni ne duplique rien.

**Partage de `raw-landing` entre batch et streaming (Lambda).** NiFi liste le bucket toutes les 5 s
(état « Tracking Timestamps », aucun fichier publié deux fois). Le batch (`dag_ingest_raw`) n'ingère et
n'archive que les fichiers déposés depuis plus de `waba_batch_min_age_minutes` minutes (Variable Airflow,
défaut 5) : NiFi lit toujours un fichier avant son archivage, sans couplage entre les deux chaînes.

```bash
./scripts/kafka-check.sh                 # messages par topic + exemple
```

**Requête Lambda (Trino).** Le catalogue `kafka` (`trino/catalog/kafka.properties` + schémas JSON
`trino/kafka/*.json`) expose les topics `silver-*` et `gold-*` en SQL. `sql/level3_lambda.sql` contient la
requête de l'énoncé et une vue Lambda sans double comptage : la couche batch fait foi pour les jours
qu'elle a calculés, la couche temps réel complète les jours suivants.

```bash
cat sql/level3_lambda.sql | docker compose exec -T trino trino --catalog lakehouse --output-format ALIGNED
```

## Level 4 — Kubernetes (en cours)

```bash
./scripts/k8s/install-tools.sh     # kubectl, minikube, helm, helmfile (versions épinglées + SHA-256)
docker compose stop                # libère la mémoire : la stack Compose reste la démo des Levels 1-3
./scripts/k8s/deploy.sh            # cluster + secrets depuis .env + helmfile sync + état
```

| Namespace | Composants |
|---|---|
| `ingestion` | MinIO (StatefulSet + PVC, Job d'initialisation), NiFi, Kafka |
| `processing` | Catalogue Iceberg REST (PVC SQLite), Spark Operator, Airflow |
| `serving` | Trino, Superset |
| `governance` | Keycloak, OpenMetadata |
| `monitoring` | Prometheus, Grafana, Loki |

Secrets Kubernetes créés par `scripts/k8s/bootstrap.sh` depuis `.env` (rien dans les manifestes),
sondes liveness/readiness sur chaque composant, interfaces exposées par Ingress (`*.waba.local`).
