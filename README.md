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

## 3. Démarrage from scratch

```powershell
git clone <url-du-depot> waba-lakehouse
cd waba-lakehouse
Copy-Item .env.example .env        # bash : cp .env.example .env
# éditer .env : remplacer toutes les valeurs "change-me"

docker compose up -d --build       # 1er build : ~5-10 min (téléchargement des images et jars)
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
├── tests/                      # pytest : générateur + validation Spark
└── docs/                       # ROADMAP (installation + niveaux 2-4), ARCHITECTURE (write-up)
```

## 6. Tests

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
$env:PYTHONPATH="generator;spark"; pytest -q tests
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
* **MinIO** : l'édition communautaire n'est plus distribuée sur Docker Hub ; les images sont tirées de `quay.io` et figées sur une release. Alternatives S3 compatibles : SeaweedFS, Garage.
* Le catalogue REST (SQLite) est mono-instance : adapté au développement, pas à la haute disponibilité.

## 9. Dépannage

| Symptôme | Cause / solution |
|---|---|
| `/bin/sh^M: bad interpreter` dans `minio-init` | Fins de ligne CRLF : `git config core.autocrlf input` puis re-cloner (le `.gitattributes` force LF). |
| Spark `OutOfMemoryError` sur les référentiels | Augmenter `SPARK_WORKER_MEMORY` et la RAM allouée à WSL2 (`.wslconfig`). |
| Trino : `Table not found` | Lancer l'ingestion d'abord ; vérifier `curl http://localhost:8181/v1/namespaces`. |
| Port déjà utilisé | Modifier le port publié côté hôte dans `docker-compose.yml`. |
| Réinitialisation complète | `docker compose down -v` (⚠️ supprime les volumes : données et catalogue). |
