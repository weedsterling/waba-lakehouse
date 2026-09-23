# Guide d'installation & feuille de route (Levels 1 → 4)

Machine cible : **Windows 10/11, 32 Go RAM**, 8 cœurs recommandés, 100 Go de disque libre.

---

## Partie A — Logiciels à installer (dans cet ordre)

### A.1 Socle Windows

| # | Logiciel | Pourquoi | Installation (PowerShell **administrateur**) |
|---|---|---|---|
| 1 | **WSL2 + Ubuntu 24.04** | Linux natif sous Windows : Docker, scripts bash, performances I/O | `wsl --install -d Ubuntu-24.04` puis redémarrer |
| 2 | **Docker Desktop** (backend WSL2) | Exécute tous les conteneurs (Levels 1-3) | `winget install -e --id Docker.DockerDesktop` |
| 3 | **Git** | Versionnement + dépôt GitHub/GitLab | `winget install -e --id Git.Git` |
| 4 | **VS Code** + extensions *Python, Pylance, Docker, WSL, YAML, Kubernetes, Ruff* | IDE | `winget install -e --id Microsoft.VisualStudioCode` |
| 5 | **DBeaver Community** | Client SQL graphique pour Trino | `winget install -e --id dbeaver.dbeaver` |
| 6 | **OBS Studio** | Vidéo de démo (livrable) | `winget install -e --id OBSProject.OBSStudio` |

### A.2 Dans Ubuntu (WSL) — tests locaux et outillage

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk python3-venv python3-pip make jq curl unzip
# Python 3.12 est fourni par Ubuntu 24.04
java -version && python3 --version
```

### A.3 Level 4 uniquement (Kubernetes)

| Logiciel | Installation (PowerShell) |
|---|---|
| **kubectl** | `winget install -e --id Kubernetes.kubectl` |
| **Minikube** | `winget install -e --id Kubernetes.minikube` |
| **Helm 3** | `winget install -e --id Helm.Helm` |
| **k9s** (optionnel, supervision du cluster) | `winget install -e --id Derailed.k9s` |

### A.4 Réglages indispensables

**1. Mémoire WSL2** — créer `C:\Users\<vous>\.wslconfig` puis `wsl --shutdown` :

```ini
[wsl2]
memory=22GB        # Levels 1-3 : 12 Go suffisent ; Level 4 : 20-24 Go
processors=8
swap=8GB
```

**2. Docker Desktop** → Settings → Resources → WSL integration : activer Ubuntu-24.04.

**3. Git** — fins de ligne (sinon les scripts `.sh` cassent dans les conteneurs) :

```bash
git config --global core.autocrlf input
git config --global user.name "Mohamed FOFANA"
git config --global user.email "<votre email>"
```

**4. Travailler dans le système de fichiers Linux** (`~/projects/waba-lakehouse` dans WSL, et non `C:\…`) :
les montages de volumes Docker depuis `/mnt/c` sont 5 à 10 fois plus lents. Ouvrir le projet avec
`code .` depuis le terminal Ubuntu (VS Code en mode *Remote-WSL*).

**5. Vérification** :

```bash
docker run --rm hello-world
docker compose version
```

---

## Partie B — Feuille de route sur 14 jours

| Jours | Niveau | Objectif livrable | Critère de sortie |
|---|---|---|---|
| J1 | Setup + L1 | Installer, lancer la stack fournie, générer, ingérer, requêter | `sql/level1_checks.sql` passe entièrement |
| J2 | L1 | Relire/maîtriser le code, pousser sur Git, captures pour la vidéo | Dépôt Git propre, README vérifié from scratch |
| J3-J5 | L2 | Airflow + médaillon Bronze/Silver/Gold + 7 KPIs Gold | 4 DAGs verts, 7 tables Gold requêtables dans Trino |
| J6-J8 | L3 | NiFi → Kafka → Spark Structured Streaming, fraude, AML, DLQ | Alerte fraude visible dans `gold-fraud-alerts`, requête Lambda OK |
| J9-J12 | L4 | Minikube + Helm, Superset, Keycloak, OpenMetadata, observabilité | `helm install` unique, 3 dashboards, 3 alertes déclenchées |
| J13 | Livrables | Write-up (2-5 p.) + vidéo 5-10 min | Documents relus |
| J14 | Tampon | Corrections, test from scratch sur une machine propre | Envoi du lien Git avant l'échéance |

> Règle d'or : **un niveau n'est commencé que lorsque le précédent passe ses critères**. Un Level 3
> fragile vaut moins qu'un Level 2 impeccable. Committer à chaque étape fonctionnelle.

---

## Partie C — Level 2 : points de conception

**Stack** : Airflow 2.10 (image officielle `apache/airflow`, LocalExecutor + PostgreSQL), ajouté au `docker-compose.yml`.

1. **Image Airflow personnalisée** : `apache/airflow` + OpenJDK 17 + `pyspark==3.5.3` + `apache-airflow-providers-apache-spark`
   → `SparkSubmitOperator` vers `spark://spark-master:7077` (connexion Airflow `spark_default`, jamais de credential dans le code).
2. **Bronze** : réutiliser `ingest_raw.py` en paramétrant l'espace de noms cible (`raw` → `bronze`) ; partitionnement `country_code` + date d'ingestion.
3. **Silver** (`dag_bronze_to_silver`) :
   * dédoublonnage (`row_number()` sur l'id, dernière `_ingestion_ts`) ;
   * conversion EUR : XOF → **parité fixe 1 EUR = 655,957 XOF** ; GHS → table `silver.fx_rates` (taux quotidien simulé) ;
   * jointures `customers`, `accounts`, `branches`, `products` (LEFT JOIN + indicateur `is_orphan`) ;
   * gestion des nulls (`coalesce`, valeurs `UNKNOWN`), bornage des valeurs aberrantes (montants > p99,9 signalés).
4. **Gold** (`dag_silver_to_gold`) — 7 tables :

| Table | Calcul |
|---|---|
| `daily_transaction_volume` | `count`, `sum(amount_eur)` par jour, pays, entité, type |
| `npl_ratio_by_country` | encours des prêts en `DEFAULT` / encours total, par pays et type ; `is_above_bceao_threshold = ratio > 5 %` |
| `customer_arpu_monthly` | (frais + intérêts perçus) / `COUNT(DISTINCT customer_id)` par mois, pays, segment |
| `loss_ratio_by_product` | sinistres payés / primes acquises par produit, pays, mois ; seuil CIMA 70 % |
| `claims_processing_time` | moyenne de `processing_days` par pays, IARD/Vie |
| `mobile_money_daily_flow` | volume, montant, taux d'échec, utilisateurs actifs (`count distinct sender_id`) |
| `cross_border_transfers` | flux par corridor `sender_country-receiver_country`, montant moyen, évolution hebdo |

5. **`dag_regulatory_report`** : `schedule="30 0 * * *"` (00h30 UTC), `catchup=False`, écrit `gold.regulatory_bceao_daily` et `gold.regulatory_cima_daily`.
6. **Bonnes pratiques attendues** : `retries=3`, `retry_delay`, `on_failure_callback` (alerte), `params={"country_codes": [...]}` pour backfill sélectif, enchaînement via `TriggerDagRunOperator` ou *Datasets* Airflow, écriture Gold en `INSERT OVERWRITE` par partition (idempotente).

## Partie D — Level 3 : points de conception

* **Kafka** en mode **KRaft** (image `apache/kafka:3.8.x`, pas de Zookeeper) + `kafka-ui` pour la démo ; topics créés par un conteneur init (partitions = 8, une par pays).
* **NiFi** (`apache/nifi:1.28.x`) : `ListS3` → `FetchS3Object` → `ConvertRecord` (CSVReader → JsonRecordSetWriter) → `UpdateRecord` (`ingestion_timestamp`, `source_file`) → `PublishKafkaRecord_2_6` (topic calculé depuis le chemin S3, clé = `country_code`). Back-pressure : 10 000 objets / 1 Go par connexion. Exporter le flow en JSON dans le dépôt.
* ⚠️ **Conflit Lambda** : Spark batch archive les fichiers que NiFi doit aussi lire. Solution : NiFi lit en premier (état de `ListS3`) et le batch n'archive que les fichiers de plus de N minutes, **ou** notifications MinIO → Kafka. Documenter le choix.
* **Job 1 (Raw → Silver)** : `from_json` avec schéma, messages invalides → `dlq-financial-events`, `withWatermark("event_time", "10 minutes").dropDuplicatesWithinWatermark(["transaction_id"])` (Spark 3.5), `foreachBatch` pour le double sink Kafka + Iceberg, checkpoints sur MinIO.
* **Job 2 (Silver → Gold)** : fenêtres `window(event_time, "5 minutes", "1 minute")` ; règles : > 500 000 XOF multiples sur un compte en 5 min, pays inhabituel pour le client, sinistre > 3 × prime annuelle ; AML : > 1 000 000 XOF / 5 000 GHS → `gold-aml-events` ; liquidité : soldes agrégés sous seuil.
* **Trino** : connecteur Kafka (`kafka.table-description-dir` avec les définitions JSON des topics silver) pour la requête Lambda unifiée.
* Faire générer des **cas de fraude volontaires** par le générateur (option à ajouter) pour que la démo soit déterministe.

## Partie E — Level 4 : points de conception

* **Cluster** : `minikube start --cpus 8 --memory 24g --disk-size 80g --addons ingress,metrics-server`.
* **Charts officiels** (regroupés dans un *umbrella chart* `helm/waba-platform` → un seul `helm install`) :
  Spark Operator (kubeflow), Airflow (apache-airflow), Strimzi (Kafka), Trino (trinodb), Superset (apache), Keycloak,
  OpenMetadata, kube-prometheus-stack, Loki + Promtail. MinIO et NiFi : manifestes/chart maison simples.
* **Namespaces** : `ingestion` (MinIO, NiFi, Kafka), `processing` (Spark, Airflow), `serving` (Trino, Superset), `governance` (Keycloak, OpenMetadata), `monitoring`.
* Secrets via `kubectl create secret` / Sealed Secrets, jamais dans les manifestes ; probes liveness/readiness ; PVC MinIO, PostgreSQL Airflow, OpenMetadata.
* **Keycloak** : realm `waba`, clients OIDC `superset` et `trino`, rôles `group_admin`, `country_analyst`, `compliance_officer`, `viewer` ; filtrage pays côté Superset (Row Level Security) et règles d'accès Trino (`rules.json`) pour masquer les colonnes sensibles au rôle `viewer`.
* **OpenMetadata** : ingestion Trino, 5 tables Gold documentées, tags PII sur `customer_id`, `account_number`, `iban_*`, lineage raw → bronze → silver → gold.
* **Observabilité** : exporters JMX (Kafka, Spark, Trino), `statsd-exporter` Airflow, logs JSON → Promtail → Loki ; 3 alertes Grafana (job fraude en erreur > 5 min, lag consumer AML > 5 000, `dag_regulatory_report` en échec à 06h00 UTC).
* ⚠️ **Budget mémoire** : OpenMetadata (+ OpenSearch) et Superset sont les plus gourmands ; réduire les replicas à 1, limiter les requests/limits, et démontrer les composants par vagues si nécessaire.

---

## Partie F — Conseils pour se distinguer (niveau sénior)

1. **Justifier chaque compromis** dans le write-up (catalogue REST vs Hive, merge-on-read vs copy-on-write, KRaft vs Zookeeper…).
2. **Qualité de code** : typage, `ruff`, tests `pytest`, logs JSON, aucune UDF Python inutile.
3. **Reproductibilité** : tester le README from scratch (`docker compose down -v`, nouveau clone).
4. **Démo scénarisée** : générer → ingérer → requêter → rejouer (idempotence) → alerte fraude → dashboard.
5. **Utiliser l'IA de façon traçable** (recommandé par l'énoncé) : expliquer dans le write-up comment elle a été utilisée et comment le code a été vérifié.
