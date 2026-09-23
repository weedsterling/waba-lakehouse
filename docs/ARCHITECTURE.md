# Write-up technique — trame (à compléter au fil des niveaux, 2 à 5 pages)

## 1. Contexte et objectifs
WABA Group : 8 pays, 4 lignes métier, données dispersées (core banking T24, CBS Assurance, API mobile money,
agences). Objectif : une plateforme Lakehouse unique, batch + streaming, gouvernée et déployable sur Kubernetes.

## 2. Architecture cible (Lambda)
* **Batch layer** : raw-landing (CSV) → Spark → Iceberg Bronze/Silver/Gold, orchestré par Airflow.
* **Speed layer** : NiFi → Kafka → Spark Structured Streaming → topics silver/gold + Iceberg.
* **Serving layer** : Trino (Iceberg + Kafka) → Superset ; gouvernance OpenMetadata, SSO Keycloak.

## 3. Choix structurants et compromis
| Décision | Alternatives étudiées | Raison du choix | Compromis accepté |
|---|---|---|---|
| Apache Iceberg | Delta Lake, Hudi | Format ouvert, moteur-agnostique (Spark + Trino), partition cachée, time travel | Maintenance (compaction, expiration des snapshots) à planifier |
| Catalogue Iceberg REST | Hive Metastore, JDBC | Standard du protocole, pas de dépendance Hadoop | Implémentation de dev (SQLite) : non HA |
| MERGE sur clé métier | append + dédoublonnage aval | Idempotence garantie à l'écriture | Coût du MERGE sur gros volumes (atténué par l'élagage `country_code`) |
| Rejets tracés (`audit.rejected_records`) | abandon silencieux (`DROPMALFORMED`) | Auditabilité exigée en contexte bancaire | Volume de stockage supplémentaire |
| Pseudonymisation SHA-256 salée | chiffrement réversible, tokenisation | Simple, jointures possibles, irréversible | Rotation du sel = recalcul des hash |

## 4. Qualité, sécurité, conformité
Contrats de données déclaratifs (`spark/waba_spark/schemas.py`), contrôle devise/pays, masquage PII,
compte de service MinIO, secrets hors du code, archivage versionné.

## 5. Limites connues et prochaines étapes
(voir README §8, à compléter pour chaque niveau)

## 6. Utilisation de l'IA
Décrire les outils utilisés, pour quelles tâches, et la manière dont le code produit a été relu et testé.
