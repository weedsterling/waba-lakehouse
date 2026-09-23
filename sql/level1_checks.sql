-- ============================================================================
-- WABA Group — Level 1 : requêtes de contrôle et d'analyse via Trino
-- Exécution : docker compose exec trino trino --catalog lakehouse --file /dev/stdin < sql/level1_checks.sql
--         ou  docker compose exec -it trino trino --catalog lakehouse   (mode interactif)
-- ============================================================================

-- 1. Les 8 tables raw.* existent
SHOW TABLES FROM lakehouse.raw;

-- 2. Volumétrie par table
SELECT 'customers' AS table_name, count(*) AS rows FROM lakehouse.raw.customers
UNION ALL SELECT 'accounts', count(*) FROM lakehouse.raw.accounts
UNION ALL SELECT 'branches', count(*) FROM lakehouse.raw.branches
UNION ALL SELECT 'products', count(*) FROM lakehouse.raw.products
UNION ALL SELECT 'bank_transactions', count(*) FROM lakehouse.raw.bank_transactions
UNION ALL SELECT 'insurance_operations', count(*) FROM lakehouse.raw.insurance_operations
UNION ALL SELECT 'mobile_money_payments', count(*) FROM lakehouse.raw.mobile_money_payments
UNION ALL SELECT 'loan_repayments', count(*) FROM lakehouse.raw.loan_repayments;

-- 3. Soldes par pays (comptes actifs, devise locale)
SELECT country_code, currency, account_type,
       count(*)                       AS nb_comptes,
       round(sum(balance), 0)         AS encours_total,
       round(avg(balance), 0)         AS solde_moyen
FROM lakehouse.raw.accounts
WHERE status = 'ACTIVE'
GROUP BY country_code, currency, account_type
ORDER BY country_code, account_type;

-- 4. Volumes de transactions bancaires par pays et par jour
SELECT country_code, date(timestamp) AS txn_date, transaction_type,
       count(*) AS nb_txn, round(sum(amount), 0) AS montant_total
FROM lakehouse.raw.bank_transactions
WHERE transaction_status = 'SUCCESS'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
LIMIT 100;

-- 5. Comptages par entité et par pays (toutes sources)
SELECT entity_type, country_code, source, count(*) AS nb
FROM (
          SELECT entity_type, country_code, 'bank_transactions'     AS source FROM lakehouse.raw.bank_transactions
UNION ALL SELECT entity_type, country_code, 'insurance_operations'  FROM lakehouse.raw.insurance_operations
UNION ALL SELECT entity_type, country_code, 'mobile_money_payments' FROM lakehouse.raw.mobile_money_payments
UNION ALL SELECT entity_type, country_code, 'loan_repayments'       FROM lakehouse.raw.loan_repayments
)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- 6. Contrôle d'idempotence : doit retourner 0 ligne
SELECT transaction_id, count(*) FROM lakehouse.raw.bank_transactions
GROUP BY transaction_id HAVING count(*) > 1;

-- 7. Intégrité référentielle : doit retourner 0 pour chaque contrôle
SELECT 'bank_txn.account_id orphelin' AS controle, count(*) AS nb
FROM lakehouse.raw.bank_transactions t LEFT JOIN lakehouse.raw.accounts a ON t.account_id = a.account_id
WHERE a.account_id IS NULL
UNION ALL
SELECT 'bank_txn.branch_id orphelin', count(*)
FROM lakehouse.raw.bank_transactions t LEFT JOIN lakehouse.raw.branches b ON t.branch_id = b.branch_id
WHERE b.branch_id IS NULL
UNION ALL
SELECT 'insurance.customer_id orphelin', count(*)
FROM lakehouse.raw.insurance_operations i LEFT JOIN lakehouse.raw.customers c ON i.customer_id = c.customer_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT 'mobile_money.receiver_id orphelin', count(*)
FROM lakehouse.raw.mobile_money_payments m LEFT JOIN lakehouse.raw.customers c ON m.receiver_id = c.customer_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT 'loans.loan_account_id orphelin', count(*)
FROM lakehouse.raw.loan_repayments l LEFT JOIN lakehouse.raw.accounts a ON l.loan_account_id = a.account_id
WHERE a.account_id IS NULL;

-- 8. Partitionnement Iceberg (country_code, jour)
SELECT * FROM lakehouse.raw."bank_transactions$partitions" LIMIT 20;

-- 9. PII : l'IBAN n'est jamais stocké en clair
SELECT account_id, iban_masked, substr(iban_hash, 1, 16) AS iban_hash_prefix
FROM lakehouse.raw.accounts LIMIT 5;

-- 10. Audit d'ingestion et rejets
SELECT dataset, status, count(*) AS fichiers, sum(rows_read) AS lues,
       sum(rows_valid) AS valides, sum(rows_rejected) AS rejetees
FROM lakehouse.audit.ingestion_log GROUP BY 1, 2 ORDER BY 1;

SELECT dataset, reject_reasons, count(*) AS nb
FROM lakehouse.audit.rejected_records GROUP BY 1, 2 ORDER BY 3 DESC;

-- 11. Aperçu des indicateurs métier réalistes (préparation Level 2)
-- Taux de défaut (en nombre) par pays : attendu entre ~3 % et ~8 %
SELECT country_code,
       round(100.0 * count_if(repayment_status = 'DEFAULT') / count(*), 2) AS pct_default
FROM lakehouse.raw.loan_repayments GROUP BY 1 ORDER BY 1;

-- Loss ratio par pays : attendu entre 50 % et 85 %
SELECT country_code,
       round(100.0 * sum(amount) FILTER (WHERE operation_type = 'CLAIM_PAYMENT')
             / sum(amount) FILTER (WHERE operation_type IN ('PREMIUM_PAYMENT', 'POLICY_RENEWAL')), 1) AS loss_ratio_pct
FROM lakehouse.raw.insurance_operations GROUP BY 1 ORDER BY 1;
