-- ============================================================================
-- WABA Group — Level 2 : contrôles de la couche Silver
-- cat sql/level2_silver_checks.sql | docker compose exec -T trino trino --catalog lakehouse --output-format ALIGNED
-- ============================================================================

-- 1. Tables Silver
SHOW TABLES FROM lakehouse.silver;

-- 2. Conservation des volumes Bronze -> Silver (les écarts doivent être nuls)
SELECT 'bank_transactions' t, (SELECT count(*) FROM lakehouse.bronze.bank_transactions) bronze,
       (SELECT count(*) FROM lakehouse.silver.bank_transactions) silver
UNION ALL SELECT 'insurance_operations', (SELECT count(*) FROM lakehouse.bronze.insurance_operations),
       (SELECT count(*) FROM lakehouse.silver.insurance_operations)
UNION ALL SELECT 'mobile_money_payments', (SELECT count(*) FROM lakehouse.bronze.mobile_money_payments),
       (SELECT count(*) FROM lakehouse.silver.mobile_money_payments)
UNION ALL SELECT 'loan_repayments', (SELECT count(*) FROM lakehouse.bronze.loan_repayments),
       (SELECT count(*) FROM lakehouse.silver.loan_repayments);

-- 3. Unicité de la clé métier après dédoublonnage (0 ligne attendue)
SELECT transaction_id, count(*) FROM lakehouse.silver.bank_transactions GROUP BY 1 HAVING count(*) > 1;

-- 4. Conversion EUR : volumes par pays en devise locale et en euros
SELECT country_code, currency, count(*) nb, round(sum(amount), 0) montant_local,
       round(sum(amount_eur), 0) montant_eur, round(avg(fx_units_per_eur), 4) taux_moyen
FROM lakehouse.silver.bank_transactions GROUP BY 1, 2 ORDER BY 1;

-- 5. Qualité : orphelins et valeurs aberrantes (orphelins attendus = 0)
SELECT dataset, country_code, rows, flags, computed_at
FROM lakehouse.audit.dq_metrics
WHERE computed_at = (SELECT max(computed_at) FROM lakehouse.audit.dq_metrics)
ORDER BY dataset, country_code;

-- 6. Enrichissement : segments clients présents, pas de 'UNKNOWN'
SELECT customer_segment, count(*) FROM lakehouse.silver.bank_transactions GROUP BY 1 ORDER BY 2 DESC;

-- 7. Corridors transfrontaliers mobile money (préparation Gold)
SELECT corridor, count(*) nb, round(sum(amount_eur), 0) montant_eur
FROM lakehouse.silver.mobile_money_payments WHERE is_cross_border
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
