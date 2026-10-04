-- ============================================================================
-- WABA Group — Level 3 : contrôles du Job 1 (raw -> silver temps réel)
-- cat sql/level3_stream_checks.sql | docker compose exec -T trino trino --catalog lakehouse --output-format ALIGNED
-- ============================================================================

-- 1. Tables Silver temps réel (rt_*) : volumes et fraîcheur
SELECT 'rt_bank_transactions' t, count(*) n, max(_silver_ts) derniere_ecriture FROM lakehouse.silver.rt_bank_transactions
UNION ALL SELECT 'rt_insurance_operations', count(*), max(_silver_ts) FROM lakehouse.silver.rt_insurance_operations
UNION ALL SELECT 'rt_mobile_money_payments', count(*), max(_silver_ts) FROM lakehouse.silver.rt_mobile_money_payments
UNION ALL SELECT 'rt_loan_repayments', count(*), max(_silver_ts) FROM lakehouse.silver.rt_loan_repayments;

-- 2. Unicité (0 ligne attendue, même après redémarrage du job)
SELECT transaction_id, count(*) FROM lakehouse.silver.rt_bank_transactions GROUP BY 1 HAVING count(*) > 1;

-- 3. Conversion EUR et enrichissement : identiques au batch
SELECT country_code, currency, count(*) nb, round(avg(fx_units_per_eur), 4) taux,
       sum(CASE WHEN is_orphan_account THEN 1 ELSE 0 END) orphelins,
       sum(CASE WHEN is_outlier THEN 1 ELSE 0 END) outliers
FROM lakehouse.silver.rt_bank_transactions GROUP BY 1, 2 ORDER BY 1;
