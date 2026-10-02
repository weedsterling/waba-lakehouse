-- ============================================================================
-- WABA Group — Level 2 : contrôles de la couche Gold (7 KPIs)
-- cat sql/level2_gold_checks.sql | docker compose exec -T trino trino --catalog lakehouse --output-format ALIGNED
-- ============================================================================

-- 1. Tables Gold et volumétrie
SELECT 'daily_transaction_volume' t, count(*) n FROM lakehouse.gold.daily_transaction_volume
UNION ALL SELECT 'npl_ratio_by_country', count(*) FROM lakehouse.gold.npl_ratio_by_country
UNION ALL SELECT 'customer_arpu_monthly', count(*) FROM lakehouse.gold.customer_arpu_monthly
UNION ALL SELECT 'loss_ratio_by_product', count(*) FROM lakehouse.gold.loss_ratio_by_product
UNION ALL SELECT 'claims_processing_time', count(*) FROM lakehouse.gold.claims_processing_time
UNION ALL SELECT 'mobile_money_daily_flow', count(*) FROM lakehouse.gold.mobile_money_daily_flow
UNION ALL SELECT 'cross_border_transfers', count(*) FROM lakehouse.gold.cross_border_transfers;

-- 2. Réconciliation Silver -> Gold (écart attendu = 0)
SELECT (SELECT sum(txn_count) FROM lakehouse.gold.daily_transaction_volume) gold_txn,
       (SELECT count(*) FROM lakehouse.silver.bank_transactions) + (SELECT count(*) FROM lakehouse.silver.mobile_money_payments)
     + (SELECT count(*) FROM lakehouse.silver.insurance_operations) + (SELECT count(*) FROM lakehouse.silver.loan_repayments) silver_txn;

-- 3. NPL par pays (dernier mois) — seuil BCEAO 5 %
SELECT country_code, entity_type, report_month, loans_count, npl_loans_count,
       round(npl_ratio * 100, 2) npl_pct, round(npl_ratio_count * 100, 2) npl_pct_nombre, is_above_threshold
FROM lakehouse.gold.npl_ratio_by_country
WHERE report_month = (SELECT max(report_month) FROM lakehouse.gold.npl_ratio_by_country)
ORDER BY country_code, entity_type;

-- 4. Loss ratio cumulé par pays et branche — seuil CIMA 70 %
SELECT country_code, insurance_branch, round(sum(premiums_eur), 0) primes_eur,
       round(sum(claims_paid_eur), 0) sinistres_eur,
       round(100 * sum(claims_paid_eur) / nullif(sum(premiums_eur), 0), 1) loss_ratio_pct
FROM lakehouse.gold.loss_ratio_by_product GROUP BY 1, 2 ORDER BY 1, 2;

-- 5. ARPU mensuel moyen par segment
SELECT customer_segment, round(sum(total_revenue_eur) / sum(active_customers), 2) arpu_eur,
       sum(active_customers) clients_mois
FROM lakehouse.gold.customer_arpu_monthly GROUP BY 1 ORDER BY 2 DESC;

-- 6. Délai de traitement des sinistres (jours ouvrés)
SELECT country_code, insurance_branch, sum(closed_claims_count) sinistres,
       round(sum(avg_working_days * closed_claims_count) / sum(closed_claims_count), 1) jours_ouvres_moyens
FROM lakehouse.gold.claims_processing_time GROUP BY 1, 2 ORDER BY 1, 2;

-- 7. Mobile Money : taux d'échec et utilisateurs actifs par pays
SELECT country_code, sum(txn_count) txn, round(100.0 * sum(failed_count) / sum(txn_count), 2) echec_pct,
       round(sum(total_amount_eur), 0) montant_eur, round(avg(active_users), 0) utilisateurs_actifs_jour
FROM lakehouse.gold.mobile_money_daily_flow GROUP BY 1 ORDER BY 1;

-- 8. Corridors transfrontaliers : top 10 et évolution de la dernière semaine
SELECT corridor, is_uemoa_corridor, sum(transfer_count) transferts, round(sum(total_amount_eur), 0) montant_eur,
       round(sum(total_amount_eur) / sum(success_count), 2) montant_moyen_eur
FROM lakehouse.gold.cross_border_transfers GROUP BY 1, 2 ORDER BY montant_eur DESC LIMIT 10;
