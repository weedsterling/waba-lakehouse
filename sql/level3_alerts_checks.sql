-- ============================================================================
-- WABA Group — Level 3 : contrôles du Job 2 (fraude, AML, liquidité)
-- cat sql/level3_alerts_checks.sql | docker compose exec -T trino trino --catalog lakehouse --output-format ALIGNED
-- ============================================================================

-- 1. Alertes par règle et pays
SELECT rule_code, country_code, count(*) alertes, max(txn_count) max_txn, round(sum(amount_eur), 0) montant_eur,
       max(detected_at) derniere
FROM lakehouse.gold.rt_fraud_alerts GROUP BY 1, 2 ORDER BY 1, 2;

-- 2. AML : événements au-dessus du seuil déclaratif (devise locale)
SELECT country_code, source, currency, count(*) evenements, min(amount_local) min_local, max(threshold_local) seuil
FROM lakehouse.gold.rt_aml_events GROUP BY 1, 2, 3 ORDER BY 1, 2;

-- 3. Liquidité : fenêtres en alerte
SELECT country_code, window_start, window_end, round(amount_eur, 0) sorties_nettes_eur, details
FROM lakehouse.gold.rt_liquidity_alerts ORDER BY window_start DESC LIMIT 10;

-- 4. Détail des 5 dernières alertes de fraude
SELECT rule_code, subject_id, txn_count, round(amount_eur, 2) montant_eur, details
FROM lakehouse.gold.rt_fraud_alerts ORDER BY detected_at DESC LIMIT 5;
