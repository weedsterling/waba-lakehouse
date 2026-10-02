-- ============================================================================
-- WABA Group — Level 2 : contrôles du reporting réglementaire (dernier rapport)
-- cat sql/level2_regulatory_checks.sql | docker compose exec -T trino trino --catalog lakehouse --output-format ALIGNED
-- ============================================================================

-- 1. Rapports disponibles
SELECT 'bceao_prudential' rapport, report_date, count(*) lignes, sum(CASE WHEN is_breach THEN 1 ELSE 0 END) depassements
FROM lakehouse.reporting.bceao_prudential GROUP BY 2
UNION ALL
SELECT 'cima_technical', report_date, count(*), sum(CASE WHEN is_breach THEN 1 ELSE 0 END)
FROM lakehouse.reporting.cima_technical GROUP BY 2
ORDER BY 2 DESC, 1;

-- 2. BCEAO : NPL par régulateur, pays et entité
SELECT regulator, country_code, entity_type, data_month, round(npl_ratio * 100, 2) npl_pct,
       round(npl_ratio_count * 100, 2) npl_pct_nombre, is_breach
FROM lakehouse.reporting.bceao_prudential
WHERE report_date = (SELECT max(report_date) FROM lakehouse.reporting.bceao_prudential)
ORDER BY regulator, country_code, entity_type;

-- 3. CIMA : produits en dépassement (loss ratio cumulé > 70 %)
SELECT regulator, country_code, product_line, period_start, data_month, round(loss_ratio_ytd * 100, 1) lr_pct,
       avg_claim_working_days_ytd jours_ouvres
FROM lakehouse.reporting.cima_technical
WHERE report_date = (SELECT max(report_date) FROM lakehouse.reporting.cima_technical) AND is_breach
ORDER BY lr_pct DESC;
