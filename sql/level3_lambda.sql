-- ============================================================================
-- WABA Group — Level 3 : requête Lambda unifiée (Iceberg batch + Kafka temps réel via Trino)
-- cat sql/level3_lambda.sql | docker compose exec -T trino trino --catalog lakehouse --output-format ALIGNED
-- ============================================================================

-- 1. Requête de l'énoncé, telle quelle (critère : « s'exécute sans erreur »)
SELECT
    COALESCE(b.country_code, s.country_code) AS country,
    COALESCE(b.txn_date, CAST(s.event_time AS DATE)) AS date,
    COALESCE(b.total_amount_eur, 0) + COALESCE(s.streaming_amount_eur, 0) AS total_eur
FROM gold.daily_transaction_volume b
FULL OUTER JOIN kafka.default."silver-bank-transactions" s
    ON b.country_code = s.country_code
    AND b.txn_date = CAST(s.event_time AS DATE)
ORDER BY 1, 2
LIMIT 20;

-- 2. Vue Lambda correcte : la couche batch fait foi pour les jours qu'elle a calculés ; la couche
--    temps réel complète uniquement les jours postérieurs (pas de double comptage, une ligne par jour).
WITH batch AS (
    SELECT country_code, txn_date, sum(txn_count) AS txn, sum(total_amount_eur) AS amount_eur
    FROM gold.daily_transaction_volume WHERE flow = 'BANK' GROUP BY 1, 2
), cutoff AS (
    SELECT max(txn_date) AS last_batch_day FROM batch
), speed AS (
    SELECT country_code, CAST(event_time AS DATE) AS txn_date,
           count(*) AS txn, sum(CASE WHEN transaction_status <> 'FAILED' THEN streaming_amount_eur END) AS amount_eur
    FROM kafka.default."silver-bank-transactions"
    WHERE CAST(event_time AS DATE) > (SELECT last_batch_day FROM cutoff)
    GROUP BY 1, 2
)
SELECT COALESCE(b.country_code, s.country_code) AS country,
       COALESCE(b.txn_date, s.txn_date) AS date,
       CASE WHEN b.txn_date IS NOT NULL THEN 'batch' ELSE 'speed' END AS layer,
       COALESCE(b.txn, 0) + COALESCE(s.txn, 0) AS txn,
       round(COALESCE(b.amount_eur, 0) + COALESCE(s.amount_eur, 0), 2) AS total_amount_eur
FROM batch b FULL OUTER JOIN speed s ON b.country_code = s.country_code AND b.txn_date = s.txn_date
WHERE COALESCE(b.txn_date, s.txn_date) >= (SELECT last_batch_day FROM cutoff) - INTERVAL '3' DAY
ORDER BY 2 DESC, 1;

-- 3. Supervision temps réel : alertes de fraude des 30 dernières minutes, lues directement dans Kafka
SELECT rule_code, country_code, count(DISTINCT subject_id) sujets, count(*) alertes, max(detected_at) derniere
FROM kafka.default."gold-fraud-alerts"
WHERE detected_at > localtimestamp - INTERVAL '30' MINUTE   -- fuseau de session : UTC
GROUP BY 1, 2 ORDER BY 1, 2;
