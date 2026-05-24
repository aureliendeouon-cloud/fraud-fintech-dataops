-- KPI 1 : Moyenne glissante sur les 3 dernières transactions par utilisateur
CREATE OR REPLACE VIEW `fintec-496811.fraud_detection.v_kpi_rolling_avg` AS
SELECT
  step,
  nameOrig,
  amount,
  AVG(amount) OVER (
    PARTITION BY nameOrig
    ORDER BY step
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS avg_amount_3_tx
FROM `fintec-496811.fraud_detection.raw_transactions_partitioned_clustered_v2`
WHERE step BETWEEN 100 AND 200;
