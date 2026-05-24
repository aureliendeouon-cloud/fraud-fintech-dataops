-- KPI 2 : Cumul du volume financier envoyé par utilisateur (anti-blanchiment)
CREATE OR REPLACE VIEW `fintec-496811.fraud_detection.v_kpi_running_total` AS
SELECT
  nameOrig,
  step,
  amount,
  SUM(amount) OVER (
    PARTITION BY nameOrig
    ORDER BY step
  ) AS cumulative_amount
FROM `fintec-496811.fraud_detection.raw_transactions_partitioned_clustered_v2`
WHERE step BETWEEN 100 AND 200;
