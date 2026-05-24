-- KPI 3 : Temps écoulé depuis la dernière transaction (détection vitesse anormale)
CREATE OR REPLACE VIEW `fintec-496811.fraud_detection.v_kpi_lag_behavior` AS
SELECT
  nameOrig,
  step,
  type,
  amount,
  LAG(step, 1) OVER (PARTITION BY nameOrig ORDER BY step)            AS prev_step,
  step - LAG(step, 1) OVER (PARTITION BY nameOrig ORDER BY step)     AS steps_since_last_tx
FROM `fintec-496811.fraud_detection.raw_transactions_partitioned_clustered_v2`
WHERE step BETWEEN 100 AND 200;
