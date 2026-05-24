-- KPI 4 : Détection du smurfing (fractionnement de transactions)
CREATE OR REPLACE VIEW `fintec-496811.fraud_detection.v_kpi_smurfing` AS
WITH velocity_analysis AS (
  SELECT
    nameOrig,
    step,
    type,
    amount,
    IFNULL(step - LAG(step, 1) OVER (PARTITION BY nameOrig ORDER BY step), -1) AS steps_since_last_tx,
    COUNT(*)   OVER (PARTITION BY nameOrig, step)                               AS nb_tx_same_step,
    SUM(amount) OVER (
      PARTITION BY nameOrig
      ORDER BY step
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )                                                                            AS rolling_sum_3_tx
  FROM `fintec-496811.fraud_detection.raw_transactions_partitioned_clustered_v2`
  WHERE step BETWEEN 100 AND 200
)
SELECT *
FROM velocity_analysis
WHERE steps_since_last_tx = 0
  AND nb_tx_same_step >= 3;
