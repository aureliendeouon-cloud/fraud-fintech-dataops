CREATE OR REPLACE VIEW `fintec-496811.fraud_detection.v_dashboard_anomalies` AS
WITH velocity AS (
  SELECT
    f.step,
    f.type,
    f.amount,
    f.isFraud,
    orig.user_name AS orig_user_name,
    -- Délai depuis la dernière transaction
    IFNULL(
      f.step - LAG(f.step, 1) OVER (PARTITION BY f.orig_user_id ORDER BY f.step),
      -1
    ) AS steps_since_last_tx,
    -- Nombre de transactions sur le même step (même heure simulée)
    COUNT(*) OVER (PARTITION BY f.orig_user_id, f.step) AS nb_tx_same_step,
    -- Somme cumulée sur les 3 dernières transactions
    SUM(f.amount) OVER (
      PARTITION BY f.orig_user_id
      ORDER BY f.step
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_sum_3_tx,
    -- Score de fraude
    `fintec-496811.fraud_detection.GET_TEMPORAL_SCORE`(
      IFNULL(
        f.step - LAG(f.step, 1) OVER (PARTITION BY f.orig_user_id ORDER BY f.step),
        -1
      ),
      f.type,
      f.amount
    ) AS fraud_behavior_score
  FROM `fintec-496811.fraud_detection.fact_transactions` f
  JOIN `fintec-496811.fraud_detection.dim_users` orig ON f.orig_user_id = orig.user_id
)
SELECT *
FROM velocity
WHERE steps_since_last_tx = 0
  AND nb_tx_same_step >= 2
  AND type IN ('TRANSFER', 'CASH_OUT');
