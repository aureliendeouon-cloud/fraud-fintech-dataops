CREATE OR REPLACE VIEW `fintec-496811.fraud_detection.v_dashboard_transactions` AS
SELECT
  f.step,
  f.type,
  f.amount,
  f.isFraud,
  f.isFlaggedFraud,
  f.oldbalanceOrg,
  f.newbalanceOrig,
  f.oldbalanceDest,
  f.newbalanceDest,
  orig.user_name  AS orig_user_name,
  dest.user_name  AS dest_user_name,
  -- Score d'agressivité temporelle via UDF
  `fintec-496811.fraud_detection.GET_TEMPORAL_SCORE`(
    IFNULL(
      f.step - LAG(f.step, 1) OVER (PARTITION BY f.orig_user_id ORDER BY f.step),
      -1
    ),
    f.type,
    f.amount
  ) AS fraud_behavior_score,
  -- Moyenne glissante sur les 3 dernières transactions
  AVG(f.amount) OVER (
    PARTITION BY f.orig_user_id
    ORDER BY f.step
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS avg_amount_3_tx,
  -- Cumul du volume financier par utilisateur
  SUM(f.amount) OVER (
    PARTITION BY f.orig_user_id
    ORDER BY f.step
  ) AS cumulative_amount
FROM `fintec-496811.fraud_detection.fact_transactions` f
JOIN `fintec-496811.fraud_detection.dim_users` orig ON f.orig_user_id = orig.user_id
JOIN `fintec-496811.fraud_detection.dim_users` dest ON f.dest_user_id = dest.user_id;
