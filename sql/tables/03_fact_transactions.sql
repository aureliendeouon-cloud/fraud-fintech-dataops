CREATE OR REPLACE TABLE `fintec-496811.fraud_detection.fact_transactions`
PARTITION BY
  RANGE_BUCKET(step, GENERATE_ARRAY(0, 800, 100))
CLUSTER BY
  type, isFraud
AS
SELECT
  t.step,
  t.type,
  t.amount,
  orig.user_id AS orig_user_id,
  dest.user_id AS dest_user_id,
  t.oldbalanceOrg,
  t.newbalanceOrig,
  t.oldbalanceDest,
  t.newbalanceDest,
  t.isFraud,
  t.isFlaggedFraud
FROM
  `fintec-496811.fraud_detection.raw_transactions` t
JOIN
  `fintec-496811.fraud_detection.dim_users` orig ON t.nameOrig = orig.user_name
JOIN
  `fintec-496811.fraud_detection.dim_users` dest ON t.nameDest = dest.user_name;
