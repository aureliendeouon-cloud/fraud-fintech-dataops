CREATE OR REPLACE TABLE `fintec-496811.fraud_detection.dim_users`
CLUSTER BY user_id
AS
SELECT
  ROW_NUMBER() OVER() AS user_id,
  user_name,
  MIN(min_step) AS first_active_step,
  MAX(max_step)  AS last_active_step
FROM (
  SELECT nameOrig AS user_name, MIN(step) AS min_step, MAX(step) AS max_step
  FROM `fintec-496811.fraud_detection.raw_transactions`
  GROUP BY 1

  UNION DISTINCT

  SELECT nameDest AS user_name, MIN(step) AS min_step, MAX(step) AS max_step
  FROM `fintec-496811.fraud_detection.raw_transactions`
  GROUP BY 1
)
GROUP BY user_name;
