CREATE OR REPLACE TABLE `fintec-496811.fraud_detection.raw_transactions_partitioned_clustered_v2`
PARTITION BY
  RANGE_BUCKET(step, GENERATE_ARRAY(0, 800, 100))
CLUSTER BY
  type, isFraud
AS
SELECT *
FROM `fintec-496811.fraud_detection.raw_transactions`;
