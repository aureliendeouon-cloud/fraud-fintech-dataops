CREATE OR REPLACE PROCEDURE `fintec-496811.fraud_detection.UPDATE_DAILY_FEATURES`(target_step INT64)
BEGIN
  DECLARE min_bucket INT64;
  DECLARE max_bucket INT64;

  SET min_bucket = target_step - 10;
  SET max_bucket = target_step;

  CREATE OR REPLACE TEMP TABLE tmp_features AS
  SELECT
    nameOrig,
    step,
    type,
    amount,
    `fintec-496811.fraud_detection.GET_TEMPORAL_SCORE`(
      IFNULL(step - LAG(step, 1) OVER (PARTITION BY nameOrig ORDER BY step), -1),
      type,
      amount
    ) AS calculated_score
  FROM `fintec-496811.fraud_detection.raw_transactions_partitioned_clustered_v2`
  WHERE step BETWEEN min_bucket AND max_bucket;

  INSERT INTO `fintec-496811.fraud_detection.model_features_output`
  SELECT * FROM tmp_features
  WHERE step = target_step;
END;
