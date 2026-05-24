CREATE OR REPLACE FUNCTION `fintec-496811.fraud_detection.GET_TEMPORAL_SCORE`(
  steps_since_last_tx INT64,
  tx_type             STRING,
  amount              FLOAT64
)
AS (
  CASE
    WHEN steps_since_last_tx = 0  AND tx_type IN ('TRANSFER', 'CASH_OUT') THEN 0.95
    WHEN steps_since_last_tx = 0  AND amount > 5000                        THEN 0.80
    WHEN steps_since_last_tx = 0                                           THEN 0.30
    WHEN steps_since_last_tx = -1                                          THEN 0.10
    ELSE 0.0
  END
);
