from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# --- PARAMÈTRES GCP ---
PROJECT_ID   = "fintec-496811"
REGION       = "europe-west1"
BUCKET_NAME  = "fintec-496811-bucket"
DATASET      = "fraud_detection"

DATAFLOW_TEMPLATE = f"gs://{BUCKET_NAME}/templates/fraud_detection_template"

# Appel de la procédure stockée sur le step du jour
BIGQUERY_CALL_PROCEDURE = """
CALL `fintec-496811.fraud_detection.UPDATE_DAILY_FEATURES`(
  CAST(FORMAT_DATE('%j', CURRENT_DATE()) AS INT64)
);
"""

# --- CONFIGURATION DU DAG ---
default_args = {
    "owner":           "mlops_team",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id="prod_fraud_detection_pipeline",
    default_args=default_args,
    description="Ingestion Dataflow + Feature Engineering BigQuery (fraude)",
    schedule="@daily",
    start_date=datetime(2026, 5, 20),
    catchup=False,
    tags=["prod", "gcp", "mlops", "fraud"],
) as dag:

    # Tâche 1 : Ingestion via Dataflow (Apache Beam)
    trigger_dataflow = DataflowTemplatedJobStartOperator(
        task_id="gcp_dataflow_ingestion",
        template=DATAFLOW_TEMPLATE,
        job_name="dataflow-fraud-ingestion",
        location=REGION,
        project_id=PROJECT_ID,
        parameters={
            "input_gcs_path": f"gs://{BUCKET_NAME}/incoming_raw_transactions/*.json",
        },
    )

    # Tâche 2 : Calcul des features via procédure stockée BQ
    run_bq_features = BigQueryInsertJobOperator(
        task_id="gcp_bigquery_feature_engineering",
        configuration={
            "query": {
                "query": BIGQUERY_CALL_PROCEDURE,
                "useLegacySql": False,
            }
        },
        location="EU",
        project_id=PROJECT_ID,
    )

    # Chaînage : Dataflow doit réussir avant BQ
    trigger_dataflow >> run_bq_features
