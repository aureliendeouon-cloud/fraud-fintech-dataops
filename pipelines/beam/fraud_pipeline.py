import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

# --- CONFIGURATION GCP ---
PROJECT_ID    = "fintec-496811"
REGION        = "europe-west1"
BUCKET_NAME   = "fintec-496811-bucket"
TARGET_TABLE  = "fintec-496811:fraud_detection.raw_transactions_partitioned_clustered_v2"
INPUT_PATH    = f"gs://{BUCKET_NAME}/incoming_raw_transactions/*.json"

# --- TRANSFORMATIONS (DoFn) ---

class ParseAndClean(beam.DoFn):
    """Parse une ligne JSON, nettoie les valeurs nulles, filtre les lignes invalides."""

    def process(self, element):
        try:
            data = json.loads(element)

            step         = int(data.get("step", -1))
            tx_type      = str(data.get("type", "UNKNOWN")).upper()
            amount       = float(data.get("amount", 0.0))
            name_orig    = str(data.get("nameOrig", "MISSING"))
            name_dest    = str(data.get("nameDest",  "MISSING"))
            old_bal_orig = float(data.get("oldbalanceOrg",  0.0))
            new_bal_orig = float(data.get("newbalanceOrig", 0.0))
            old_bal_dest = float(data.get("oldbalanceDest", 0.0))
            new_bal_dest = float(data.get("newbalanceDest", 0.0))
            is_fraud     = int(data.get("isFraud",        0))
            is_flagged   = int(data.get("isFlaggedFraud", 0))

            if step != -1 and name_orig != "MISSING":
                yield {
                "step":            step,
                "type":            tx_type,
                "amount":          amount,
                "nameOrig":        name_orig,
                "oldbalanceOrg":   old_bal_orig,
                "newbalanceOrig":  new_bal_orig,
                "nameDest":        name_dest,
                "oldbalanceDest":  old_bal_dest,
                "newbalanceDest":  new_bal_dest,
                "isFraud":         is_fraud,
                "isFlaggedFraud":  is_flagged,
            }
        except Exception:
            pass  # en production : écrire dans un bucket d'anomalies (DLQ)


class FilterHighRisk(beam.DoFn):
    """Isole les transactions à fort montant sur types à risque."""

    HIGH_RISK_TYPES = {"TRANSFER", "CASH_OUT"}
    THRESHOLD       = 100_000.0

    def process(self, element):
        if element["type"] in self.HIGH_RISK_TYPES and element["amount"] > self.THRESHOLD:
            yield element


# --- PIPELINE ---

def run(mode: str = "local"):
    options = PipelineOptions()

    if mode == "production":
        gcp = options.view_as(GoogleCloudOptions)
        gcp.project          = PROJECT_ID
        gcp.region           = REGION
        gcp.job_name         = "fraud-detection-ingestion"
        gcp.staging_location = f"gs://{BUCKET_NAME}/staging"
        gcp.temp_location    = f"gs://{BUCKET_NAME}/temp"
        runner = "DataflowRunner"
    else:
        runner = "DirectRunner"

    # Données simulées pour le mode local
    mock_data = [
        '{"step":120,"type":"TRANSFER","amount":540000.0,"nameOrig":"C1001","nameDest":"C2001","oldbalanceOrg":600000.0,"newbalanceOrig":60000.0,"oldbalanceDest":0.0,"newbalanceDest":540000.0,"isFraud":1,"isFlaggedFraud":0}',
        '{"step":120,"type":"PAYMENT","amount":15.40,"nameOrig":"C1002","nameDest":"M1001","oldbalanceOrg":500.0,"newbalanceOrig":484.6,"oldbalanceDest":0.0,"newbalanceDest":0.0,"isFraud":0,"isFlaggedFraud":0}',
        '{"step":121,"type":"CASH_OUT","amount":850000.0,"nameOrig":"C1003","nameDest":"C2003","oldbalanceOrg":900000.0,"newbalanceOrig":50000.0,"oldbalanceDest":0.0,"newbalanceDest":850000.0,"isFraud":1,"isFlaggedFraud":0}',
        '{"step":-1,"type":"TRANSFER","amount":100.0,"nameOrig":"MISSING","nameDest":"C9999","oldbalanceOrg":0.0,"newbalanceOrig":0.0,"oldbalanceDest":0.0,"newbalanceDest":0.0,"isFraud":0,"isFlaggedFraud":0}',
    ]

    with beam.Pipeline(runner=runner, options=options) as pipeline:

        if mode == "production":
            source = pipeline | "Lire GCS" >> beam.io.ReadFromText(INPUT_PATH)
        else:
            source = pipeline | "Données mock" >> beam.Create(mock_data)

        cleaned = source | "Parser & Nettoyer" >> beam.ParDo(ParseAndClean())

        high_risk = cleaned | "Filtrer haut risque" >> beam.ParDo(FilterHighRisk())

        if mode == "production":
            high_risk | "Écrire BQ" >> beam.io.WriteToBigQuery(
                table=TARGET_TABLE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        else:
            high_risk | "Afficher alertes" >> beam.Map(print)


if __name__ == "__main__":
    run(mode="local")
