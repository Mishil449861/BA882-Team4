from google.cloud import bigquery
from google.oauth2 import service_account

# Set project and key file path
project_id = "ba882-team4-474802"
key_path = "/home/jin1221/gcp/ba882-team4-474802-123e6d60061f.json"

# Load credentials and create BigQuery client
credentials = service_account.Credentials.from_service_account_file(key_path)
bq_client = bigquery.Client(project=project_id, credentials=credentials)