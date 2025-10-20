import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account

def get_bq_client():
    key_json_str = st.secrets["GCP_SERVICE_ACCOUNT"]
    key_info = json.loads(key_json_str)
    credentials = service_account.Credentials.from_service_account_info(key_info)
    return bigquery.Client(credentials=credentials, project=key_info["project_id"])


# from google.cloud import bigquery
# from google.oauth2 import service_account

# # Set project and key file path
# project_id = "ba882-team4-474802"
# key_path = "/home/jin1221/gcp/ba882-team4-474802-123e6d60061f.json"

# # Load credentials and create BigQuery client
# credentials = service_account.Credentials.from_service_account_file(key_path)
# bq_client = bigquery.Client(project=project_id, credentials=credentials)
