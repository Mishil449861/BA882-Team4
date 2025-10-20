import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account

def get_bq_client():
    # Streamlit Secrets 환경 변수에서 서비스 계정 키 가져오기
    key_json_str = os.environ["GCP_SERVICE_ACCOUNT"]
    key_info = json.loads(key_json_str)

    # 서비스 계정 정보를 기반으로 인증 객체 생성
    credentials = service_account.Credentials.from_service_account_info(key_info)
    project_id = key_info["project_id"]

    # BigQuery 클라이언트 생성
    return bigquery.Client(project=project_id, credentials=credentials)


# from google.cloud import bigquery
# from google.oauth2 import service_account

# # Set project and key file path
# project_id = "ba882-team4-474802"
# key_path = "/home/jin1221/gcp/ba882-team4-474802-123e6d60061f.json"

# # Load credentials and create BigQuery client
# credentials = service_account.Credentials.from_service_account_file(key_path)
# bq_client = bigquery.Client(project=project_id, credentials=credentials)