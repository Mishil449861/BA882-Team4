import os
import json
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# Set project and key file path
project_id = "ba882-team4-474802"
key_path = "/home/jin1221/gcp/ba882-team4-474802-123e6d60061f.json"

# Step 1: Load credentials from file
credentials = service_account.Credentials.from_service_account_file(key_path)

# Step 2: Initialize BigQuery client
bq_client = bigquery.Client(project=project_id, credentials=credentials)

# ✅ Test query
@st.cache_data
def run_sample_query():
    query = "SELECT job_id, title FROM `ba882-team4-474802.ba882_jobs.jobs` LIMIT 10"
    return bq_client.query(query).to_dataframe()

# Streamlit UI
st.title("✅ Streamlit + BigQuery 연결 (로컬 키 파일 사용)")
df = run_sample_query()
st.dataframe(df)