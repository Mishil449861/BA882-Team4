import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# 인증 정보
project_id = "ba882-team4-474802"
key_path = "/home/jin1221/gcp/ba882-team4-474802-123e6d60061f.json"
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=project_id)

st.title("🏢 Company-wise Hiring Dashboard")

# 전체 회사 리스트 불러오기
@st.cache_data
def get_company_list():
    query = """
        SELECT DISTINCT company_name
        FROM `ba882-team4-474802.ba882_jobs.companies`
        WHERE company_name IS NOT NULL
        ORDER BY company_name
    """
    df = client.query(query).to_dataframe()
    return df["company_name"].tolist()

company_list = get_company_list()
selected_company = st.selectbox("Select a company", company_list)

# 선택된 회사 기준 데이터 불러오기
@st.cache_data
def get_company_metrics(company):
    query = f"""
        SELECT
            j.title,
            c.company_name,
            cat.category_label,
            j.salary_min,
            j.salary_max
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        JOIN `ba882-team4-474802.ba882_jobs.companies` c ON j.job_id = c.job_id
        JOIN `ba882-team4-474802.ba882_jobs.categories` cat ON j.job_id = cat.job_id
        WHERE c.company_name = @company
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("company", "STRING", company)
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    return df

company_df = get_company_metrics(selected_company)

# KPI 표시
total_postings = len(company_df)
avg_salary = round(((company_df['salary_min'] + company_df['salary_max']) / 2).mean())

col1, col2 = st.columns(2)
col1.metric("📌 Total Postings", f"{total_postings:,}")
col2.metric("💰 Avg. Salary", f"${avg_salary:,}")

# 테이블 표시
st.subheader(f"📋 Job Titles at: {selected_company}")
st.dataframe(company_df[['title', 'category_label']])