import streamlit as st
import pandas as pd
from gcp_utils import get_bq_client

client = get_bq_client()
st.title("🏢 Company-wise Hiring Dashboard")

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

@st.cache_data
def get_company_metrics(company):
    query = """
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
        query_parameters=[bigquery.ScalarQueryParameter("company", "STRING", company)]
    )
    return client.query(query, job_config=job_config).to_dataframe()

company_df = get_company_metrics(selected_company)
total_postings = len(company_df)
avg_salary = round(((company_df['salary_min'] + company_df['salary_max']) / 2).mean())

col1, col2 = st.columns(2)
col1.metric("📌 Total Postings", f"{total_postings:,}")
col2.metric("💰 Avg. Salary", f"${avg_salary:,}")

st.subheader(f"📋 Job Titles at: {selected_company}")
st.dataframe(company_df[['title', 'category_label']])
