import streamlit as st
import pandas as pd
from google.cloud import bigquery
from gcp_utils import get_bq_client  # Function to load BigQuery client from Streamlit Secrets

# Set page configuration
st.set_page_config(page_title="Daily Trends", page_icon="📅", layout="wide")
st.title("📅 Daily Job Posting Trends")

# Initialize BigQuery client
client = get_bq_client()

# Cached function to fetch daily job posting counts
@st.cache_data(ttl=86400)
def fetch_daily_posting_trends():
    query = """
        SELECT
          DATE(posted_date) AS post_date,
          COUNT(*) AS job_count
        FROM `ba882-team4-474802.ba882_jobs.jobs`
        WHERE posted_date IS NOT NULL
        GROUP BY post_date
        ORDER BY post_date
    """
    df = client.query(query).to_dataframe()
    return df

# Load data from BigQuery
df = fetch_daily_posting_trends()

# Display line chart
if df.empty:
    st.warning("No posting data available.")
else:
    st.line_chart(
        df.set_index("post_date"),
        y="job_count",
        use_container_width=True
    )
    st.caption("Line chart showing number of job postings by day")
