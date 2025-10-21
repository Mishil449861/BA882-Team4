import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

project_id = "ba882-team4-474802"

# --- Authentication ---
if "GCP_SERVICE_ACCOUNT" in st.secrets:
    key_info = st.secrets["GCP_SERVICE_ACCOUNT"]
    credentials = service_account.Credentials.from_service_account_info(dict(key_info))
else:
    key_path = "/home/jin1221/gcp/ba882-team4-474802-123e6d60061f.json"
    credentials = service_account.Credentials.from_service_account_file(key_path)

client = bigquery.Client(credentials=credentials, project=project_id)

# --- Cached Queries ---
@st.cache_data
def get_company_list():
    """Fetch distinct company names for dropdown."""
    query = """
        SELECT DISTINCT row.company_name AS company
        FROM `ba882-team4-474802.ba882_jobs.jobs`
        WHERE row.company_name IS NOT NULL
        ORDER BY company
    """
    return client.query(query).to_dataframe()


@st.cache_data
def get_company_jobs(selected_company):
    """Fetch jobs for a selected company."""
    query = """
        SELECT
            row.company_name AS company,
            row.title AS job_title,
            row.location AS location,
            row.category AS category,
            row.salary_min AS min_salary,
            row.salary_max AS max_salary,
            row.created AS date_posted
        FROM `ba882-team4-474802.ba882_jobs.jobs`
        WHERE row.company_name = @company
        ORDER BY row.created DESC
        LIMIT 50
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("company", "STRING", selected_company)
        ]
    )
    return client.query(query, job_config).to_dataframe()


@st.cache_data
def get_company_insights():
    """Fetch top companies by job count and average salaries."""
    query = """
        SELECT
            row.company_name AS company,
            COUNT(*) AS job_count,
            AVG(row.salary_min) AS avg_min_salary,
            AVG(row.salary_max) AS avg_max_salary
        FROM `ba882-team4-474802.ba882_jobs.jobs`
        WHERE row.company_name IS NOT NULL
        GROUP BY company
        ORDER BY job_count DESC
        LIMIT 15
    """
    return client.query(query).to_dataframe()


# --- UI ---
st.title("🏢 Company Insights")

# Dropdown for company selection
company_df = get_company_list()
company_names = company_df["company"].tolist()

selected_company = st.selectbox("Select a company to view its jobs:", company_names)

if selected_company:
    st.subheader(f"Job listings for **{selected_company}**")
    df_jobs = get_company_jobs(selected_company)

    if df_jobs.empty:
        st.warning("No job listings found for this company.")
    else:
        st.dataframe(df_jobs)

# --- Insights Table ---
st.markdown("---")
st.subheader("Top Companies by Job Count")

df = get_company_insights()
if df.empty:
    st.warning("No data available for company insights.")
else:
    st.dataframe(df)
