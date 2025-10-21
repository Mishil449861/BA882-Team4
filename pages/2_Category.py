import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import altair as alt

# ✅ BigQuery authentication
def get_bq_client():
    project_id = "ba882-team4-474802"

    if "GCP_SERVICE_ACCOUNT" in st.secrets:
        key_info = st.secrets["GCP_SERVICE_ACCOUNT"]
        credentials = service_account.Credentials.from_service_account_info(dict(key_info))
    else:
        key_path = "/home/jin1221/gcp/ba882-team4-474802-123e6d60061f.json"
        credentials = service_account.Credentials.from_service_account_file(key_path)

    return bigquery.Client(credentials=credentials, project=project_id)


# ✅ Initialize client
client = get_bq_client()

# 🌟 Dashboard title
st.title("📂 Job Categories Dashboard")

# ========================================
# 📊 CATEGORY-WISE JOB COUNT
# ========================================
@st.cache_data
def load_category_data():
    query = """
        SELECT
            cat.category_label AS category_label,
            COUNT(DISTINCT j.row.job_id) AS job_count
        FROM `ba882-team4-474802.ba882_jobs.jobs` AS j
        JOIN `ba882-team4-474802.ba882_jobs.categories` AS cat
            ON j.row.job_id = cat.job_id
        GROUP BY cat.category_label
        ORDER BY job_count DESC
    """
    return client.query(query).to_dataframe()


# ========================================
# 📋 JOBS UNDER SELECTED CATEGORY
# ========================================
@st.cache_data
def load_jobs_by_category(category):
    query = """
        SELECT 
            j.row.title AS job_title,
            COALESCE(comp.company_name, 'N/A') AS company_name,
            j.row.salary_min,
            j.row.salary_max,
            j.row.created,
            j.row.redirect_url
        FROM `ba882-team4-474802.ba882_jobs.jobs` AS j
        JOIN `ba882-team4-474802.ba882_jobs.categories` AS cat
            ON j.row.job_id = cat.job_id
        LEFT JOIN `ba882-team4-474802.ba882_jobs.companies` AS comp
            ON j.row.job_id = comp.job_id
        WHERE cat.category_label = @category
        ORDER BY j.row.created DESC
        LIMIT 100
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("category", "STRING", category)
        ]
    )

    try:
        return client.query(query, job_config).to_dataframe()
    except Exception as e:
        st.error(f"❌ BigQuery Error: {e}")
        return pd.DataFrame()


# ========================================
# 📈 BAR CHART OF TOP 5 CATEGORIES
# ========================================
df_cat = load_category_data()
top5_df = df_cat.head(5)

st.subheader("📊 Top 5 Categories by Job Count")
bar_chart = (
    alt.Chart(top5_df)
    .mark_bar(color="#66b3ff")
    .encode(
        x=alt.X("category_label:N", sort="-y", axis=alt.Axis(labelAngle=-25)),
        y=alt.Y("job_count:Q"),
        tooltip=["category_label", "job_count"]
    )
    .properties(width=700, height=400)
)
st.altair_chart(bar_chart, use_container_width=True)

# ========================================
# 🧭 CATEGORY SELECTION & JOB DETAILS
# ========================================
st.sidebar.subheader("🔎 Select a Category")
selected_category = st.sidebar.selectbox("Choose category", df_cat["category_label"])

if selected_category:
    jobs_df = load_jobs_by_category(selected_category)
    st.subheader(f"🗂️ Jobs in {selected_category}")
    if not jobs_df.empty:
        st.dataframe(
            jobs_df[
                ["job_title", "company_name", "salary_min", "salary_max", "created", "redirect_url"]
            ]
        )
    else:
        st.info("No jobs found for this category.")
