import streamlit as st
import pandas as pd
from gcp_utils import get_bq_client

# ✅ BigQuery client 
client = get_bq_client()

st.set_page_config(page_title="Overview", page_icon="📊", layout="wide")
st.title("📊 Overview Dashboard")

@st.cache_data
def get_overview_metrics():
    query = """
        WITH combined AS (
          SELECT
            j.job_id,
            c.company_name,
            cat.category_label,
            j.salary_min,
            j.salary_max
          FROM `ba882-team4-474802.ba882_jobs.jobs` j
          JOIN `ba882-team4-474802.ba882_jobs.companies` c
            ON j.job_id = c.job_id
          JOIN `ba882-team4-474802.ba882_jobs.categories` cat
            ON j.job_id = cat.job_id
        )

        SELECT
          COUNT(DISTINCT job_id) AS total_jobs,
          COUNT(DISTINCT company_name) AS unique_companies,
          COUNT(DISTINCT category_label) AS total_categories,
          ROUND(AVG((salary_min + salary_max) / 2), 0) AS avg_salary,
          (SELECT category_label
           FROM combined
           GROUP BY category_label
           ORDER BY COUNT(*) DESC
           LIMIT 1) AS most_common_category
        FROM combined
    """
    return client.query(query).to_dataframe()

# Load and display
metrics_df = get_overview_metrics()
total_jobs = int(metrics_df["total_jobs"][0])
unique_companies = int(metrics_df["unique_companies"][0])
total_categories = int(metrics_df["total_categories"][0])
avg_salary = int(metrics_df["avg_salary"][0])
popular_category = metrics_df["most_common_category"][0]

col1, col2, col3 = st.columns(3)
col1.metric("📌 Total Jobs Posted", f"{total_jobs:,}")
col2.metric("🏢 Unique Companies Hiring", f"{unique_companies:,}")
col3.metric("🗂️ Categories", f"{total_categories:,}")

col4, col5 = st.columns([1, 2])  # wider second column to avoid truncation
col4.metric("💰 Average Salary", f"${avg_salary:,}")
col5.markdown(
    f"#### 🔥 Most Popular Job Category\n<span style='font-size:19px'>{popular_category}</span>",
    unsafe_allow_html=True
)

# import streamlit as st
# import pandas as pd
# from gcp_utils import get_bq_client  # 

# # ✅ BigQuery client 
# client = get_bq_client()

# st.set_page_config(page_title="Overview", page_icon="📊", layout="wide")
# st.title("📊 Overview Dashboard")

# @st.cache_data
# def get_overview_metrics():
#     query = """
#         WITH combined AS (
#           SELECT
#             j.job_id,
#             c.company_name,
#             cat.category_label,
#             j.salary_min,
#             j.salary_max,
#             j.title
#           FROM `ba882-team4-474802.ba882_jobs.jobs` j
#           JOIN `ba882-team4-474802.ba882_jobs.companies` c
#             ON j.job_id = c.job_id
#           JOIN `ba882-team4-474802.ba882_jobs.categories` cat
#             ON j.job_id = cat.job_id
#         )

#         SELECT
#           COUNT(DISTINCT job_id) AS total_jobs,
#           COUNT(DISTINCT company_name) AS unique_companies,
#           COUNT(DISTINCT category_label) AS total_categories,
#           ROUND(AVG((salary_min + salary_max) / 2), 0) AS avg_salary,
#           (SELECT title
#            FROM combined
#            GROUP BY title
#            ORDER BY COUNT(*) DESC
#            LIMIT 1) AS most_common_title
#         FROM combined
#     """
#     df = client.query(query).to_dataframe()
#     return df

# # Load and display
# metrics_df = get_overview_metrics()
# total_jobs = int(metrics_df["total_jobs"][0])
# unique_companies = int(metrics_df["unique_companies"][0])
# total_categories = int(metrics_df["total_categories"][0])
# avg_salary = int(metrics_df["avg_salary"][0])
# popular_title = metrics_df["most_common_title"][0]

# col1, col2, col3 = st.columns(3)
# col1.metric("📌 Total Jobs Posted", f"{total_jobs:,}")
# col2.metric("🏢 Unique Companies Hiring", f"{unique_companies:,}")
# col3.metric("🗂️ Categories", f"{total_categories:,}")

# col4, col5 = st.columns(2)
# col4.metric("💰 Average Salary", f"${avg_salary:,}")
# col5.metric("🔥 Most Popular Job Title", popular_title)


