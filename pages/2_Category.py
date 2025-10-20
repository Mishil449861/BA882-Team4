import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import altair as alt

# ✅ Secure BigQuery authentication
def get_bq_client():
    project_id = "ba882-team4-474802"

    # Streamlit Cloud: use secrets
    if "GCP_SERVICE_ACCOUNT" in st.secrets:
        key_info = st.secrets["GCP_SERVICE_ACCOUNT"]
        credentials = service_account.Credentials.from_service_account_info(dict(key_info))
    else:
        # Local fallback (for your computer)
        key_path = "/home/jin1221/gcp/ba882-team4-474802-123e6d60061f.json"
        credentials = service_account.Credentials.from_service_account_file(key_path)

    return bigquery.Client(credentials=credentials, project=project_id)

# ✅ Create client
client = get_bq_client()

# 🌟 Streamlit layout
st.title("📂 Job Categories Dashboard")

# 📌 카테고리별 공고 수 쿼리
@st.cache_data
def load_category_data():
    query = """
        SELECT
            c.category_label,
            COUNT(*) AS job_count
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        JOIN `ba882-team4-474802.ba882_jobs.categories` c
        ON j.job_id = c.job_id
        GROUP BY c.category_label
        ORDER BY job_count DESC
    """
    return client.query(query).to_dataframe()

# 📌 선택한 카테고리 내 직무 리스트
@st.cache_data
def load_jobs_by_category(category):
    query = f"""
        SELECT 
            j.title, 
            c.company_name, 
            j.salary_min, 
            j.salary_max
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        JOIN `ba882-team4-474802.ba882_jobs.categories` cat
            ON j.job_id = cat.job_id
        JOIN `ba882-team4-474802.ba882_jobs.companies` c
            ON j.job_id = c.job_id
        WHERE cat.category_label = '{category}'
        LIMIT 100
    """
    return client.query(query).to_dataframe()

# 📊 막대 차트용 데이터 불러오기
df_cat = load_category_data()
top5_df = df_cat.sort_values(by="job_count", ascending=False).head(5)

# 📊 막대 차트 (Altair로)
st.subheader("📊 Top 5 Categories by Job Count")

bar_chart = (
    alt.Chart(top5_df)
    .mark_bar(color="#66b3ff")
    .encode(
        x=alt.X("category_label:N", sort="-y", axis=alt.Axis(labelAngle=-25, labelFontSize=12)),
        y=alt.Y("job_count:Q"),
        tooltip=["category_label", "job_count"]
    )
    .properties(width=700, height=400)
)

st.altair_chart(bar_chart, use_container_width=True)

# 🧭 카테고리 선택
st.sidebar.subheader("🔎 Select a Category")
selected_category = st.sidebar.selectbox("Choose category", df_cat["category_label"])

# 📋 선택된 카테고리의 직무 리스트 표시
if selected_category:
    jobs_df = load_jobs_by_category(selected_category)
    st.subheader(f"🗂️ Jobs under: {selected_category}")
    st.dataframe(jobs_df)
