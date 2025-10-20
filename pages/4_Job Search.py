import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# GCP 인증 정보
project_id = "ba882-team4-474802"
key_path = "/home/jin1221/gcp/ba882-team4-474802-123e6d60061f.json"
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=project_id)

# 페이지 타이틀
st.title("🔍 Job Title Explorer")
st.caption("Enter a keyword to search job titles (e.g. 'data', 'engineer', etc.)")

# 사용자 입력
keyword = st.text_input("🔎 Search for job titles:")

# 쿼리 실행 함수
@st.cache_data
def search_jobs_by_keyword(kw):
    query = f"""
        SELECT
            j.title,
            c.company_name,
            cat.category_label,
            l.city,
            l.state,
            l.country,
            j.description
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        JOIN `ba882-team4-474802.ba882_jobs.companies` c ON j.job_id = c.job_id
        JOIN `ba882-team4-474802.ba882_jobs.categories` cat ON j.job_id = cat.job_id
        JOIN `ba882-team4-474802.ba882_jobs.locations` l ON j.job_id = l.job_id
        WHERE LOWER(j.title) LIKE '%{kw.lower()}%'
        LIMIT 100
    """
    return client.query(query).to_dataframe()

# 검색 수행
if keyword:
    results_df = search_jobs_by_keyword(keyword)

    if not results_df.empty:
        st.markdown(f"#### Showing results for: **{keyword}**")

        # 테이블 표시 (요약 정보)
        display_cols = ["title", "company_name", "category_label", "city", "state", "country"]
        st.dataframe(results_df[display_cols])

        # 사용자가 보고 싶은 직무 선택
        job_titles = results_df["title"] + " at " + results_df["company_name"]
        selected_job = st.selectbox("📌 Select a job to view full description:", job_titles)

        if selected_job:
            row = results_df[job_titles == selected_job].iloc[0]
            st.markdown("### 📝 Job Description")
            st.markdown(f"**Job Title**: {row['title']}")
            st.markdown(f"**Company**: {row['company_name']}")
            st.markdown(f"**Location**: {row['city']}, {row['state']}, {row['country']}")
            st.markdown(f"**Category**: {row['category_label']}")
            st.markdown("---")
            st.write(row["description"])
    else:
        st.warning("No jobs found for the given keyword.")