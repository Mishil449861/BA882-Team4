import streamlit as st
import pandas as pd
from gcp_utils import get_bq_client

client = get_bq_client()

st.title("🔍 Job Title Explorer")
st.caption("Enter a keyword to search job titles (e.g. 'data', 'engineer', etc.)")

keyword = st.text_input("🔎 Search for job titles:")

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

if keyword:
    results_df = search_jobs_by_keyword(keyword)
    if not results_df.empty:
        st.markdown(f"#### Showing results for: **{keyword}**")
        display_cols = ["title", "company_name", "category_label", "city", "state", "country"]
        st.dataframe(results_df[display_cols])

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
