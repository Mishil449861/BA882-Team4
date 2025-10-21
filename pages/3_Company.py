import streamlit as st
import pandas as pd
import altair as alt
from gcp_utils import get_bq_client

client = get_bq_client()
st.title("🏢 Company Insights Dashboard")

@st.cache_data
def get_company_insights():
    query = """
        SELECT
            c.company_name AS company_name,
            COUNT(j.row.job_id) AS job_count,
            ROUND(AVG((j.row.salary_min + j.row.salary_max)/2), 0) AS avg_salary,
            MAX(j.row.created) AS last_posted
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        JOIN `ba882-team4-474802.ba882_jobs.companies` c
          ON j.row.job_id = c.job_id
        WHERE c.company_name IS NOT NULL
        GROUP BY company_name
        ORDER BY job_count DESC
        LIMIT 15
    """
    return client.query(query).to_dataframe()

df = get_company_insights()

if df.empty:
    st.warning("No data available for company insights.")
else:
    st.subheader("🏆 Top Companies by Job Count")
    chart = alt.Chart(df).mark_bar(color="#4B9CD3").encode(
        x=alt.X("company_name:N", sort='-y', title="Company"),
        y=alt.Y("job_count:Q", title="Number of Job Postings"),
        tooltip=["company_name", "job_count", "avg_salary", "last_posted"]
    ).properties(height=400)
    st.altair_chart(chart, use_container_width=True)

    st.subheader("💰 Salary Insights")
    col1, col2, col3 = st.columns(3)
    top_salary = df.sort_values("avg_salary", ascending=False).iloc[0]
    col1.metric("Highest Paying Company", top_salary["company_name"], f"${top_salary['avg_salary']:,}")

    avg_overall = int(df["avg_salary"].mean())
    col2.metric("Average Salary Across Companies", f"${avg_overall:,}")

    latest_post = df["last_posted"].max()
    col3.metric("Most Recent Posting Date", str(latest_post)[:10])
