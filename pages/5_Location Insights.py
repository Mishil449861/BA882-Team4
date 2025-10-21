import streamlit as st
import pandas as pd
import altair as alt
from gcp_utils import get_bq_client

client = get_bq_client()
st.title("🌎 State-Based Job Insights")

@st.cache_data
def get_state_insights(selected_category=None):
    filter_clause = f"AND cat.category_label = '{selected_category}'" if selected_category else ""
    query = f"""
        SELECT
            l.state,
            COUNT(j.job_id) AS job_count,
            ROUND(AVG((j.salary_min + j.salary_max)/2), 0) AS avg_salary
        FROM `ba882-team4-474802.ba882_jobs.jobs` j
        JOIN `ba882-team4-474802.ba882_jobs.locations` l ON j.job_id = l.job_id
        JOIN `ba882-team4-474802.ba882_jobs.categories` cat ON j.job_id = cat.job_id
        WHERE l.state IS NOT NULL
        {filter_clause}
        GROUP BY l.state
        ORDER BY job_count DESC
        LIMIT 10
    """
    return client.query(query).to_dataframe()

@st.cache_data
def get_all_categories():
    query = """
        SELECT DISTINCT category_label
        FROM `ba882-team4-474802.ba882_jobs.categories`
        ORDER BY category_label
    """
    return client.query(query).to_dataframe()["category_label"].tolist()

st.sidebar.subheader("🔎 Filter by Category")
categories = get_all_categories()
selected_category = st.sidebar.selectbox("Choose a category (optional):", ["All"] + categories)

df = get_state_insights(None if selected_category == "All" else selected_category)

if df.empty:
    st.warning("No data available for this category.")
else:
    st.subheader("🗺️ Top States by Job Count")
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X("state:N", sort='-y', title="State"),
        y=alt.Y("job_count:Q", title="Job Count"),
        tooltip=["state", "job_count", "avg_salary"]
    ).properties(height=400)
    st.altair_chart(chart, use_container_width=True)

    st.subheader("💰 Average Salary by State")
    col1, col2, col3 = st.columns(3)
    top_salary = df.sort_values("avg_salary", ascending=False).iloc[0]
    col1.metric("Highest Paying State", top_salary["state"], f"${top_salary['avg_salary']:,}")

    avg_overall = int(df["avg_salary"].mean())
    col2.metric("Average Across Top States", f"${avg_overall:,}")
    col3.metric("Category Filter", selected_category if selected_category != "All" else "None")
