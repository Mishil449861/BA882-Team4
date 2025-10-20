import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import altair as alt

# 인증 설정
project_id = "ba882-team4-474802"
key_path = "/home/jin1221/gcp/ba882-team4-474802-123e6d60061f.json"
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=project_id)

st.title("📈 Weekly Job Posting Trends")

# 캐시된 쿼리 함수
@st.cache_data
def get_weekly_trends():
    query = """
        SELECT
            FORMAT_DATE('%Y-%W', DATE(created)) AS week,
            COUNT(*) AS job_count
        FROM `ba882-team4-474802.ba882_jobs.jobs`
        WHERE created IS NOT NULL
        GROUP BY week
        ORDER BY week
    """
    df = client.query(query).to_dataframe()
    return df

df = get_weekly_trends()

# 라인 차트로 표시
st.subheader("🗓️ Weekly Job Postings")
chart = (
    alt.Chart(df)
    .mark_line(point=True)
    .encode(
        x=alt.X("week:T", title="Week"),
        y=alt.Y("job_count:Q", title="Number of Job Postings"),
        tooltip=["week", "job_count"]
    )
    .properties(width=700, height=400)
)
st.altair_chart(chart, use_container_width=True)

# 테이블로도 보기
with st.expander("📄 Show Raw Weekly Data"):
    st.dataframe(df)