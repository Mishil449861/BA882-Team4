# ingest.py

import os
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential
import argparse

# --------------- CONFIG ---------------
ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY")
GCS_BUCKET = os.environ.get("BUCKET_NAME")
COUNTRY = "us"
# --------------------------------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential())
def fetch_jobs(page=1, per_page=50):
    """Fetch job listings from Adzuna API."""
    url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": per_page,
        "content-type": "application/json",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("results", [])

def transform(records):
    """Transform raw records into structured DataFrames for each table."""
    jobs_data, companies_data, locations_data, categories_data, jobstats_data = [], [], [], [], []
    now_ts = datetime.now(timezone.utc).isoformat()
    today_str = datetime.utcnow().date().isoformat()

    for r in records:
        job_id = str(r.get("id"))
        title = r.get("title")
        description = r.get("description")
        created = r.get("created")
        salary_min = r.get("salary_min")
        salary_max = r.get("salary_max")
        redirect_url = r.get("redirect_url")

        # ------------------- JOBS -------------------
        jobs_data.append({
            "job_id": job_id,
            "title": title,
            "description": description,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "created": created,
            "redirect_url": redirect_url,
            "ingest_date": today_str,
            "ingest_ts": now_ts
        })

        # ------------------- COMPANIES -------------------
        company_name = r.get("company", {}).get("display_name")
        companies_data.append({
            "job_id": job_id,
            "company_name": company_name
        })

        # ------------------- LOCATIONS (fixed) -------------------
        loc = r.get("location", {})
        area = loc.get("area") or []

        country = "US"
        state = None
        city = None
        if len(area) == 3:
            country, state, city = area[0], area[1], area[2]
        elif len(area) == 2:
            country, state = area[0], area[1]
        elif len(area) == 1:
            country = area[0]

        locations_data.append({
            "job_id": job_id,
            "city": city,
            "state": state,
            "country": country
        })

        # ------------------- CATEGORIES -------------------
        category_label = r.get("category", {}).get("label")
        categories_data.append({
            "job_id": job_id,
            "category_label": category_label
        })

        # ------------------- JOBSTATS -------------------
        contract_type = r.get("contract_type")
        contract_time = r.get("contract_time")
        posting_week = pd.to_datetime(created).isocalendar().week if created else None
        jobstats_data.append({
            "job_id": job_id,
            "contract_type": contract_type,
            "contract_time": contract_time,
            "posting_week": posting_week
        })

    return (
        pd.DataFrame(jobs_data),
        pd.DataFrame(companies_data),
        pd.DataFrame(locations_data),
        pd.DataFrame(categories_data),
        pd.DataFrame(jobstats_data),
    )

def upload_to_gcs(df: pd.DataFrame, prefix: str):
    """Upload a DataFrame to GCS as Parquet, if not empty."""
    if df.empty:
        print(f"⚠️ Skipping upload for {prefix} — empty DataFrame")
        return

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    file_name = f"processed/{prefix}/{prefix}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.parquet"
    tmp_file = f"/tmp/{prefix}.parquet"
    df.to_parquet(tmp_file, index=False)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(tmp_file)

    print(f"✅ Uploaded {prefix} to gs://{GCS_BUCKET}/{file_name}")

def main(pages: int, per_page: int):
    all_records = []
    for page in range(1, pages + 1):
        all_records.extend(fetch_jobs(page, per_page))

    jobs_df, companies_df, locations_df, categories_df, jobstats_df = transform(all_records)

    upload_to_gcs(jobs_df, "jobs")
    upload_to_gcs(companies_df, "companies")
    upload_to_gcs(locations_df, "locations")
    upload_to_gcs(categories_df, "categories")
    upload_to_gcs(jobstats_df, "jobstats")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--per_page", type=int, default=50)
    args = parser.parse_args()
    main(args.pages, args.per_page)
