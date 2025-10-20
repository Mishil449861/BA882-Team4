import os
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage
import tempfile
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Constants
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
GCS_BUCKET = os.getenv("BUCKET_NAME")
PROCESSED_PREFIX = "processed"

BASE_URL = "https://api.adzuna.com/v1/api/jobs/us/search"

def fetch_data(pages=2, per_page=50):
    """Fetch job data from Adzuna API."""
    all_results = []
    for page in range(1, pages + 1):
        params = {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "results_per_page": per_page,
            "page": page,
            "content-type": "application/json",
        }
        logger.info(f"Fetching page {page}")
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            all_results.extend(response.json().get("results", []))
        else:
            logger.error(f"Failed to fetch page {page}: {response.text}")
    return all_results


def transform(records):
    """Transform raw Adzuna data into structured tables."""

    jobs_rows, companies_rows, locations_rows, categories_rows, jobstats_rows = [], [], [], [], []

    for r in records:
        jid = r.get("id")
        title = r.get("title")
        desc = r.get("description")
        created = r.get("created")
        company = r.get("company", {}).get("display_name")
        category = r.get("category", {}).get("label")
        salary_min = r.get("salary_min")
        salary_max = r.get("salary_max")

        # Jobs
        jobs_rows.append({
            "job_id": jid,
            "title": title,
            "description": desc,
            "company": company,
            "category": category,
            "created": created,
            "salary_min": salary_min,
            "salary_max": salary_max
        })

        # Companies
        companies_rows.append({
            "job_id": jid,
            "company": company
        })

        # LOCATIONS (city & state only)
        loc = r.get("location") or {}
        area = loc.get("area") if isinstance(loc.get("area"), list) else []

        city, state = None, None
        if isinstance(area, list):
            if len(area) >= 3:
                state = area[1]
                city = area[2]
            elif len(area) == 2:
                state = area[1]
            elif len(area) == 1:
                state = area[0]

        if not state and loc.get("display_name"):
            state = loc.get("display_name")

        locations_rows.append({
            "job_id": jid,
            "city": str(city) if city is not None else None,
            "state": str(state) if state is not None else None
        })

        # Categories
        categories_rows.append({
            "job_id": jid,
            "category": category
        })

        # Job Stats
        jobstats_rows.append({
            "job_id": jid,
            "created": created,
            "posting_week": datetime.strptime(created, "%Y-%m-%dT%H:%M:%SZ").isocalendar().week
                if created else None
        })

    # Convert to DataFrames
    jobs_df = pd.DataFrame(jobs_rows)
    companies_df = pd.DataFrame(companies_rows)
    locations_df = pd.DataFrame(locations_rows)
    categories_df = pd.DataFrame(categories_rows)
    jobstats_df = pd.DataFrame(jobstats_rows)

    # Enforce string types for locations
    locations_df["city"] = locations_df["city"].astype(str)
    locations_df["state"] = locations_df["state"].astype(str)

    return jobs_df, companies_df, locations_df, categories_df, jobstats_df


def upload_to_gcs(df, prefix):
    """Upload DataFrame to GCS as Parquet."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    path = f"{PROCESSED_PREFIX}/{prefix}/{prefix}_{ts}.parquet"
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.close()

    df.to_parquet(tmp.name, index=False, engine="pyarrow")
    blob = bucket.blob(path)
    blob.upload_from_filename(tmp.name)
    gs_path = f"gs://{GCS_BUCKET}/{path}"
    logger.info("✅ Uploaded %s -> %s", prefix, gs_path)
    return gs_path


def main(pages=2, per_page=50):
    """Main ingestion entry point."""
    logger.info("🚀 Starting Adzuna ingestion pipeline")
    records = fetch_data(pages, per_page)
    jobs_df, companies_df, locations_df, categories_df, jobstats_df = transform(records)

    upload_to_gcs(jobs_df, "jobs")
    upload_to_gcs(companies_df, "companies")
    upload_to_gcs(locations_df, "locations")
    upload_to_gcs(categories_df, "categories")
    upload_to_gcs(jobstats_df, "jobstats")
    logger.info("🎉 Ingestion pipeline completed successfully")


if __name__ == "__main__":
    main()
