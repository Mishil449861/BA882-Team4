import os
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage
import tempfile
import logging
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")
logger = logging.getLogger(__name__)

# Constants
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
GCS_BUCKET = os.getenv("BUCKET_NAME")
PROCESSED_PREFIX = "processed"

COUNTRY = "us"
RESULTS_PER_PAGE = 50
MAX_PAGES = 5
BASE_URL = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search"


def fetch_data(pages=MAX_PAGES, per_page=RESULTS_PER_PAGE):
    """Fetch job listings from Adzuna API for data, analytics, and research roles."""
    all_results = []
    for page in range(1, pages + 1):
        url = (
            f"{BASE_URL}/{page}"
            f"?app_id={ADZUNA_APP_ID}"
            f"&app_key={ADZUNA_APP_KEY}"
            f"&results_per_page={per_page}"
            f"&what=data"
        )
        logger.info(f"Fetching page {page}")
        response = requests.get(url)

        if response.status_code != 200:
            logger.error(f"Failed to fetch page {page}: {response.text[:300]}")
            continue

        try:
            results = response.json().get("results", [])
            all_results.extend(results)
            sleep(1)  # avoid rate limiting
        except Exception as e:
            logger.error(f"Error parsing page {page}: {e}")
            continue

    logger.info(f"Fetched {len(all_results)} total jobs")
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
        companies_rows.append({"job_id": jid, "company": company})

        # Locations
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
            "city": str(city) if city else None,
            "state": str(state) if state else None
        })

        # Categories
        categories_rows.append({"job_id": jid, "category": category})

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

    logger.info(
        f"DataFrame counts → Jobs: {len(jobs_df)} | Companies: {len(companies_df)} | "
        f"Locations: {len(locations_df)} | Categories: {len(categories_df)} | JobStats: {len(jobstats_df)}"
    )

    return jobs_df, companies_df, locations_df, categories_df, jobstats_df


def upload_to_gcs(df, prefix):
    """Upload DataFrame to GCS as Parquet (skips empty DataFrames)."""
    if df.empty:
        logger.warning(f"⚠️ Skipping upload for {prefix}: empty DataFrame")
        return None

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
    logger.info("✅ Uploaded %s → %s", prefix, gs_path)
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
