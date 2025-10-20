# ingest.py
# Purpose: Fetch Adzuna USA data jobs, transform into relational structure,
# return five DataFrames (jobs, companies, locations, categories, jobstats),
# and optionally upload processed Parquet files to GCS.

import os
import json
import hashlib
import tempfile
import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from google.cloud import storage

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Config (environment-driven)
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
GCS_BUCKET = os.getenv("BUCKET_NAME")
COUNTRY = os.getenv("ADZUNA_COUNTRY", "us")
PROCESSED_PREFIX = "processed"


# ----------------------------
# API / helper functions
# ----------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_jobs(page: int = 1, per_page: int = 50) -> List[Dict]:
    """Fetch one page of Adzuna job listings (returns list of records)."""
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        raise RuntimeError("ADZUNA_APP_ID and ADZUNA_APP_KEY must be set in environment.")

    url = f"https://api.adzuna.com/v1/api/jobs/{COUNTRY}/search/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": per_page,
        "content-type": "application/json",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def stable_job_id(record: Dict) -> str:
    """Return API id if present, else stable SHA256 hash of title|company|location."""
    if record.get("id"):
        return str(record["id"])
    key_fields = "|".join(str(record.get(k, "")).strip() for k in ("title", "company", "location"))
    return hashlib.sha256(key_fields.encode("utf-8")).hexdigest()


# ----------------------------
# Transform -> returns 5 DataFrames
# ----------------------------
def transform(records: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Transform raw Adzuna records into 5 DataFrames:
      1) jobs (job_id, title, description, salary_min, salary_max, created, redirect_url, ingest_date, ingest_ts)
      2) companies (job_id, company_name)
      3) locations (job_id, city, state, country)
      4) categories (job_id, category_label)
      5) jobstats (job_id, contract_type, contract_time, posting_week)

    Returns (jobs_df, companies_df, locations_df, categories_df, jobstats_df)
    """
    jobs_rows = []
    companies_rows = []
    locations_rows = []
    categories_rows = []
    jobstats_rows = []

    ts = datetime.now(timezone.utc).isoformat()
    ingest_date = datetime.utcnow().date().isoformat()

    for r in records:
        jid = stable_job_id(r)
        # canonical fields
        title = r.get("title")
        description = r.get("description")
        salary_min = r.get("salary_min")
        salary_max = r.get("salary_max")
        created = r.get("created")
        redirect_url = r.get("redirect_url")

        # JOBS
        jobs_rows.append({
            "job_id": jid,
            "title": title,
            "description": description,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "created": created,
            "redirect_url": redirect_url,
            "ingest_ts": ts,
            "ingest_date": ingest_date
        })

        # COMPANIES
        comp = r.get("company") or {}
        companies_rows.append({
            "job_id": jid,
            "company_name": comp.get("display_name") if isinstance(comp, dict) else comp
        })

        # LOCATIONS - robust parsing of `location.area` if present
        loc = r.get("location") or {}
        area = loc.get("area") if isinstance(loc.get("area"), list) else []
        # Default None values (cast to strings later)
        city = None
        state = None
        country = None
        # Adzuna area often looks like: [ 'United States', 'California', 'Pleasant Hill' ] or similar
        if isinstance(area, list):
            if len(area) >= 3:
                country = area[0]
                state = area[1]
                # sometimes the 3rd contains comma+county; keep it whole
                city = area[2]
            elif len(area) == 2:
                country = area[0]
                state = area[1]
            elif len(area) == 1:
                country = area[0]

        # as a fallback, also check display_name
        if not country and loc.get("display_name"):
            country = loc.get("display_name")

        locations_rows.append({
            "job_id": jid,
            "city": str(city) if city is not None else None,
            "state": str(state) if state is not None else None,
            "country": str(country) if country is not None else None
        })

        # CATEGORIES
        cat = r.get("category") or {}
        categories_rows.append({
            "job_id": jid,
            "category_label": cat.get("label") if isinstance(cat, dict) else cat
        })

        # JOBSTATS
        contract_type = r.get("contract_type")
        contract_time = r.get("contract_time")
        posting_week = None
        if created:
            try:
                posting_week = int(pd.to_datetime(created).isocalendar().week)
            except Exception:
                posting_week = None

        jobstats_rows.append({
            "job_id": jid,
            "contract_type": contract_type,
            "contract_time": contract_time,
            "posting_week": posting_week
        })

    # Build DataFrames and enforce types
    jobs_df = pd.DataFrame(jobs_rows)
    companies_df = pd.DataFrame(companies_rows)
    locations_df = pd.DataFrame(locations_rows)
    categories_df = pd.DataFrame(categories_rows)
    jobstats_df = pd.DataFrame(jobstats_rows)

    # Ensure salary columns are numeric
    if "salary_min" in jobs_df.columns:
        jobs_df["salary_min"] = pd.to_numeric(jobs_df["salary_min"], errors="coerce")
    if "salary_max" in jobs_df.columns:
        jobs_df["salary_max"] = pd.to_numeric(jobs_df["salary_max"], errors="coerce")

    # Ensure location fields are strings (avoid accidental integer types)
    for col in ("city", "state", "country"):
        if col in locations_df.columns:
            locations_df[col] = locations_df[col].astype("string")  # pandas string dtype

    return jobs_df, companies_df, locations_df, categories_df, jobstats_df


# ----------------------------
# GCS upload helper (optional)
# ----------------------------
def upload_df_to_gcs(df: pd.DataFrame, prefix: str, bucket_name: Optional[str] = None) -> str:
    """Upload a DataFrame as parquet to GCS under processed/<prefix>/ and return gs:// path."""
    bucket = bucket_name or GCS_BUCKET
    if not bucket:
        raise RuntimeError("GCS bucket not configured via BUCKET_NAME environment variable.")
    if df.empty:
        logger.info("Skipping upload for %s: DataFrame is empty", prefix)
        return ""

    client = storage.Client()
    b = client.bucket(bucket)
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    path = f"{PROCESSED_PREFIX}/{prefix}/{prefix}_{ts}.parquet"
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.close()
    df.to_parquet(tmp.name, index=False)
    blob = b.blob(path)
    blob.upload_from_filename(tmp.name)
    gs_path = f"gs://{bucket}/{path}"
    logger.info("Uploaded %s -> %s", prefix, gs_path)
    return gs_path


# ----------------------------
# Runner (only when executed directly)
# ----------------------------
def run_ingestion(pages: int = 1, per_page: int = 50, upload: bool = False, bucket_override: Optional[str] = None):
    """Main runner: fetch pages, transform, (optionally) upload each table to GCS."""
    all_records = []
    for p in range(1, pages + 1):
        logger.info("Fetching page %d/%d", p, pages)
        page_records = fetch_jobs(page=p, per_page=per_page)
        if not page_records:
            logger.info("No records returned on page %d, stopping fetch loop.", p)
            break
        all_records.extend(page_records)

    jobs_df, companies_df, locations_df, categories_df, jobstats_df = transform(all_records)

    if upload:
        bucket = bucket_override or GCS_BUCKET
        upload_df_to_gcs(jobs_df, "jobs", bucket)
        upload_df_to_gcs(companies_df, "companies", bucket)
        upload_df_to_gcs(locations_df, "locations", bucket)
        upload_df_to_gcs(categories_df, "categories", bucket)
        upload_df_to_gcs(jobstats_df, "jobstats", bucket)

    return jobs_df, companies_df, locations_df, categories_df, jobstats_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Adzuna ingestion -> transform -> optional GCS upload")
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--per_page", type=int, default=50)
    parser.add_argument("--upload", action="store_true", help="Upload processed parquet files to GCS")
    parser.add_argument("--bucket", type=str, default=None, help="Override GCS bucket name (optional)")
    args = parser.parse_args()

    run_ingestion(pages=args.pages, per_page=args.per_page, upload=args.upload, bucket_override=args.bucket)
