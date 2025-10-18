# ingest.py
# Purpose: Fetch Adzuna USA data jobs, transform into relational structure, deduplicate, upload processed Parquet to GCS.

import os
import json
import hashlib
import tempfile
import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from gcs_utils import upload_file, download_blob_to_file, blob_exists

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingest")

# --- Config ---
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
ADZUNA_COUNTRY = "us"

# Expanded multi-role search query for analytics, data & AI jobs
ADZUNA_QUERY = os.getenv(
    "ADZUNA_QUERY",
    (
        "data analyst OR data scientist OR data engineer OR machine learning engineer OR AI engineer OR "
        "data science OR data analysis OR analytics engineer OR business intelligence OR BI analyst OR "
        "quantitative analyst OR statistician OR research analyst OR marketing analyst OR financial analyst OR "
        "reporting analyst OR insights analyst OR operations analyst OR risk analyst OR fraud analyst OR "
        "product analyst OR customer insights OR growth analyst OR data visualization OR data governance OR "
        "data architect OR ML engineer OR deep learning engineer OR NLP engineer OR computer vision engineer OR "
        "ETL developer OR data warehouse engineer OR BI developer OR Tableau developer OR Power BI developer OR "
        "data quality analyst OR data steward OR data strategist OR data consultant OR decision scientist OR "
        "econometrician OR forecasting analyst OR revenue analyst OR supply chain analyst OR business analyst OR "
        "quantitative researcher OR AI researcher OR ML researcher OR applied scientist OR research scientist OR "
        "ML ops engineer OR data ops engineer OR analytics consultant OR business data analyst OR "
        "insights manager OR head of analytics OR director of data OR analytics lead OR data lead OR "
        "business systems analyst OR reporting specialist OR business insights analyst OR people analytics analyst OR "
        "pricing analyst OR actuarial analyst OR workforce analyst OR policy analyst OR performance analyst OR "
        "competitive intelligence analyst OR strategy analyst"
    )
)

GCP_PROJECT = os.getenv("GCP_PROJECT", "GCP_PROJECT")
BUCKET_NAME = os.getenv("BUCKET_NAME", "BUCKET_NAME")
GCP_SA_KEY = os.getenv("GCP_SA_KEY")  # This should contain the JSON key content

PROCESSED_PREFIX = "processed"

# --- API Fetch ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_page(page: int = 1, per_page: int = 50) -> List[Dict]:
    """Fetch one page of Adzuna USA job listings."""
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        raise RuntimeError("ADZUNA_APP_ID and ADZUNA_APP_KEY must be set as environment variables")

    base_url = f"https://api.adzuna.com/v1/api/jobs/{ADZUNA_COUNTRY}/search/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": per_page,
        "what": ADZUNA_QUERY,
        "content-type": "application/json",
    }

    resp = requests.get(base_url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("results", [])

# --- Transform ---
def stable_job_id(record: Dict) -> str:
    """Create stable hash ID for deduplication."""
    if record.get("id"):
        return str(record["id"])
    key_fields = "|".join(str(record.get(k, "")).strip() for k in ("title", "company", "location"))
    return hashlib.sha256(key_fields.encode("utf-8")).hexdigest()

def transform(records: List[Dict]) -> pd.DataFrame:
    """Transform Adzuna API JSON into relational tabular structure."""
    rows = []
    ts = datetime.now(timezone.utc).isoformat()

    for r in records:
        jid = stable_job_id(r)
        company = r.get("company") or {}
        location = r.get("location") or {}
        category = r.get("category") or {}

        rows.append({
            "job_id": jid,
            "title": r.get("title"),
            "description": r.get("description"),
            "salary_min": r.get("salary_min"),
            "salary_max": r.get("salary_max"),
            "created": r.get("created"),
            "redirect_url": r.get("redirect_url"),
            "company_name": company.get("display_name"),
            "city": location.get("area")[1] if isinstance(location.get("area"), list) and len(location.get("area")) > 1 else None,
            "state": location.get("area")[0] if isinstance(location.get("area"), list) else None,
            "country": location.get("display_name"),
            "category_label": category.get("label"),
            "contract_type": r.get("contract_type"),
            "contract_time": r.get("contract_time"),
            "posting_week": pd.to_datetime(r.get("created")).isocalendar().week if r.get("created") else None,
            "ingest_ts": ts
        })

    df = pd.DataFrame(rows)
    df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
    df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")
    df["ingest_date"] = pd.to_datetime(df["ingest_ts"]).dt.date.astype(str)
    return df

# --- GCS Handling ---
def read_existing_processed(bucket: str, ingest_date: str) -> Optional[pd.DataFrame]:
    blob = f"{PROCESSED_PREFIX}/{ingest_date}/jobs.parquet"
    if not blob_exists(bucket, blob):
        return None
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    try:
        download_blob_to_file(bucket, blob, tmp.name)
        return pd.read_parquet(tmp.name)
    finally:
        tmp.close()

def write_processed_and_upload(df: pd.DataFrame, bucket: str, ingest_date: str):
    existing = read_existing_processed(bucket, ingest_date)
    if existing is not None and not existing.empty:
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["job_id"], keep="last")
    else:
        combined = df.drop_duplicates(subset=["job_id"], keep="last")

    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    combined.to_parquet(tmp.name, index=False)
    dest_blob = f"{PROCESSED_PREFIX}/{ingest_date}/jobs.parquet"
    upload_file(bucket, dest_blob, tmp.name, content_type="application/octet-stream")
    logger.info("Uploaded processed parquet with %d rows", len(combined))

# --- Runner ---
def run_ingestion(max_pages: int = 2, per_page: int = 50, bucket_name: Optional[str] = None):
    """Main ingestion runner with paging."""
    bucket = bucket_name or BUCKET_NAME

    for page in range(1, max_pages + 1):
        logger.info("Fetching page %d of %d", page, max_pages)
        records = fetch_page(page=page, per_page=per_page)
        if not records:
            logger.info("No more records; stopping at page %d", page)
            break
        ingest_date = datetime.now(timezone.utc).date().isoformat()
        df = transform(records)
        write_processed_and_upload(df, bucket, ingest_date)

    logger.info("Ingestion completed.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest job data from Adzuna and upload to GCS")
    parser.add_argument("--pages", type=int, default=2, help="Number of pages to fetch from Adzuna")
    parser.add_argument("--per_page", type=int, default=50, help="Number of records per page")
    parser.add_argument("--bucket_name", type=str, default=None, help="Override GCS bucket name")
    args = parser.parse_args()

    logger.info(f"Starting ingestion: pages={args.pages}, per_page={args.per_page}, bucket={args.bucket_name or BUCKET_NAME}")
    try:
        run_ingestion(max_pages=args.pages, per_page=args.per_page, bucket_name=args.bucket_name)
        logger.info("Ingestion completed successfully.")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        raise
