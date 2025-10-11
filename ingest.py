# ingest.py
# Purpose: fetch jobs from Adzuna API (USA), write raw JSON to GCS, transform to Parquet, dedupe, upload processed.

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

# 👇 Country set to USA
ADZUNA_COUNTRY = os.getenv("ADZUNA_COUNTRY", "us")
# 👇 You can change the search query if needed
ADZUNA_QUERY = os.getenv("ADZUNA_QUERY", "data")

GCP_PROJECT = os.getenv("GCP_PROJECT", "ba882-team4-474802")
BUCKET_NAME = os.getenv("BUCKET_NAME", "adzuna-bucket")

RAW_PREFIX = "raw"
PROCESSED_PREFIX = "processed"

# --- API Fetch ---
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=100))
def fetch_page(page: int = 1, per_page: int = 50) -> List[Dict]:
    """
    Fetch one page from the Adzuna Jobs API (USA).
    Example:
    http://api.adzuna.com/v1/api/jobs/us/search/1?app_id=XXX&app_key=YYY&results_per_page=20&what=data
    """
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        raise RuntimeError("ADZUNA_APP_ID and ADZUNA_APP_KEY must be set as environment variables")

    base_url = f"https://api.adzuna.com/v1/api/jobs/us/search"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": per_page,
        "what": ADZUNA_QUERY,
        "content-type": "application/json"
    }

    resp = requests.get(base_url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("results", [])

# --- Transform ---
def stable_job_id(record: Dict) -> str:
    if record.get("id"):
        return str(record["id"])
    key_fields = "|".join(str(record.get(k, "")).strip() for k in ("title", "company", "location"))
    return hashlib.sha256(key_fields.encode("utf-8")).hexdigest()

def transform(records: List[Dict]) -> pd.DataFrame:
    rows = []
    ts = datetime.now(timezone.utc).isoformat()
    for r in records:
        jid = stable_job_id(r)
        rows.append({
            "job_id": jid,
            "orig_id": r.get("id"),
            "title": r.get("title"),
            "company": (r.get("company") or {}).get("display_name") if isinstance(r.get("company"), dict) else r.get("company"),
            "location": (r.get("location") or {}).get("display_name") if isinstance(r.get("location"), dict) else r.get("location"),
            "salary_min": r.get("salary_min"),
            "salary_max": r.get("salary_max"),
            "created": r.get("created"),
            "redirect_url": r.get("redirect_url"),
            "raw_json": json.dumps(r),
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

def write_raw_json(records: List[Dict], bucket: str, ingest_date: str, page_idx: int):
    tmp = tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False)
    json.dump(records, tmp)
    tmp.flush()
    tmp.close()
    dest_blob = f"{RAW_PREFIX}/{ingest_date}/raw_page_{page_idx}.json"
    upload_file(bucket, dest_blob, tmp.name, content_type="application/json")
    logger.info("Uploaded raw page %s", dest_blob)

# --- Runner ---
def run_ingestion(max_pages: int = 2, per_page: int = 20):
    for page in range(1, max_pages + 1):
        logger.info("Fetching page %d", page)
        records = fetch_page(page=page, per_page=per_page)
        if not records:
            logger.info("No records on page %d; stopping", page)
            break
        ingest_date = datetime.now(timezone.utc).date().isoformat()
        write_raw_json(records, BUCKET_NAME, ingest_date, page)
        df = transform(records)
        write_processed_and_upload(df, BUCKET_NAME, ingest_date)
    logger.info("Ingestion completed.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Adzuna USA ingestion locally")
    parser.add_argument("--pages", type=int, default=2, help="Max pages to fetch")
    parser.add_argument("--per_page", type=int, default=20)
    args = parser.parse_args()
    run_ingestion(max_pages=args.pages, per_page=args.per_page)
