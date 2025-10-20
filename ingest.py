# ingest.py
# Purpose: Fetch Adzuna USA data jobs, transform into relational structure,
# deduplicate, upload processed Parquet to GCS.

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
from google.cloud import storage

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
GCP_PROJECT = os.getenv("GCP_PROJECT")

# ------------------------
# Helpers
# ------------------------

def upload_to_gcs(bucket_name: str, source_file: str, destination_blob: str):
    """Uploads a file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    logging.info(f"Uploaded {source_file} to gs://{bucket_name}/{destination_blob}")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_jobs(page: int, results_per_page: int = 50) -> List[Dict]:
    """Fetches jobs from Adzuna API with retries."""
    url = f"https://api.adzuna.com/v1/api/jobs/us/search/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": results_per_page,
        "content-type": "application/json",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def transform_data(jobs_data: List[Dict]):
    """Transforms raw jobs into structured tables."""
    jobs_records = []
    categories_records = []
    companies_records = []
    locations_records = []
    jobstats_records = []

    for job in jobs_data:
        job_id = str(job.get("id", ""))

        # Jobs Table
        jobs_records.append({
            "job_id": job_id,
            "title": job.get("title"),
            "description": job.get("description"),
            "created": job.get("created"),
            "redirect_url": job.get("redirect_url")
        })

        # Categories Table
        if job.get("category"):
            categories_records.append({
                "job_id": job_id,
                "category_label": job["category"].get("label")
            })

        # Companies Table
        if job.get("company"):
            companies_records.append({
                "job_id": job_id,
                "company_name": job["company"].get("display_name")
            })

        # Locations Table
        if job.get("location"):
            loc = job["location"].get("area", [])
            city = loc[1] if len(loc) > 1 else None
            state = loc[0] if len(loc) > 0 else None
            locations_records.append({
                "job_id": job_id,
                "city": city,
                "state": state,
                "country": "US"
            })

        # Jobstats Table (example: number of words)
        jobstats_records.append({
            "job_id": job_id,
            "desc_word_count": len(job.get("description", "").split())
        })

    return (
        pd.DataFrame(jobs_records),
        pd.DataFrame(categories_records),
        pd.DataFrame(companies_records),
        pd.DataFrame(locations_records),
        pd.DataFrame(jobstats_records)
    )


def process_and_upload(pages: int = 2, per_page: int = 50):
    all_jobs = []
    for page in range(1, pages + 1):
        logging.info(f"Fetching page {page}")
        jobs = fetch_jobs(page, per_page)
        all_jobs.extend(jobs)

    jobs_df, categories_df, companies_df, locations_df, jobstats_df = transform_data(all_jobs)

    # Fix schema: city → STRING (to avoid STRING vs INTEGER mismatch)
    if "city" in locations_df.columns:
        locations_df["city"] = locations_df["city"].fillna("").astype(str)

    with tempfile.TemporaryDirectory() as tmpdir:
        paths = {
            "jobs": os.path.join(tmpdir, "jobs.parquet"),
            "categories": os.path.join(tmpdir, "categories.parquet"),
            "companies": os.path.join(tmpdir, "companies.parquet"),
            "locations": os.path.join(tmpdir, "locations.parquet"),
            "jobstats": os.path.join(tmpdir, "jobstats.parquet"),
        }

        jobs_df.to_parquet(paths["jobs"], index=False)
        categories_df.to_parquet(paths["categories"], index=False)
        companies_df.to_parquet(paths["companies"], index=False)
        locations_df.to_parquet(paths["locations"], index=False)
        jobstats_df.to_parquet(paths["jobstats"], index=False)

        for table_name, file_path in paths.items():
            destination_blob = f"processed/{table_name}/{os.path.basename(file_path)}"
            upload_to_gcs(BUCKET_NAME, file_path, destination_blob)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--per_page", type=int, default=50)
    args = parser.parse_args()

    process_and_upload(args.pages, args.per_page)
