"""
ingest.py
Purpose: Ingest Adzuna API data, process and export to GCS as Parquet files
         for loading into BigQuery.
"""

import os
import hashlib
import pandas as pd
import requests
from datetime import datetime
from google.cloud import storage


# -------------------
# Helper: stable job_id
# -------------------
def stable_job_id(record):
    """Return existing job ID if present, otherwise generate SHA256 hash."""
    if record.get("id"):
        return record["id"]
    raw = (record.get("title", "") + record.get("company", {}).get("display_name", "") +
           record.get("location", {}).get("display_name", "") +
           record.get("created", ""))
    return hashlib.sha256(raw.encode()).hexdigest()


# -------------------
# Fetch from Adzuna
# -------------------
def fetch_adzuna_jobs(pages=2, per_page=50):
    """Fetch jobs from Adzuna API."""
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")

    jobs = []
    for page in range(1, pages + 1):
        url = (
            f"https://api.adzuna.com/v1/api/jobs/us/search/{page}"
            f"?app_id={app_id}&app_key={app_key}&results_per_page={per_page}"
            f"&what=data%20science"
        )
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            jobs.extend(data.get("results", []))
        else:
            print(f"⚠️ Failed to fetch page {page}: {resp.status_code}")
    return jobs


# -------------------
# Transform
# -------------------
def transform(records):
    """Process raw records into a cleaned DataFrame."""
    processed = []
    for rec in records:
        processed.append({
            "job_id": stable_job_id(rec),
            "title": rec.get("title"),
            "company": rec.get("company", {}).get("display_name"),
            "city": rec.get("location", {}).get("display_name"),
            "state": rec.get("location", {}).get("area", [None, None, None])[-1],
            "country": "US",
            "salary_min": rec.get("salary_min"),
            "salary_max": rec.get("salary_max"),
            "created": rec.get("created"),
            "redirect_url": rec.get("redirect_url"),
            "category": rec.get("category", {}).get("label"),
            "contract_type": rec.get("contract_type"),
            "contract_time": rec.get("contract_time"),
            "description": rec.get("description"),
            "ingest_ts": datetime.utcnow().isoformat(),
            "ingest_date": datetime.utcnow().date().isoformat(),
        })

    df = pd.DataFrame(processed)

    # Normalize data types
    for col in ["job_id", "title", "company", "city", "state", "country",
                "redirect_url", "category", "contract_type", "contract_time", "description"]:
        df[col] = df[col].astype(str)

    numeric_cols = ["salary_min", "salary_max"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# -------------------
# Split tables
# -------------------
def split_tables(df):
    """Split main DataFrame into 5 tables: jobs, jobstats, categories, companies, locations."""
    jobs = df[[
        "job_id", "title", "created", "redirect_url",
        "contract_type", "contract_time", "description",
        "ingest_ts", "ingest_date"
    ]]

    jobstats = df[["job_id", "salary_min", "salary_max", "ingest_ts", "ingest_date"]]

    categories = df[["job_id", "category", "ingest_ts", "ingest_date"]].drop_duplicates(subset=["category"])

    companies = df[["job_id", "company", "ingest_ts", "ingest_date"]].drop_duplicates(subset=["company"])

    locations = df[["job_id", "city", "state", "country", "ingest_ts", "ingest_date"]].drop_duplicates(
        subset=["city", "state", "country"]
    )

    # Ensure all text columns are strings (to fix schema mismatch)
    for d in [jobs, jobstats, categories, companies, locations]:
        for col in d.select_dtypes(exclude=["float", "int"]).columns:
            d[col] = d[col].astype(str)

    return jobs, jobstats, categories, companies, locations


# -------------------
# Save to Parquet + Upload
# -------------------
def save_and_upload(df, table_name):
    """Save dataframe to parquet and upload to GCS."""
    bucket_name = os.getenv("BUCKET_NAME")
    gcs_path = f"processed/{table_name}/{table_name}_{datetime.utcnow().date()}.parquet"
    local_path = f"/tmp/{table_name}.parquet"

    # Ensure correct dtypes for BQ schema
    if table_name == "locations":
        df["city"] = df["city"].astype(str)
        df["state"] = df["state"].astype(str)
        df["country"] = df["country"].astype(str)
        df["job_id"] = df["job_id"].astype(str)

    df.to_parquet(local_path, index=False)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded {table_name} to gs://{bucket_name}/{gcs_path}")


# -------------------
# Main
# -------------------
def main(pages=2, per_page=50):
    raw = fetch_adzuna_jobs(pages, per_page)
    df = transform(raw)
    jobs, jobstats, categories, companies, locations = split_tables(df)

    # Save & upload each
    for table_name, data in zip(
        ["jobs", "jobstats", "categories", "companies", "locations"],
        [jobs, jobstats, categories, companies, locations]
    ):
        save_and_upload(data, table_name)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--per_page", type=int, default=50)
    args = parser.parse_args()

    main(args.pages, args.per_page)
