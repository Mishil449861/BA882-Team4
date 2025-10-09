# BA882-Team4

# Data Ingestion: Adzuna-like job postings → GCS

These contains Mishil's deliverables: code to extract job postings from an external API, write raw JSON and processed Parquet (partitioned by `ingest_date`) to Google Cloud Storage (GCS). This is scoped to ingestion only

## Files
- `ingest.py` — main ingestion logic
- `gcs_utils.py` — GCS helper functions
- `dag_ingest.py` — Airflow DAG stub (do not deploy; Aryan will integrate)
- `tests/test_ingest.py` — pytest unit tests
- `.github/workflows/ci.yml` — CI (lint + tests)
- `requirements.txt` — Python deps

## Local dev prerequisites
- Python 3.10+
- Google Cloud SDK (`gcloud`) (for production/service-account setup)
- `pip install -r requirements.txt`
