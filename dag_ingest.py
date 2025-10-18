# dag_ingest.py
# Purpose: Airflow DAG to run Adzuna ingestion daily and push to GCS

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger(__name__)

# Default args for DAG
default_args = {
    "owner": "dataeng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Python callable that actually runs ingestion
def task_run_ingest(**kwargs):
    """
    This task runs the Adzuna ingestion pipeline and uploads processed data to GCS.
    """
    from ingest import run_ingestion

    # Load bucket name and GCP project from Airflow environment
    bucket_name = os.environ.get("BUCKET_NAME")
    if not bucket_name:
        raise ValueError("Missing BUCKET_NAME in environment — cannot upload to GCS.")

    gcp_project = os.environ.get("GCP_PROJECT", "unknown")
    logger.info(f"Starting ingestion for project={gcp_project}, bucket={bucket_name}")

    try:
        # ✅ Pass the bucket name to ingestion
        run_ingestion(
            max_pages=3,
            per_page=50,
            bucket_name=bucket_name,
        )
        logger.info("Ingestion completed successfully.")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        raise

# Define DAG
with DAG(
    dag_id="adzuna_ingestion_daily",
    default_args=default_args,
    description="Daily Adzuna job postings ingestion -> GCS",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["adzuna", "gcs", "ingestion"],
) as dag:

    run_ingestion_task = PythonOperator(
        task_id="run_ingestion",
        python_callable=task_run_ingest,
        provide_context=True,
    )

    run_ingestion_task
