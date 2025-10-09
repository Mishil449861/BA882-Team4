# dag_ingest.py
# Purpose: minimal Airflow DAG stub to call the ingestion function daily.
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "dataeng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def task_run_ingest(**kwargs):
    # Import inside task so Airflow environment loads modules when task runs
    from ingest import run_ingestion
    run_ingestion(max_pages=3, per_page=50)

with DAG(
    dag_id="person1_ingest_stub",
    default_args=default_args,
    description="Stub DAG for Person1 ingestion; Person2 will deploy/schedule in Astronomer",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    run_task = PythonOperator(
        task_id="run_person1_ingest",
        python_callable=task_run_ingest,
        provide_context=True
    )

    run_task
