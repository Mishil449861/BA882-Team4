# test_ingest.py
# Purpose: tests for the simplified processed job ingestion (Data Science–specific).

import pandas as pd


def sample_record(id_val=None, title="Data Scientist", company="ACME Analytics", location="New York"):
    """Simulate a single Adzuna record for testing."""
    return {
        "id": id_val,
        "title": title,
        "company": {"display_name": company},
        "location": {"display_name": location},
        "salary_min": 90000,
        "salary_max": 130000,
        "created": "2025-10-10T12:00:00Z",
        "redirect_url": "https://adzuna.com/job/12345",
        "category": {"label": "Data Science"},
        "contract_type": "permanent",
        "contract_time": "full_time",
        "description": "Data Scientist role focused on machine learning and data analytics."
    }


def test_stable_job_id_uses_existing_id():
    """Ensure stable_job_id returns provided ID if available."""
    record = sample_record(id_val="JOB123")
    assert stable_job_id(record) == "JOB123"


def test_stable_job_id_hash_when_missing():
    """Ensure stable_job_id generates a valid hash when ID is missing."""
    record = sample_record(id_val=None)
    sid = stable_job_id(record)
    assert isinstance(sid, str)
    assert len(sid) == 64  # SHA256 hex digest


def test_transform_returns_dataframe():
    """Ensure transform() returns a DataFrame with correct processed columns."""
    records = [
        sample_record("1", title="Data Scientist"),
        sample_record("2", title="Machine Learning Engineer")
    ]
    df = transform(records)
    assert isinstance(df, pd.DataFrame)
    # Match your current processed columns
    expected_cols = {
        "job_id", "title", "salary_min", "salary_max", "created",
        "redirect_url", "ingest_ts", "ingest_date"
    }
    missing = expected_cols - set(df.columns)
    assert not missing, f"Missing columns in transformed DataFrame: {missing}"
    assert len(df) == 2


def test_salary_columns_are_numeric():
    """Ensure salary_min and salary_max are numeric after transform."""
    df = transform([sample_record("1")])
    assert pd.api.types.is_numeric_dtype(df["salary_min"])
    assert pd.api.types.is_numeric_dtype(df["salary_max"])


def test_ingest_date_is_string_date():
    """Ensure ingest_date is a string-formatted date."""
    df = transform([sample_record("1")])
    sample_date = df["ingest_date"].iloc[0]
    assert isinstance(sample_date, str)
    assert len(sample_date.split("-")) == 3  # YYYY-MM-DD
