# test_ingest.py
# Purpose: basic tests for processed job ingestion focused on Data Science-related roles.

import pandas as pd
from ingest import stable_job_id, transform


def sample_record(id_val=None, title="Data Scientist", company="ACME Analytics", location="New York"):
    """Create a representative Adzuna-like record for testing."""
    return {
        "id": id_val,
        "title": title,
        "company": {"display_name": company},
        "location": {"display_name": location},
        "salary_min": 90000,
        "salary_max": 130000,
        "created": "2025-10-10T12:00:00Z",
        "redirect_url": "https://adzuna.com/job/12345",
        "contract_type": "permanent",
        "contract_time": "full_time",
        "category": {"label": "Data Science"},
        "description": "We are looking for a Data Scientist with strong analytical and ML skills."
    }


def test_stable_job_id_uses_existing_id():
    """Ensure stable_job_id returns the provided job ID if present."""
    record = sample_record(id_val="JOB123")
    assert stable_job_id(record) == "JOB123"


def test_stable_job_id_hash_when_missing():
    """Ensure stable_job_id generates a valid hash when ID is missing."""
    record = sample_record(id_val=None)
    sid = stable_job_id(record)
    assert isinstance(sid, str)
    assert len(sid) == 64  # SHA256 hash length


def test_transform_returns_dataframe():
    """Ensure transform() returns a proper DataFrame with expected processed columns."""
    records = [
        sample_record("1", title="Data Scientist"),
        sample_record("2", title="Machine Learning Engineer")
    ]
    df = transform(records)
    assert isinstance(df, pd.DataFrame)
    expected_cols = {
        "job_id", "orig_id", "title", "company", "location",
        "salary_min", "salary_max", "created", "redirect_url",
        "ingest_date", "ingest_ts", "raw_json"
    }
    missing = expected_cols - set(df.columns)
    assert not missing, f"Missing columns in transformed DataFrame: {missing}"
    assert len(df) == 2


def test_salary_columns_are_numeric():
    """Ensure salary_min and salary_max are numeric after transform."""
    records = [sample_record("1")]
    df = transform(records)
    assert pd.api.types.is_numeric_dtype(df["salary_min"])
    assert pd.api.types.is_numeric_dtype(df["salary_max"])


def test_ingest_date_is_string_date():
    """Ensure ingest_date is a string-formatted date."""
    records = [sample_record("1")]
    df = transform(records)
    sample_date = df["ingest_date"].iloc[0]
    assert isinstance(sample_date, str)
    assert len(sample_date.split("-")) == 3  # YYYY-MM-DD format
