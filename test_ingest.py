# test_ingest.py
# Purpose: Tests for the multi-table Adzuna data ingestion pipeline.

import pandas as pd
from ingest import transform_data


def sample_record(id_val="JOB123", title="Data Scientist", company="ACME Analytics", city="New York"):
    """Simulate a single Adzuna API record."""
    return {
        "id": id_val,
        "title": title,
        "description": "Data Scientist role focused on machine learning and data analytics.",
        "company": {"display_name": company},
        "location": {"area": ["US", "California", city]},
        "salary_min": 90000,
        "salary_max": 130000,
        "created": "2025-10-10T12:00:00Z",
        "redirect_url": "https://adzuna.com/job/12345",
        "category": {"label": "Data Science"},
        "contract_type": "permanent",
        "contract_time": "full_time"
    }


def test_transform_returns_five_dataframes():
    """Ensure transform() returns five DataFrames."""
    records = [sample_record("1"), sample_record("2")]
    outputs = transform(records)
    assert isinstance(outputs, tuple), "transform() should return a tuple"
    assert len(outputs) == 5, "transform() should return 5 DataFrames"
    for df in outputs:
        assert isinstance(df, pd.DataFrame), "Each output should be a DataFrame"


def test_jobs_table_structure():
    """Validate jobs table has all required columns."""
    records = [sample_record("1")]
    jobs_df, *_ = transform(records)
    expected_cols = {
        "job_id", "title", "description", "salary_min", "salary_max",
        "created", "redirect_url", "ingest_ts"
    }
    assert expected_cols.issubset(jobs_df.columns), f"Missing columns in jobs_df: {expected_cols - set(jobs_df.columns)}"


def test_companies_table_structure():
    """Validate companies table has correct columns."""
    records = [sample_record("1")]
    _, companies_df, *_ = transform(records)
    expected_cols = {"job_id", "company_name"}
    assert expected_cols == set(companies_df.columns)


def test_locations_table_structure():
    """Validate locations table has correct columns and proper area parsing."""
    records = [sample_record("1", city="Pleasant Hill")]
    _, _, locations_df, *_ = transform(records)
    expected_cols = {"job_id", "city", "state", "country"}
    assert expected_cols == set(locations_df.columns)
    assert locations_df.loc[0, "city"] == "Pleasant Hill"
    assert locations_df.loc[0, "state"] == "California"
    assert locations_df.loc[0, "country"] == "US"


def test_categories_table_structure():
    """Validate categories table has correct columns."""
    records = [sample_record("1")]
    *_, categories_df, _ = transform(records)
    expected_cols = {"job_id", "category_label"}
    assert expected_cols == set(categories_df.columns)


def test_jobstats_table_structure():
    """Validate jobstats table has correct columns and posting week logic."""
    records = [sample_record("1")]
    *_, jobstats_df = transform(records)
    expected_cols = {"job_id", "contract_type", "contract_time", "posting_week"}
    assert expected_cols == set(jobstats_df.columns)
    assert pd.notnull(jobstats_df.loc[0, "posting_week"]), "posting_week should not be null"


def test_salary_columns_are_numeric():
    """Ensure salary columns in jobs_df are numeric."""
    records = [sample_record("1")]
    jobs_df, *_ = transform(records)
    assert pd.api.types.is_numeric_dtype(jobs_df["salary_min"])
    assert pd.api.types.is_numeric_dtype(jobs_df["salary_max"])
