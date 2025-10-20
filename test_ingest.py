import pytest
from ingest import transform

def sample_record(jid, city="Pleasant Hill", state="California"):
    return {
        "id": jid,
        "title": "Data Analyst",
        "description": "Analyze data trends",
        "created": "2024-05-10T12:00:00Z",
        "company": {"display_name": "DataCorp"},
        "category": {"label": "Analytics"},
        "salary_min": 50000,
        "salary_max": 90000,
        "location": {"area": ["United States", state, city]},
    }


def test_transform_returns_five_dataframes():
    records = [sample_record("1"), sample_record("2")]
    outputs = transform(records)
    assert len(outputs) == 5


def test_jobs_table_structure():
    records = [sample_record("1")]
    jobs_df, *_ = transform(records)
    expected_cols = {"job_id", "title", "description", "company", "category", "created", "salary_min", "salary_max"}
    assert expected_cols == set(jobs_df.columns)


def test_companies_table_structure():
    records = [sample_record("1")]
    _, companies_df, *_ = transform(records)
    expected_cols = {"job_id", "company"}
    assert expected_cols == set(companies_df.columns)


def test_locations_table_structure():
    records = [sample_record("1", city="Pleasant Hill")]
    _, _, locations_df, *_ = transform(records)
    expected_cols = {"job_id", "city", "state"}
    assert expected_cols == set(locations_df.columns)
    assert locations_df.loc[0, "city"] == "Pleasant Hill"
    assert locations_df.loc[0, "state"] == "California"


def test_categories_table_structure():
    records = [sample_record("1")]
    *_, categories_df, _ = transform(records)
    expected_cols = {"job_id", "category"}
    assert expected_cols == set(categories_df.columns)


def test_jobstats_table_structure():
    records = [sample_record("1")]
    *_, jobstats_df = transform(records)
    expected_cols = {"job_id", "created", "posting_week"}
    assert expected_cols == set(jobstats_df.columns)


def test_salary_columns_are_numeric():
    records = [sample_record("1")]
    jobs_df, *_ = transform(records)
    assert jobs_df["salary_min"].dtype in ("int64", "float64")
    assert jobs_df["salary_max"].dtype in ("int64", "float64")
