# test_ingest.py
# Purpose: basic tests for ingestion transform and stable id generation.

import pandas as pd
from ingest import stable_job_id, transform

def sample_record(id_val=None, title="Data Scientist", company="ACME", location="Boston"):
    return {
        "id": id_val,
        "title": title,
        "company": {"display_name": company},
        "location": {"display_name": location},
        "salary_min": "100000",
        "salary_max": "150000",
        "created": "2025-09-30T12:00:00Z",
        "redirect_url": "https://example.com/job"
    }

def test_stable_job_id_prefers_id():
    r = sample_record(id_val="ABC123")
    assert stable_job_id(r) == "ABC123"

def test_stable_job_id_hash_when_missing():
    r = sample_record(id_val=None)
    sid = stable_job_id(r)
    assert isinstance(sid, str) and len(sid) == 64  # sha256 hex digest

def test_transform_returns_dataframe():
    records = [sample_record("1"), sample_record("2", title="Engineer")]
    df = transform(records)
    assert isinstance(df, pd.DataFrame)
    assert "job_id" in df.columns
    assert "ingest_date" in df.columns
    assert df.shape[0] == 2
