# gcs_utils.py
# Purpose: small utilities for GCS uploads/downloads and existence checks.

import tempfile
from typing import List
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__name__)

def get_storage_client():
    # Will use GOOGLE_APPLICATION_CREDENTIALS or default credentials
    return storage.Client()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
def upload_file(bucket_name: str, dest_blob: str, local_path: str, content_type: str = None) -> str:
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_blob)
    blob.upload_from_filename(local_path, content_type=content_type)
    logger.info("Uploaded local %s to gs://%s/%s", local_path, bucket_name, dest_blob)
    return f"gs://{bucket_name}/{dest_blob}"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
def download_blob_to_file(bucket_name: str, blob_name: str, local_dest: str) -> str:
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{bucket_name}/{blob_name} not found")
    blob.download_to_filename(local_dest)
    logger.info("Downloaded gs://%s/%s to %s", bucket_name, blob_name, local_dest)
    return local_dest

def blob_exists(bucket_name: str, blob_name: str) -> bool:
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    return bucket.blob(blob_name).exists()

def list_blobs_with_prefix(bucket_name: str, prefix: str) -> List[str]:
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    return [b.name for b in client.list_blobs(bucket, prefix=prefix)]
