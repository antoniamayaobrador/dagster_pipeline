import requests
import time
from dagster import asset, Output, MetadataValue

# Base config
AIRBYTE_HOST = "http://13.39.50.171.nip.io"
CONNECTION_ID = "9391a2b8-d03e-4c77-b294-77ba57b359d7"  # Updated to your actual connection ID
CLIENT_ID = "66d891a9-88a9-4963-8798-2aa3ea54f402"
CLIENT_SECRET = "KzxIM8REdry0ytOGT2CkU4aYWtGkwXhV"

def get_airbyte_token():
    url = f"{AIRBYTE_HOST}/api/v1/applications/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
        # Removed grant_type as it's not needed based on our testing
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    
    # For explicit debugging:
    if response.status_code != 200:
        raise Exception(f"Token request failed: {response.status_code} - {response.text}")

    return response.json()["access_token"]


def sync_airbyte_connection():
    token = get_airbyte_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json"
    }

    # Changed from public API to v1 API which we know works
    sync_url = f"{AIRBYTE_HOST}/api/v1/connections/sync"
    response = requests.post(sync_url, json={"connectionId": CONNECTION_ID}, headers=headers)
    response.raise_for_status()
    job_info = response.json()
    return job_info, token

@asset
def airbyte_sync_asset():
    # Starting sync
    print(f"Triggering sync for Airbyte connection: {CONNECTION_ID}")
    job, token = sync_airbyte_connection()
    
    # Get job ID from correct location in response
    job_id = job.get("job", {}).get("id")
    if not job_id:
        raise Exception(f"Failed to get job ID from response: {job}")
    
    print(f"Started Airbyte sync job with ID: {job_id}")
    
    status = "running"
    poll_url = f"{AIRBYTE_HOST}/api/v1/jobs/get"

    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json"
    }
    
    # Track some job metrics
    attempts = 0
    start_time = time.time()
    
    # Poll for job status
    while status not in ["succeeded", "failed", "cancelled"]:
        time.sleep(10)
        try:
            resp = requests.post(poll_url, json={"id": job_id}, headers=headers)
            resp.raise_for_status()
            job_info = resp.json().get("job", {})
            status = job_info.get("status", "unknown")
            
            # Extract more useful info for logging
            attempts = len(job_info.get("attempts", []))
            current_attempt = job_info.get("attempts", [])[-1] if attempts > 0 else {}
            attempt_status = current_attempt.get("status", "unknown") if current_attempt else "unknown"
            
            print(f"Polling job {job_id}... status: {status}, attempt #{attempts}, attempt status: {attempt_status}")
        except Exception as e:
            print(f"Error polling job status: {e}")
            # Don't fail yet, try a few more times
            time.sleep(10)

    # Calculate duration
    duration_seconds = int(time.time() - start_time)
    
    if status != "succeeded":
        raise Exception(f"Airbyte job {job_id} failed with status: {status}")

    # Get final job details
    resp = requests.post(poll_url, json={"id": job_id}, headers=headers)
    job_info = resp.json().get("job", {})
    
    # Extract record counts and other statistics if available
    records_stats = {}
    total_records = 0
    
    for attempt in job_info.get("attempts", []):
        for stream_stat in attempt.get("streamStats", []):
            stream_name = stream_stat.get("streamName", "unknown")
            records = stream_stat.get("stats", {}).get("recordsEmitted", 0)
            records_stats[stream_name] = records
            total_records += records
    
    print(f"Airbyte sync completed successfully. Job ID: {job_id}, Total records: {total_records}")
    
    # Create metadata for Dagster
    metadata = {
        "job_id": MetadataValue.text(str(job_id)),
        "status": MetadataValue.text(status),
        "duration_seconds": MetadataValue.int(duration_seconds),
        "total_records": MetadataValue.int(total_records),
        "attempts": MetadataValue.int(attempts)
    }
    
    # Add stream stats to metadata
    for stream, count in records_stats.items():
        metadata[f"records_{stream}"] = MetadataValue.int(count)
    
    # Add timestamps if available
    if job_info.get("createdAt"):
        metadata["start_time"] = MetadataValue.text(
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job_info.get("createdAt") / 1000))
        )
    if job_info.get("updatedAt"):
        metadata["end_time"] = MetadataValue.text(
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job_info.get("updatedAt") / 1000))
        )

    return Output(
        value={"job_id": job_id, "records": total_records},
        metadata=metadata
    )