import requests
import time
from datetime import datetime

# === CONFIGURATION ===
AIRBYTE_HOST = "http://13.39.50.171.nip.io"
CLIENT_ID = "66d891a9-88a9-4963-8798-2aa3ea54f402"
CLIENT_SECRET = "KzxIM8REdry0ytOGT2CkU4aYWtGkwXhV"

# === API FUNCTIONS ===

def get_access_token():
    url = f"{AIRBYTE_HOST}/api/v1/applications/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()["access_token"]

def list_workspaces(token):
    url = f"{AIRBYTE_HOST}/api/v1/workspaces/list"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json"
    }
    response = requests.post(url, headers=headers, json={})
    response.raise_for_status()
    return response.json()["workspaces"]

def list_connections(token, workspace_id):
    url = f"{AIRBYTE_HOST}/api/v1/connections/list"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json"
    }
    payload = {
        "workspaceId": workspace_id
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["connections"]

def get_connection_details(token, connection_id):
    url = f"{AIRBYTE_HOST}/api/v1/connections/get"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json"
    }
    payload = {
        "connectionId": connection_id
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def list_jobs(token, connection_id, limit=10):
    url = f"{AIRBYTE_HOST}/api/v1/jobs/list"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json"
    }
    payload = {
        "configTypes": ["sync"],
        "configId": connection_id,
        "pagination": {
            "pageSize": limit
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["jobs"]

def trigger_sync(token, connection_id):
    url = f"{AIRBYTE_HOST}/api/v1/connections/sync"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json"
    }
    payload = {
        "connectionId": connection_id
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def get_job_status(token, job_id):
    url = f"{AIRBYTE_HOST}/api/v1/jobs/get"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json"
    }
    payload = {
        "id": job_id
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()["job"]

def get_job_logs(token, job_id):
    url = f"{AIRBYTE_HOST}/api/v1/jobs/get_debug_info"
    headers = {
        "Authorization": f"Bearer {token}",
        "accept": "application/json",
        "content-type": "application/json"
    }
    payload = {
        "id": job_id
    }
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def format_timestamp(timestamp_ms):
    """Convert millisecond timestamp to readable date format"""
    if timestamp_ms is None:
        return "N/A"
    return datetime.fromtimestamp(timestamp_ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

# === MENU FUNCTIONS ===

def display_menu():
    print("\n=== Airbyte API Manager ===")
    print("1. List connections")
    print("2. View connection details")
    print("3. View recent sync jobs")
    print("4. Trigger a new sync")
    print("5. Check sync job status")
    print("6. Get job logs")
    print("7. Exit")
    return input("\nSelect an option (1-7): ")

def handle_list_connections(token, workspace_id):
    print("\n🔄 Listing connections...")
    connections = list_connections(token, workspace_id)
    if not connections:
        print("No connections found.")
        return None
    
    print(f"Found {len(connections)} connections:")
    for i, conn in enumerate(connections, 1):
        print(f"{i}. {conn['name']} (ID: {conn['connectionId']})")
    
    if len(connections) == 1:
        return connections[0]['connectionId']
    
    # If multiple connections, let user choose one
    if len(connections) > 1:
        try:
            idx = int(input("\nSelect a connection number: ")) - 1
            if 0 <= idx < len(connections):
                return connections[idx]['connectionId']
        except ValueError:
            pass
    
    return None

def handle_view_connection_details(token, connection_id):
    print(f"\n📋 Getting details for connection {connection_id}...")
    details = get_connection_details(token, connection_id)
    
    print(f"Name: {details.get('name', 'Unknown')}")
    print(f"Status: {details.get('status', 'Unknown')}")
    
    # Display schedule information
    schedule_type = details.get("scheduleType", "Unknown")
    print(f"Schedule Type: {schedule_type}")
    if schedule_type == "manual":
        print("Schedule: Manual trigger only")
    elif schedule_type == "basic":
        print(f"Schedule: {details.get('scheduleData', {}).get('basicSchedule', {})}")
    elif schedule_type == "cron":
        print(f"Cron Expression: {details.get('scheduleData', {}).get('cron', {}).get('cronExpression')}")
    
    # Source/destination information
    print(f"Source ID: {details.get('sourceId', 'Unknown')}")
    print(f"Destination ID: {details.get('destinationId', 'Unknown')}")
    
    # Stream information
    if "catalog" in details:
        streams = details["catalog"].get("streams", [])
        print(f"\nSyncing {len(streams)} streams:")
        for stream in streams:
            stream_name = stream.get("stream", {}).get("name", "Unknown")
            sync_mode = stream.get("config", {}).get("syncMode", "Unknown")
            destination_sync_mode = stream.get("config", {}).get("destinationSyncMode", "Unknown")
            print(f"  - {stream_name}")
            print(f"    Sync Mode: {sync_mode}")
            print(f"    Destination Sync Mode: {destination_sync_mode}")

def handle_view_recent_jobs(token, connection_id):
    print(f"\n📊 Getting recent jobs for connection {connection_id}...")
    jobs = list_jobs(token, connection_id)
    
    if not jobs:
        print("No recent jobs found.")
        return None
    
    print(f"Recent jobs (most recent first):")
    for i, job in enumerate(jobs, 1):
        status = job.get('status', 'Unknown')
        created_at = format_timestamp(job.get('createdAt'))
        updated_at = format_timestamp(job.get('updatedAt'))
        
        print(f"{i}. Job ID: {job['id']}")
        print(f"   Status: {status}")
        print(f"   Created: {created_at}")
        print(f"   Last Updated: {updated_at}")
        print()
    
    if len(jobs) == 1:
        return jobs[0]['id']
    
    # If multiple jobs, let user choose one
    if len(jobs) > 1:
        try:
            idx = int(input("\nSelect a job number to view details (or 0 to cancel): ")) - 1
            if 0 <= idx < len(jobs):
                return jobs[idx]['id']
        except ValueError:
            pass
    
    return None

def handle_trigger_sync(token, connection_id):
    print(f"\n▶️ Triggering sync for connection {connection_id}...")
    try:
        result = trigger_sync(token, connection_id)
        job_id = result.get('job', {}).get('id')
        if job_id:
            print(f"✅ Sync job triggered successfully! Job ID: {job_id}")
            return job_id
        else:
            print("⚠️ Sync triggered but could not get job ID.")
            return None
    except requests.HTTPError as e:
        print(f"❌ Failed to trigger sync: {e}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print(e.response.text)
        return None

def handle_check_job_status(token, job_id):
    if not job_id:
        job_id = input("\nEnter Job ID: ")
    
    print(f"\n🔍 Checking status for job {job_id}...")
    try:
        job = get_job_status(token, job_id)
        
        status = job.get('status', 'Unknown')
        created_at = format_timestamp(job.get('createdAt'))
        updated_at = format_timestamp(job.get('updatedAt'))
        
        print(f"Job ID: {job_id}")
        print(f"Status: {status}")
        print(f"Created: {created_at}")
        print(f"Last Updated: {updated_at}")
        
        if 'attempts' in job:
            for i, attempt in enumerate(job['attempts'], 1):
                attempt_status = attempt.get('status', 'Unknown')
                attempt_created = format_timestamp(attempt.get('createdAt'))
                attempt_updated = format_timestamp(attempt.get('updatedAt'))
                attempt_ended = format_timestamp(attempt.get('endedAt'))
                
                print(f"\nAttempt {i}:")
                print(f"  Status: {attempt_status}")
                print(f"  Started: {attempt_created}")
                print(f"  Updated: {attempt_updated}")
                print(f"  Ended: {attempt_ended}")
                
                # Show records info if available
                if 'streamStats' in attempt:
                    print("\n  Stream Stats:")
                    for stream in attempt['streamStats']:
                        name = stream.get('streamName', 'Unknown')
                        stats = stream.get('stats', {})
                        records_emitted = stats.get('recordsEmitted', 0)
                        bytes_emitted = stats.get('bytesEmitted', 0)
                        records_committed = stats.get('recordsCommitted', 0)
                        
                        print(f"    {name}:")
                        print(f"      Records Emitted: {records_emitted}")
                        print(f"      Bytes Emitted: {bytes_emitted}")
                        print(f"      Records Committed: {records_committed}")
        
        return job_id
    except requests.HTTPError as e:
        print(f"❌ Failed to get job status: {e}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print(e.response.text)
        return None

def handle_get_job_logs(token, job_id):
    if not job_id:
        job_id = input("\nEnter Job ID: ")
    
    print(f"\n📜 Getting logs for job {job_id}...")
    try:
        log_info = get_job_logs(token, job_id)
        
        # Display attempts info and logs
        attempts = log_info.get('attempts', [])
        for i, attempt in enumerate(attempts, 1):
            print(f"\n=== Attempt {i} Logs ===")
            logs = attempt.get('logs', {}).get('logLines', [])
            
            if logs:
                for log in logs[:100]:  # Show only first 100 log lines to avoid overwhelming the console
                    print(log)
                
                if len(logs) > 100:
                    print(f"\n... {len(logs) - 100} more log lines not shown ...")
            else:
                print("No logs available for this attempt.")
        
        return job_id
    except requests.HTTPError as e:
        print(f"❌ Failed to get job logs: {e}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print(e.response.text)
        return None

def wait_for_job_completion(token, job_id, timeout_seconds=300):
    """Wait for a job to complete or fail, with a timeout"""
    print(f"\n⏳ Waiting for job {job_id} to complete...")
    print("Press Ctrl+C to stop waiting")
    
    start_time = time.time()
    try:
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                print(f"\n⚠️ Timeout after waiting {timeout_seconds} seconds")
                return False
            
            job = get_job_status(token, job_id)
            status = job.get('status', 'Unknown')
            
            if status == "succeeded":
                print(f"\n✅ Job completed successfully after {int(elapsed)} seconds")
                return True
            elif status in ["failed", "cancelled"]:
                print(f"\n❌ Job {status} after {int(elapsed)} seconds")
                return False
            
            print(f"Current status: {status} (waiting {int(elapsed)} seconds so far)")
            time.sleep(10)  # Check every 10 seconds
    except KeyboardInterrupt:
        print("\n⚠️ Waiting interrupted by user")
        return False

# === MAIN FUNCTION ===

def main():
    try:
        print("🔐 Getting access token...")
        token = get_access_token()
        print("✅ Access token retrieved")
        
        # Get workspace ID just once
        workspaces = list_workspaces(token)
        workspace_id = workspaces[0]["workspaceId"]
        print(f"✅ Using workspace: {workspaces[0]['name']} (ID: {workspace_id})")
        
        connection_id = None
        job_id = None
        
        while True:
            choice = display_menu()
            
            if choice == '1':
                connection_id = handle_list_connections(token, workspace_id)
            
            elif choice == '2':
                if not connection_id:
                    connection_id = handle_list_connections(token, workspace_id)
                
                if connection_id:
                    handle_view_connection_details(token, connection_id)
            
            elif choice == '3':
                if not connection_id:
                    connection_id = handle_list_connections(token, workspace_id)
                
                if connection_id:
                    job_id = handle_view_recent_jobs(token, connection_id)
            
            elif choice == '4':
                if not connection_id:
                    connection_id = handle_list_connections(token, workspace_id)
                
                if connection_id:
                    job_id = handle_trigger_sync(token, connection_id)
                    
                    if job_id:
                        wait_response = input("\nWould you like to wait for the job to complete? (y/n): ")
                        if wait_response.lower() == 'y':
                            success = wait_for_job_completion(token, job_id)
                            if success:
                                handle_check_job_status(token, job_id)
            
            elif choice == '5':
                if not job_id:
                    if not connection_id:
                        connection_id = handle_list_connections(token, workspace_id)
                    
                    if connection_id:
                        job_id = handle_view_recent_jobs(token, connection_id)
                
                if job_id:
                    handle_check_job_status(token, job_id)
            
            elif choice == '6':
                if not job_id:
                    if not connection_id:
                        connection_id = handle_list_connections(token, workspace_id)
                    
                    if connection_id:
                        job_id = handle_view_recent_jobs(token, connection_id)
                
                if job_id:
                    handle_get_job_logs(token, job_id)
            
            elif choice == '7':
                print("\nExiting Airbyte API Manager. Goodbye!")
                break
            
            else:
                print("\n❌ Invalid option. Please try again.")
            
            input("\nPress Enter to continue...")

    except requests.HTTPError as e:
        print(f"❌ HTTPError: {e}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print(e.response.text)
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()