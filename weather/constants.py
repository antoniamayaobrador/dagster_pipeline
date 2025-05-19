import os
from pathlib import Path
from dagster import EnvVar
from dotenv import load_dotenv
load_dotenv()


# Define paths
def file_relative_path(file: str, relative_path: str) -> str:
    """Get the absolute path of a file relative to the current file."""
    return str(Path(file).parent.joinpath(relative_path).resolve())

# Project directories
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR")
DBT_TARGET_DIR = os.environ.get("DBT_TARGET_DIR")
# Ensure target directory exists
Path(DBT_TARGET_DIR).mkdir(parents=True, exist_ok=True)

# Airbyte configuration
AIRBYTE_CONNECTION_ID = os.environ.get("AIRBYTE_CONNECTION_ID", "9391a2b8-d03e-4c77-b294-77ba57b359d7")

# Airflow API credentials (consider moving these to environment variables in production)
CLIENT_ID = os.environ.get("AIRBYTE_CLIENT_ID", "66d891a9-88a9-4963-8798-2aa3ea54f402")
CLIENT_SECRET = os.environ.get("AIRBYTE_CLIENT_SECRET", "KzxIM8REdry0ytOGT2CkU4aYWtGkwXhV")

# Airbyte server configuration
AIRBYTE_HOST = os.environ.get("AIRBYTE_HOST", "http://13.39.50.171.nip.io")
AIRBYTE_PORT = os.environ.get("AIRBYTE_PORT", "80")

AIRBYTE_CONFIG = {
    "host": AIRBYTE_HOST,
    "port": AIRBYTE_PORT,
    "username": os.environ.get("AIRBYTE_USERNAME", "antoni.amaya.obrador@gmail.com"),
    "password": os.environ.get("AIRBYTE_PASSWORD", ""),
}

DBT_CONFIG = {
    "project_dir": DBT_PROJECT_DIR,
    "profiles_dir": DBT_PROFILES_DIR,
}

# Schedule configuration
WEATHER_SCHEDULE_CRON = os.environ.get("WEATHER_SCHEDULE_CRON", "0 8 * * *") # 8am daily
EXECUTION_TIMEZONE = os.environ.get("EXECUTION_TIMEZONE", "Europe/Madrid")

