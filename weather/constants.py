import os
from pathlib import Path
from dagster_dbt import DbtCliResource
from dagster import file_relative_path

# Define ruta absoluta al proyecto dbt
dbt_project_dir = Path(__file__).joinpath("..", "..", "weather_project").resolve()
dbt = DbtCliResource(project_dir=os.fspath(dbt_project_dir))


# Generación o carga del manifest.json de dbt
if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_parse_invocation = dbt.cli(["parse"], manifest={}).wait()
    dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")
else:
    dbt_manifest_path = dbt_project_dir.joinpath("target", "manifest.json")

DBT_MANIFEST_PATH = dbt_manifest_path

# Airbyte configs
AIRBYTE_CONNECTION_ID = os.environ.get(
    "AIRBYTE_CONNECTION_ID", "9391a2b8-d03e-4c77-b294-77ba57b359d7" 
)


CLIENT_ID = "66d891a9-88a9-4963-8798-2aa3ea54f402"
CLIENT_SECRET = "KzxIM8REdry0ytOGT2CkU4aYWtGkwXhV"
AIRBYTE_HOST = "http://13.39.50.171.nip.io"
CONNECTION_ID = "9391a2b8-d03e-4c77-b294-77ba57b359d7"

AIRBYTE_CONFIG = {
    "host": os.environ.get("AIRBYTE_HOST", "13.39.50.171.nip.io/"),
    "port": os.environ.get("AIRBYTE_PORT", "80"),
    "username": "antoni.amaya.obrador@gmail.com",
    "password": os.environ.get("AIRBYTE_PASSWORD"),
}

# dbt config para uso con resources o Assets
def file_relative_path(file: str, relative_path: str) -> str:
    return str(Path(file).joinpath(relative_path).resolve())

DBT_PROJECT_DIR = file_relative_path(__file__, "../weather_project")
DBT_PROFILES_DIR = file_relative_path(__file__, "../weather_project")

DBT_CONFIG = {
    "project_dir": DBT_PROJECT_DIR,
    "profiles_dir": DBT_PROFILES_DIR,
}

# Schedule configuration
WEATHER_SCHEDULE_CRON = "0 8 * * *"  # 8am daily
EXECUTION_TIMEZONE = "Europe/Madrid"

