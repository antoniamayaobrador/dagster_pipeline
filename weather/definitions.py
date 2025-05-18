# dbt_weather/weather/definitions.py

import os
from pathlib import Path
from dagster import Definitions, file_relative_path
from dagster_dbt import DbtCliResource

# Import assets
from weather.airbyte import airbyte_sync_asset
from weather.dbt import weather_project_dbt_assets
from weather.schedules import schedules
from weather.constants import DBT_PROJECT_DIR, DBT_PROFILES_DIR, DBT_TARGET_DIR

# Debug information
print("=== DBT Configuration ===")
print(f"Project directory: {DBT_PROJECT_DIR}")
print(f"Profiles directory: {DBT_PROFILES_DIR}")
print(f"Target directory: {DBT_TARGET_DIR}")
print(f"Project exists: {os.path.exists(DBT_PROJECT_DIR)}")
print(f"dbt_project.yml exists: {os.path.exists(os.path.join(DBT_PROJECT_DIR, 'dbt_project.yml'))}")

# Ensure the target directory exists
Path(DBT_TARGET_DIR).mkdir(parents=True, exist_ok=True)

# Prepare assets list
assets_list = [airbyte_sync_asset]  # Always include Airbyte asset
if weather_project_dbt_assets is not None:
    assets_list.append(weather_project_dbt_assets)
    print("Added dbt assets to the pipeline")

# Initialize DBT resource
dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    target_dir=DBT_TARGET_DIR,
    target="dev"
)

# Definitions
defs = Definitions(
    assets=assets_list,
    jobs=[],  # Jobs are defined in schedules.py
    schedules=schedules,
    resources={
        "dbt": dbt_resource,
    },
)

print("=== Dagster Definitions Loaded Successfully ===")
