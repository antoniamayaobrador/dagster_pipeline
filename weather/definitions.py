# dbt_weather/weather/definitions.py

import os
from pathlib import Path
from dagster import Definitions, file_relative_path, load_assets_from_modules
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

# Load all dbt assets
dbt_assets = [weather_project_dbt_assets] if weather_project_dbt_assets is not None else []

# Initialize DBT resource
dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    target_dir=DBT_TARGET_DIR,
    target="dev"
)

# Combine all assets
all_assets = [
    airbyte_sync_asset,  # Airbyte sync asset
    *dbt_assets,  # dbt assets
]

# Debug information
print("=== Asset Configuration ===")
print(f"Total assets loaded: {len(all_assets)}")
for asset in all_assets:
    print(f"- {asset.__name__ if hasattr(asset, '__name__') else str(asset)}")

# Definitions
defs = Definitions(
    assets=all_assets,
    jobs=[],  # Jobs are defined in schedules.py
    schedules=schedules,
    resources={
        "dbt": dbt_resource,
    },
)

print("=== Dagster Definitions Loaded Successfully ===")
