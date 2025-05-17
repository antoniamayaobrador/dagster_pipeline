# dbt_weather/weather/definitions.py

import os
from dagster import Definitions
from dagster_dbt import DbtCliResource

# Importaciones
from weather.airbyte import airbyte_sync_asset
from weather.dbt import weather_project_dbt_assets
from weather.schedules import schedules  # Only import schedules
from weather.constants import WEATHER_SCHEDULE_CRON, EXECUTION_TIMEZONE
from dagster import file_relative_path

# Ruta del proyecto DBT
weather_project_path = file_relative_path(__file__, "../weather_project")


# Logs para depuración
print(f">>> DBT PROJECT DIR: {weather_project_path}")
print(">>> EXISTS:", os.path.exists(weather_project_path))
print(">>> HAS dbt_project.yml:", os.path.exists(os.path.join(weather_project_path, "dbt_project.yml")))

# Definitions
defs = Definitions(
    assets=[
        weather_project_dbt_assets,
        airbyte_sync_asset,
    ],
    jobs=[],  # No definimos jobs aquí porque están en schedules.py
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=weather_project_path),
    },
)
