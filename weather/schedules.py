from dagster import ScheduleDefinition, define_asset_job, in_process_executor, AssetSelection, AssetKey
from typing import List, Union
from .constants import WEATHER_SCHEDULE_CRON, EXECUTION_TIMEZONE
from .airbyte import airbyte_sync_asset
from .dbt import weather_project_dbt_assets

# Create a job that runs only the Airbyte sync
airbyte_sync_job = define_asset_job(
    name="airbyte_sync_job",
    description="Job that only runs the Airbyte sync",
    selection=["airbyte_sync_asset"]
)

# Create a combined job that runs Airbyte sync followed by all DBT models
combined_job = define_asset_job(
    name="combined_weather_job",
    description="Job that runs Airbyte sync followed by all DBT models",
    selection=AssetSelection.keys(("airbyte", "airbyte_sync_asset")) | AssetSelection.assets(*[
        asset for asset in weather_project_dbt_assets.keys
        if not asset.path[0].startswith("source_")
    ]),
    executor_def=in_process_executor
)

# Define the schedule to run the combined job
daily_weather_schedule = ScheduleDefinition(
    job=combined_job,
    cron_schedule=WEATHER_SCHEDULE_CRON,
    execution_timezone=EXECUTION_TIMEZONE,
    name="daily_weather_schedule"
)

# Export schedules
schedules = [daily_weather_schedule]