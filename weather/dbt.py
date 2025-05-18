from pathlib import Path
from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets

# Initialize dbt resources
dbt = DbtCliResource(
    project_dir=str(Path(__file__).parent.parent / "weather_project"),
    profiles_dir=str(Path(__file__).parent.parent / "weather_project"),  # Point to the weather_project directory where profiles.yml is located
)

# Define dbt assets using the older pattern
@dbt_assets(manifest=str(Path(__file__).parent.parent / "weather_project" / "target" / "manifest.json"))
def weather_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt assets for the weather project."""
    yield from dbt.cli(["build"], context=context).stream()
