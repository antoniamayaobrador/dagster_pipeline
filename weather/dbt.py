from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

# Define the path to the dbt project directory
DBT_PROJECT_DIR = "weather_project"

# Path to the dbt manifest file
DBT_MANIFEST_PATH = Path(DBT_PROJECT_DIR) / "target/manifest.json"

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def weather_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt assets for the weather project.
    
    The manifest file is expected to be generated during the CI/CD process.
    """
    yield from dbt.cli(["build"], context=context).stream()

