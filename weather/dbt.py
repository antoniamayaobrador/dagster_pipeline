from pathlib import Path
import os
from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets

# Get the project root (where the .git directory is located)
project_root = Path(__file__).parent.parent

# Path to the dbt project (at the same level as the weather package)
dbt_project_path = project_root / "weather_project"

# Path to the manifest file
manifest_path = dbt_project_path / "target" / "manifest.json"

# Initialize dbt resources with environment variable fallbacks
dbt = DbtCliResource(
    project_dir=str(dbt_project_path),
    profiles_dir=str(dbt_project_path),
)

# Define dbt assets
@dbt_assets(manifest=str(manifest_path))
def weather_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt assets for the weather project."""
    # Log environment information for debugging
    context.log.info(f"Project dir: {dbt.project_dir}")
    context.log.info(f"Profiles dir: {dbt.profiles_dir}")
    
    # Run dbt build with the appropriate target
    target = os.getenv("DBT_TARGET", "dev")  # Default to 'dev' if not specified
    context.log.info(f"Running dbt build with target: {target}")
    
    yield from dbt.cli(["build", "--target", target], context=context).stream()
