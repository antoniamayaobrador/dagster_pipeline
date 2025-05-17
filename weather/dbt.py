import os
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

def get_manifest_path():
    """Resolve the path to the dbt manifest file.
    
    Tries multiple possible locations to find the manifest file:
    1. In the dbt project's target directory (local development)
    2. In the working directory (deployed environment)
    """
    # Possible locations for the manifest file
    possible_paths = [
        # Local development path
        Path("weather_project/target/manifest.json"),
        # Deployed environment path (relative to working directory)
        Path("root/weather_project/target/manifest.json"),
        # Absolute path in deployed environment
        Path("/venvs/ddc19fe0936c/lib/python3.10/site-packages/working_directory/root/weather_project/target/manifest.json")
    ]
    
    # Try each possible path
    for path in possible_paths:
        if path.exists():
            return path
    
    # If no manifest is found, return the first path as a fallback
    return possible_paths[0]

# Get the manifest path
DBT_MANIFEST_PATH = get_manifest_path()

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def weather_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt assets for the weather project.
    
    The manifest file is expected to be generated during the CI/CD process.
    """
    # Log the manifest path being used for debugging
    context.log.info(f"Using dbt manifest at: {DBT_MANIFEST_PATH}")
    if not DBT_MANIFEST_PATH.exists():
        context.log.warning(f"dbt manifest not found at: {DBT_MANIFEST_PATH}")
    
    yield from dbt.cli(["build"], context=context).stream()

