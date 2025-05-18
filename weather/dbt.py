import os
from pathlib import Path
from typing import Optional

from dagster import AssetExecutionContext, OpExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

def find_manifest_path() -> Path:
    """Find the dbt manifest.json in various possible locations."""
    # Possible locations where the manifest might be found
    possible_paths = [
        # Local development
        Path("weather_project/target/manifest.json"),
        # Deployed environment
        Path("/venvs/ec21669d8b57/lib/python3.10/site-packages/working_directory/root/weather_project/target/manifest.json"),
        # Relative to the current file
        (Path(__file__).parent.parent / "weather_project/target/manifest.json").resolve(),
    ]
    
    for path in possible_paths:
        if path.exists():
            return path
    
    # If no existing manifest is found, return the first path as a default
    return possible_paths[0]

# Get the manifest path
DBT_MANIFEST_PATH = find_manifest_path()

# Create a minimal manifest if it doesn't exist
if not DBT_MANIFEST_PATH.exists():
    DBT_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    DBT_MANIFEST_PATH.write_text("""{
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
            "dbt_version": "1.7.2",
            "generated_at": "2025-05-18T00:00:00.000000Z",
            "adapter_type": "snowflake"
        },
        "nodes": {},
        "sources": {},
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "parent_map": {},
        "child_map": {},
        "group_map": {},
        "disabled": {},
        "state": {}
    }""")

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def weather_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """dbt assets for the weather project."""
    # Log environment information
    context.log.info(f"Python working directory: {os.getcwd()}")
    context.log.info(f"Manifest path: {DBT_MANIFEST_PATH.absolute()}")
    context.log.info(f"Manifest exists: {DBT_MANIFEST_PATH.exists()}")
    
    # Verify required environment variables
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_WAREHOUSE'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    try:
        # Try to run dbt build
        yield from dbt.cli(["build"], context=context).stream()
    except Exception as e:
        context.log.error(f"Error running dbt build: {str(e)}")
        raise
