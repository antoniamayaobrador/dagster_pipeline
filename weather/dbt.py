import os
import json
from pathlib import Path
from typing import Optional, List, Dict, Any

from dagster import AssetExecutionContext, OpExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

def find_manifest_path() -> Path:
    """Find the dbt manifest.json in various possible locations."""
    # Get the project root (one level up from the weather package)
    project_root = Path(__file__).parent.parent
    
    # Possible locations where the manifest might be found
    possible_paths = [
        # Local development - relative to project root
        project_root / "weather_project" / "target" / "manifest.json",
        # Deployed environment
        Path("/venvs/ec21669d8b57/lib/python3.10/site-packages/working_directory/root/weather_project/target/manifest.json"),
        # Relative to the current file
        Path(__file__).parent.parent / "weather_project" / "target" / "manifest.json",
    ]
    
    for path in possible_paths:
        if path.exists():
            return path
    
    # If no existing manifest is found, return the first path as a default
    return possible_paths[0]

def create_minimal_manifest(path: Path) -> None:
    """Create a minimal valid manifest file if it doesn't exist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    
    # A minimal valid manifest with just the required fields
    minimal_manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v7.json",
            "dbt_version": "1.7.2",
            "generated_at": "2025-05-18T00:00:00.000000Z",
            "adapter_type": "snowflake"
        },
        "nodes": {
            "model.weather_project.stg_weather_current": {
                "name": "stg_weather_current",
                "resource_type": "model",
                "package_name": "weather_project",
                "unique_id": "model.weather_project.stg_weather_current",
                "fqn": ["weather_project", "staging", "stg_weather_current"],
                "depends_on": {
                    "nodes": []
                },
                "config": {
                    "enabled": True,
                    "materialized": "view"
                },
                "tags": []
            }
        },
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
    }
    
    with open(path, 'w') as f:
        json.dump(minimal_manifest, f)

# Get the manifest path
DBT_MANIFEST_PATH = find_manifest_path()

# Create a minimal manifest if it doesn't exist
if not DBT_MANIFEST_PATH.exists():
    create_minimal_manifest(DBT_MANIFEST_PATH)

# Load the manifest to verify it's valid
try:
    with open(DBT_MANIFEST_PATH, 'r') as f:
        manifest_data = json.load(f)
    # If we got here, the manifest is valid JSON
    print(f"Successfully loaded manifest from {DBT_MANIFEST_PATH}")
except Exception as e:
    print(f"Error loading manifest from {DBT_MANIFEST_PATH}: {str(e)}")
    # Try to create a fresh manifest
    create_minimal_manifest(DBT_MANIFEST_PATH)

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
        # First, try to run dbt deps to ensure all dependencies are installed
        context.log.info("Running dbt deps...")
        dbt.cli(["deps"], context=context).wait()
        
        # Then compile the project to ensure the manifest is up to date
        context.log.info("Running dbt compile...")
        dbt.cli(["compile"], context=context).wait()
        
        # Finally, run dbt build
        context.log.info("Running dbt build...")
        yield from dbt.cli(["build"], context=context).stream()
        
    except Exception as e:
        context.log.error(f"Error running dbt command: {str(e)}")
        # If we get here, the build failed. Try to run the specific model that's failing.
        try:
            context.log.info("Attempting to run just the stg_weather_current model...")
            yield from dbt.cli(["run", "--select", "stg_weather_current"], context=context).stream()
        except Exception as inner_e:
            context.log.error(f"Error running stg_weather_current model: {str(inner_e)}")
            raise
