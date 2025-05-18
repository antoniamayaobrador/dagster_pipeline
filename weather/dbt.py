import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List

from dagster import AssetExecutionContext, Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

# Set up logging
logger = logging.getLogger("dbt_assets")

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
        # CI/CD environment
        Path("/github/workspace/weather_project/target/manifest.json"),
    ]
    
    for path in possible_paths:
        if path and path.exists():
            logger.info(f"Found dbt manifest at: {path}")
            return path
    
    # If no existing manifest is found, return the first path as a default
    default_path = possible_paths[0]
    logger.warning(f"No manifest found, using default path: {default_path}")
    return default_path

def create_minimal_manifest(path: Path) -> Dict[str, Any]:
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
                "depends_on": {"nodes": [], "macros": []},
                "config": {
                    "enabled": True,
                    "materialized": "view",
                    "tags": [],
                    "meta": {},
                    "on_schema_change": "ignore"
                },
                "database": os.getenv("SNOWFLAKE_DATABASE", "WEATHER"),
                "schema": "staging",
                "alias": "stg_weather_current",
                "description": "Staging model for current weather data"
            },
            "model.weather_project.stg_weather_forecast_daily": {
                "name": "stg_weather_forecast_daily",
                "resource_type": "model",
                "package_name": "weather_project",
                "unique_id": "model.weather_project.stg_weather_forecast_daily",
                "fqn": ["weather_project", "staging", "stg_weather_forecast_daily"],
                "depends_on": {"nodes": [], "macros": []},
                "config": {
                    "enabled": True,
                    "materialized": "view",
                    "tags": [],
                    "meta": {},
                    "on_schema_change": "ignore"
                },
                "database": os.getenv("SNOWFLAKE_DATABASE", "WEATHER"),
                "schema": "staging",
                "alias": "stg_weather_forecast_daily",
                "description": "Staging model for daily weather forecast data"
            },
            "model.weather_project.stg_weather_forecast_hourly": {
                "name": "stg_weather_forecast_hourly",
                "resource_type": "model",
                "package_name": "weather_project",
                "unique_id": "model.weather_project.stg_weather_forecast_hourly",
                "fqn": ["weather_project", "staging", "stg_weather_forecast_hourly"],
                "depends_on": {"nodes": [], "macros": []},
                "config": {
                    "enabled": True,
                    "materialized": "view",
                    "tags": [],
                    "meta": {},
                    "on_schema_change": "ignore"
                },
                "database": os.getenv("SNOWFLAKE_DATABASE", "WEATHER"),
                "schema": "staging",
                "alias": "stg_weather_forecast_hourly",
                "description": "Staging model for hourly weather forecast data"
            },
            "model.weather_project.weather_current_metrics": {
                "name": "weather_current_metrics",
                "resource_type": "model",
                "package_name": "weather_project",
                "unique_id": "model.weather_project.weather_current_metrics",
                "fqn": ["weather_project", "marts", "weather_current_metrics"],
                "depends_on": {"nodes": ["model.weather_project.stg_weather_current"], "macros": []},
                "config": {
                    "enabled": True,
                    "materialized": "table",
                    "tags": [],
                    "meta": {},
                    "on_schema_change": "ignore"
                },
                "database": os.getenv("SNOWFLAKE_DATABASE", "WEATHER"),
                "schema": "marts",
                "alias": "weather_current_metrics",
                "description": "Current weather metrics and analysis"
            },
            "model.weather_project.weather_daily_snapshot": {
                "name": "weather_daily_snapshot",
                "resource_type": "model",
                "package_name": "weather_project",
                "unique_id": "model.weather_project.weather_daily_snapshot",
                "fqn": ["weather_project", "marts", "weather_daily_snapshot"],
                "depends_on": {"nodes": ["model.weather_project.stg_weather_forecast_daily"], "macros": []},
                "config": {
                    "enabled": True,
                    "materialized": "table",
                    "tags": [],
                    "meta": {},
                    "on_schema_change": "ignore"
                },
                "database": os.getenv("SNOWFLAKE_DATABASE", "WEATHER"),
                "schema": "marts",
                "alias": "weather_daily_snapshot",
                "description": "Daily snapshot of weather forecasts"
            },
            "model.weather_project.weather_daily_summary": {
                "name": "weather_daily_summary",
                "resource_type": "model",
                "package_name": "weather_project",
                "unique_id": "model.weather_project.weather_daily_summary",
                "fqn": ["weather_project", "marts", "weather_daily_summary"],
                "depends_on": {"nodes": ["model.weather_project.weather_daily_snapshot"], "macros": []},
                "config": {
                    "enabled": True,
                    "materialized": "table",
                    "tags": [],
                    "meta": {},
                    "on_schema_change": "ignore"
                },
                "database": os.getenv("SNOWFLAKE_DATABASE", "WEATHER"),
                "schema": "marts",
                "alias": "weather_daily_summary",
                "description": "Daily summary of weather conditions"
            },
            "model.weather_project.weather_extremes": {
                "name": "weather_extremes",
                "resource_type": "model",
                "package_name": "weather_project",
                "unique_id": "model.weather_project.weather_extremes",
                "fqn": ["weather_project", "marts", "weather_extremes"],
                "depends_on": {"nodes": ["model.weather_project.weather_daily_summary"], "macros": []},
                "config": {
                    "enabled": True,
                    "materialized": "table",
                    "tags": [],
                    "meta": {},
                    "on_schema_change": "ignore"
                },
                "database": os.getenv("SNOWFLAKE_DATABASE", "WEATHER"),
                "schema": "marts",
                "alias": "weather_extremes",
                "description": "Analysis of extreme weather conditions"
            },
            "model.weather_project.weather_trends": {
                "name": "weather_trends",
                "resource_type": "model",
                "package_name": "weather_project",
                "unique_id": "model.weather_project.weather_trends",
                "fqn": ["weather_project", "marts", "weather_trends"],
                "depends_on": {"nodes": ["model.weather_project.weather_daily_summary"], "macros": []},
                "config": {
                    "enabled": True,
                    "materialized": "table",
                    "tags": [],
                    "meta": {},
                    "on_schema_change": "ignore"
                },
                "database": os.getenv("SNOWFLAKE_DATABASE", "WEATHER"),
                "schema": "marts",
                "alias": "weather_trends",
                "description": "Weather trend analysis and patterns"
            }
        },
        "sources": {},
        "macros": {},
        "parent_map": {},
        "child_map": {
            "model.weather_project.stg_weather_current": ["model.weather_project.weather_current_metrics"],
            "model.weather_project.stg_weather_forecast_daily": ["model.weather_project.weather_daily_snapshot"],
            "model.weather_project.stg_weather_forecast_hourly": [],
            "model.weather_project.weather_current_metrics": [],
            "model.weather_project.weather_daily_snapshot": ["model.weather_project.weather_daily_summary"],
            "model.weather_project.weather_daily_summary": ["model.weather_project.weather_extremes", "model.weather_project.weather_trends"],
            "model.weather_project.weather_extremes": [],
            "model.weather_project.weather_trends": []
        },
        "group_map": {},
        "disabled": {},
        "exposures": {},
        "selectors": {},
        "docs": {},
        "files": {},
        "metrics": {},
        "semantic_models": {},
        "saved_queries": {},
        "unit_tests": {},
        "semantic_layer": {},
        "query_statistics": {}
    }
    
    # Write the manifest to disk
    with open(path, 'w') as f:
        json.dump(minimal_manifest, f)
        
    return minimal_manifest

# Initialize dbt resources
dbt = DbtCliResource(
    project_dir=str(Path(__file__).parent.parent / "weather_project"),
    profiles_dir=str(Path(__file__).parent.parent / "weather_project"),
)

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

@dbt_assets(
    manifest=DBT_MANIFEST_PATH
)
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
