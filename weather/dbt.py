from pathlib import Path
import os
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

# Use absolute path to avoid any path resolution issues
DBT_PROJECT_DIR = Path(__file__).parent.parent / "weather_project"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target/manifest.json"

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def weather_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # Log environment information
    context.log.info(f"Python working directory: {os.getcwd()}")
    context.log.info(f"Manifest path: {DBT_MANIFEST_PATH.absolute()}")
    context.log.info(f"Manifest exists: {DBT_MANIFEST_PATH.exists()}")
    
    # Verify manifest exists
    if not DBT_MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"dbt manifest not found at: {DBT_MANIFEST_PATH.absolute()}\n"
            "Make sure to run 'dbt compile' before deploying."
        )
    
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
    
    # Run dbt build
    yield from dbt.cli(["build"], context=context).stream()
