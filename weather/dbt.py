from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

# Import the project configuration
from .project import weather_project_project
from .constants import DBT_PROJECT_DIR, DBT_PROFILES_DIR

def get_manifest_path():
    """Safely get the manifest path, falling back to a default if not found"""
    try:
        manifest_path = weather_project_project.manifest_path
        if not manifest_path.exists():
            # Fall back to a path relative to the project directory
            fallback_path = Path(DBT_PROJECT_DIR) / "target/manifest.json"
            if fallback_path.exists():
                return fallback_path
        return manifest_path
    except Exception:
        return Path(DBT_PROJECT_DIR) / "target/manifest.json"

# Try to get the manifest path, but don't fail if it doesn't exist yet
try:
    manifest_path = get_manifest_path()
except Exception:
    manifest_path = None

def get_dbt_assets():
    if manifest_path and manifest_path.exists():
        @dbt_assets(manifest=manifest_path)
        def dbt_assets_fn(context: AssetExecutionContext, dbt: DbtCliResource):
            yield from dbt.cli(["build"], context=context).stream()
        return dbt_assets_fn
    else:
        # Return None if manifest doesn't exist yet
        # This allows the deployment to succeed, and you can generate the manifest later
        return None

# This will be None if manifest doesn't exist yet
weather_project_dbt_assets = get_dbt_assets()

