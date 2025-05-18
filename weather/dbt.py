from pathlib import Path
import os
from dagster import AssetExecutionContext, EnvVar
from dagster_dbt import DbtCliResource, dbt_assets

# Get the directory of the current file
current_dir = Path(__file__).parent

# Define DBT project directories relative to this file
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", str(current_dir / "weather_project"))
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", str(current_dir / "weather_project"))

# Validation with helpful error messages
project_path = Path(DBT_PROJECT_DIR)
profiles_path = Path(DBT_PROFILES_DIR)

if not project_path.exists():
    raise ValueError(
        f"DBT_PROJECT_DIR does not exist: {DBT_PROJECT_DIR}\n"
        f"Current working directory: {os.getcwd()}\n"
        f"Directory contents: {os.listdir(project_path.parent) if project_path.parent.exists() else 'Parent dir does not exist'}"
    )

if not profiles_path.exists():
    raise ValueError(
        f"DBT_PROFILES_DIR does not exist: {DBT_PROFILES_DIR}\n"
        f"Current working directory: {os.getcwd()}\n"
        f"Directory contents: {os.listdir(profiles_path.parent) if profiles_path.parent.exists() else 'Parent dir does not exist'}"
    )

# Check for manifest.json
manifest_path = project_path / "target" / "manifest.json"
if not manifest_path.exists():
    # Try to find the manifest.json in parent directories (common in dbt projects)
    possible_manifest_paths = [
        project_path / "target" / "manifest.json",
        project_path / "dbt_packages" / "dbt_project" / "target" / "manifest.json",
        project_path.parent / "target" / "manifest.json",
    ]
    
    for path in possible_manifest_paths:
        if path.exists():
            manifest_path = path
            break
    else:
        raise FileNotFoundError(
            f"manifest.json not found in any of these locations:\n"
            f"- {project_path / 'target/manifest.json'}\n"
            f"- {project_path / 'dbt_packages/dbt_project/target/manifest.json'}\n"
            f"- {project_path.parent / 'target/manifest.json'}"
        )

# Recurso dbt
dbt = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
)

# Assets
@dbt_assets(manifest=str(manifest_path))
def weather_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    target = os.getenv("DBT_TARGET", "dev")
    context.log.info(f"Running dbt build with target: {target}")
    yield from dbt.cli(["build", "--target", target], context=context).stream()
