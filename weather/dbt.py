from pathlib import Path
import os
from dagster import AssetExecutionContext, EnvVar
from dagster_dbt import DbtCliResource, dbt_assets

# Get the directory of the current file
current_dir = Path(__file__).parent

# Define DBT project directories relative to this file
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", str(current_dir / "weather_project"))
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", str(current_dir / "weather_project"))

# Set environment variables for dbt
os.environ["DBT_PROJECT_DIR"] = DBT_PROJECT_DIR
os.environ["DBT_PROFILES_DIR"] = DBT_PROFILES_DIR

# Try to find the manifest.json
project_path = Path(DBT_PROJECT_DIR)
manifest_path = None

# Common locations for manifest.json
possible_manifest_paths = [
    project_path / "target" / "manifest.json",
    project_path / "dbt_packages" / "dbt_project" / "target" / "manifest.json",
    project_path.parent / "target" / "manifest.json",
    Path("/opt/dagster/app/weather/weather_project/target/manifest.json"),
]

for path in possible_manifest_paths:
    if path.exists():
        manifest_path = path
        break

if not manifest_path:
    # If we can't find the manifest, we'll try to run dbt compile to generate it
    try:
        import subprocess
        subprocess.run(["dbt", "deps", "--project-dir", DBT_PROJECT_DIR], check=True)
        subprocess.run(["dbt", "compile", "--project-dir", DBT_PROJECT_DIR], check=True)
        manifest_path = project_path / "target" / "manifest.json"
    except Exception as e:
        raise FileNotFoundError(
            f"Could not find or generate manifest.json. Tried these locations:\n"
            f"- {project_path / 'target/manifest.json'}\n"
            f"- {project_path / 'dbt_packages/dbt_project/target/manifest.json'}\n"
            f"- {project_path.parent / 'target/manifest.json'}\n"
            f"- /opt/dagster/app/weather/weather_project/target/manifest.json\n"
            f"Error: {str(e)}"
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
