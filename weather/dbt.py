from pathlib import Path
import os
import sys
from dagster import AssetExecutionContext, EnvVar
from dagster_dbt import DbtCliResource, dbt_assets

def normalize_path(path_str):
    """Normalize path by removing any extra whitespace and resolving to absolute path."""
    if not path_str:
        return None
    return str(Path(path_str.strip()).resolve())

# Get the directory of the current file
current_dir = Path(__file__).parent.resolve()

# Define DBT project directories - handle potential newlines in environment variables
DBT_PROJECT_DIR = normalize_path(os.getenv("DBT_PROJECT_DIR")) or str(current_dir / "weather_project")
DBT_PROFILES_DIR = normalize_path(os.getenv("DBT_PROFILES_DIR")) or DBT_PROJECT_DIR

# Debug output
print(f"DBT_PROJECT_DIR: {DBT_PROJECT_DIR}", file=sys.stderr)
print(f"DBT_PROFILES_DIR: {DBT_PROFILES_DIR}", file=sys.stderr)
print(f"Current working directory: {os.getcwd()}", file=sys.stderr)
print(f"Directory contents: {os.listdir(os.path.dirname(DBT_PROJECT_DIR))}", file=sys.stderr)

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
    Path("/opt/dagster/app/weather_project/target/manifest.json"),
]

# Check each possible path
for path in possible_manifest_paths:
    try:
        if path.exists():
            manifest_path = path
            print(f"Found manifest at: {manifest_path}", file=sys.stderr)
            break
    except Exception as e:
        print(f"Error checking path {path}: {e}", file=sys.stderr)

if not manifest_path:
    # If we can't find the manifest, we'll try to run dbt compile to generate it
    try:
        import subprocess
        print("Running dbt deps...", file=sys.stderr)
        subprocess.run(
            ["dbt", "deps", "--project-dir", DBT_PROJECT_DIR], 
            check=True, 
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True
        )
        print("Running dbt compile...", file=sys.stderr)
        result = subprocess.run(
            ["dbt", "compile", "--project-dir", DBT_PROJECT_DIR], 
            check=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True
        )
        print(f"dbt compile output: {result.stdout}", file=sys.stderr)
        manifest_path = project_path / "target" / "manifest.json"
    except subprocess.CalledProcessError as e:
        error_msg = f"""
        Failed to run dbt commands.
        Command: {e.cmd}
        Return code: {e.returncode}
        Output: {e.output}
        Error: {e.stderr}
        """
        print(error_msg, file=sys.stderr)
        raise FileNotFoundError(
            f"Could not find or generate manifest.json. Tried these locations:\n"
            f"- {project_path / 'target/manifest.json'}\n"
            f"- {project_path / 'dbt_packages/dbt_project/target/manifest.json'}\n"
            f"- {project_path.parent / 'target/manifest.json'}\n"
            f"- /opt/dagster/app/weather/weather_project/target/manifest.json\n"
            f"- /opt/dagster/app/weather_project/target/manifest.json\n"
            f"Error: {str(e)}\n"
            f"Output: {getattr(e, 'output', '')}\n"
            f"Stderr: {getattr(e, 'stderr', '')}"
        ) from e

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
