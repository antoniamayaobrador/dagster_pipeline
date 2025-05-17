from pathlib import Path
from dagster_dbt import DbtProject

# Get the base directory
base_dir = Path(__file__).parent.parent
project_dir = base_dir / "weather_project"

# Ensure the target directory exists
target_dir = project_dir / "target"
target_dir.mkdir(exist_ok=True)

weather_project_project = DbtProject(
    project_dir=project_dir,
    profiles_dir=project_dir,
    target_path=target_dir
)

# This will create the target directory if it doesn't exist
weather_project_project.prepare_if_dev()
