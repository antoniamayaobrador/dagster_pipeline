from pathlib import Path

# Get the base directory
base_dir = Path(__file__).parent.parent
project_dir = base_dir / "weather_project"

# Ensure the target directory exists
target_dir = project_dir / "target"
target_dir.mkdir(exist_ok=True, parents=True)

# This is a placeholder for the project configuration
weather_project_config = {
    "project_dir": str(project_dir),
    "profiles_dir": str(project_dir),
    "target_dir": str(target_dir)
}
