from pathlib import Path
from dagster_dbt import load_assets_from_dbt_project

weather_project_dbt_assets = load_assets_from_dbt_project(
    project_dir=Path(__file__).joinpath("../../weather_project").resolve(),
    profiles_dir=Path(__file__).joinpath("../../weather_project").resolve(),
)
