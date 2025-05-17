from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import weather_project_project
from .constants import DBT_PROJECT_DIR, DBT_PROFILES_DIR



@dbt_assets(manifest=weather_project_project.manifest_path)
def weather_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

