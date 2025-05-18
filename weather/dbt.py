from pathlib import Path
import os
from dagster import AssetExecutionContext, EnvVar
from dagster_dbt import DbtCliResource, dbt_assets

# Usa Entorno en cloud, pero fallback en local
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/Users/toniamayaobrador/dbt_weather_dagster/weather/weather_project")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/Users/toniamayaobrador/dbt_weather_dagster/weather/weather_project")

# Validación para debugging
if not Path(DBT_PROJECT_DIR).exists():
    raise ValueError(f"DBT_PROJECT_DIR no existe o no está definido: {DBT_PROJECT_DIR}")
if not Path(DBT_PROFILES_DIR).exists():
    raise ValueError(f"DBT_PROFILES_DIR no existe o no está definido: {DBT_PROFILES_DIR}")

manifest_path = Path(DBT_PROJECT_DIR) / "target" / "manifest.json"
if not manifest_path.exists():
    raise FileNotFoundError(f"manifest.json no encontrado en: {manifest_path}")

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
