from pathlib import Path
import os
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

# Carga de variables de entorno
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")

# Validaciones para facilitar el debug
if not DBT_PROJECT_DIR or not os.path.exists(DBT_PROJECT_DIR):
    raise ValueError(f"DBT_PROJECT_DIR no existe o no está definido: {DBT_PROJECT_DIR}")

if not DBT_PROFILES_DIR or not os.path.exists(DBT_PROFILES_DIR):
    raise ValueError(f"DBT_PROFILES_DIR no existe o no está definido: {DBT_PROFILES_DIR}")

# Path al manifest.json
manifest_path = Path(DBT_PROJECT_DIR) / "target" / "manifest.json"
if not manifest_path.exists():
    raise FileNotFoundError(f"manifest.json no encontrado en: {manifest_path}")

# Recurso de dbt
dbt = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
)

# Definición de assets con el manifest generado previamente
@dbt_assets(manifest=str(manifest_path))
def weather_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Ejecuta `dbt build` con el manifiesto precompilado."""
    target = os.getenv("DBT_TARGET", "dev")
    context.log.info(f"Ejecutando `dbt build` con target: {target}")
    yield from dbt.cli(["build", "--target", target], context=context).stream()
