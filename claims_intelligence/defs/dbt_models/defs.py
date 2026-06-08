from pathlib import Path
from dagster_dbt import DbtProject, DbtCliResource, dbt_assets
import dagster as dg

dbt_project = DbtProject(
    project_dir=Path(__file__).parent.parent.parent,
    profiles_dir=Path.home() / ".dbt",
)
dbt_project.prepare_if_dev()

@dbt_assets(manifest=dbt_project.manifest_path)
def claims_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

defs = dg.Definitions(
    assets=[claims_dbt_assets],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
