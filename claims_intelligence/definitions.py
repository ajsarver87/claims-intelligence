from pathlib import Path

import dagster as dg
from dagster_gcp import BigQueryResource, GCSResource

defs = dg.Definitions.merge(
    dg.Definitions(assets=[]),
    dg.load_from_defs_folder(project_root=Path(__file__).parent),
    dg.Definitions(
        resources={
            "gcs": GCSResource(project=dg.EnvVar("GCP_PROJECT")),
            "bigquery": BigQueryResource(project=dg.EnvVar("GCP_PROJECT")),
        }
    ),
)
