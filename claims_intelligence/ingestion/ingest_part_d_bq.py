import os

from dagster import MaterializeResult, asset
from dagster_gcp import BigQueryResource
from google.cloud.bigquery import (
    LoadJobConfig,
    PartitionRange,
    RangePartitioning,
    SchemaField,
    SourceFormat,
    Table,
    WriteDisposition,
)

from claims_intelligence.ingestion.cms_config import (
    PART_D_STAGING_SCHEMA,
    year_partitions,
)


@asset(partitions_def=year_partitions)
def part_d_bq_raw(context, bigquery: BigQueryResource, part_d_raw: str):
    # Get Envs
    gcp_project_id = os.getenv("GCP_PROJECT")
    dataset_id = os.getenv("GCP_RAW_DATASET")

    if not gcp_project_id or not dataset_id:
        raise ValueError("Env var GCP_PROJECT and GCP_RAW_DATASET must be set.")

    # Set parameters
    year = int(context.partition_key)
    staging_table_id = "staging_raw_data"
    final_raw_table = "raw_data"

    #   1. Get the Client
    with bigquery.get_client() as bq_client:
        #   2. create final table if not exists
        final_schema = PART_D_STAGING_SCHEMA + [
            SchemaField("data_year", "INT64", mode="REQUIRED")
        ]

        table = Table(
            f"{gcp_project_id}.{dataset_id}.{final_raw_table}", schema=final_schema
        )
        table.range_partitioning = RangePartitioning(
            field="data_year", range_=PartitionRange(start=2013, end=2030, interval=1)
        )
        bq_client.create_table(table, exists_ok=True)

        #   3. load GCS → staging table (WRITE_TRUNCATE)
        job_config = LoadJobConfig(
            schema=PART_D_STAGING_SCHEMA,
            skip_leading_rows=1,
            source_format=SourceFormat.CSV,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
        )

        load_job = bq_client.load_table_from_uri(
            part_d_raw,
            f"{gcp_project_id}.{dataset_id}.{staging_table_id}",
            job_config=job_config,
        )

        load_job.result()  # Waits for the job to complete.

        destination_table = bq_client.get_table(
            f"{gcp_project_id}.{dataset_id}.{staging_table_id}"
        )  # Make an API request.

        context.log.info("Loaded {} rows.".format(destination_table.num_rows))

        #   4. DELETE FROM final WHERE data_year = year
        delete_query = f"""
            DELETE FROM `{gcp_project_id}`.`{dataset_id}`.`{final_raw_table}` 
            WHERE data_year = {year}
        """

        bq_client.query(delete_query).result()

        #   5. INSERT INTO final SELECT *, year AS data_year FROM staging
        insert_query = f"""
            INSERT INTO `{gcp_project_id}`.`{dataset_id}`.`{final_raw_table}`
            SELECT *, {year} as data_year 
            FROM `{gcp_project_id}`.`{dataset_id}`.`{staging_table_id}`
        """

        bq_client.query(insert_query).result()

        #   6. delete staging table
        bq_client.delete_table(f"{gcp_project_id}.{dataset_id}.{staging_table_id}")

    #   7. return MaterializeResult
    return MaterializeResult(
        metadata={
            "year": year,
            "rows_loaded": destination_table.num_rows,
            "final_table": f"{gcp_project_id}.{dataset_id}.{final_raw_table}",
        }
    )
