# Claims Intelligence

A data pipeline for ingesting and transforming CMS Medicare provider data using Dagster, Google Cloud Storage, and Bigquery.

## Goal
Ingest the Medicare Part D Prescriber data published by CMS and create a Gen AI endpoint that can answer questions about it.

## Tech Stack
- Orchestration: Dagster
- Storage & Compute: Google Cloud Storage and Bigquery
- Transformation: dbt (WIP)
- Environment: uv & python 3.13

## Current Progress
Can ingest the data from CMS directly into a BigQuery raw layer.  So the core extract and load framework is working.  

Still need to transform it via dbt.

## Current Architecture
```
CMS data.cms.gov API
    |
    V
part_d_raw (GCS)
- Fetches dataset metadata from CMS API
- Streams CSV directly to GCS (keep memory footprint low)
- Verifies SHA1 hash and byte count to ensure file downloaded correctly
- Skips download if file exists
    |
    V
part_d_bq_raw (BigQuery)
- Loads CSV from GCS into staging table
- Inserts into raw layers with data_year column added
- Cleans up Staging table automatically
- raw table is partitioned by data_year                                                                                                                  
```                   