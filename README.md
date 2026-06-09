# Claims Intelligence

A data pipeline for ingesting and transforming CMS Medicare provider data using Dagster, Google Cloud Storage, and Bigquery.

## Goal
Ingest the Medicare Part D Prescriber data published by CMS and create a Gen AI endpoint that can answer questions about it.

## Tech Stack
- Orchestration: Dagster
- Storage & Compute: Google Cloud Storage and Bigquery
- Transformation: dbt
- Environment: uv & python 3.13

## Current Progress
Pulled data from CMS and have the medallion architecture with a simple star scheam, complete with surrogate keys and freshness testing. 

Need to add documentation to the data models via dbt and pull it into Bigquery.  Then add testing around the relationships to ensure integrity.

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
    |
    V
part_d_silver
- cleaned up names and data types from raw data
- added freshness monitoring
- added uniqueness test to primary key (data_year and prescriber NPI)  
    |
    V
Gold Layer
- One Fact Table and 2 Dimensions                                                                      
```                   