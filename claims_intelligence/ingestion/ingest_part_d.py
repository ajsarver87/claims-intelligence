import hashlib

import httpx
from dagster import asset
from dagster_gcp import GCSResource

from claims_intelligence.ingestion.cms_config import PART_D_DATASETS, year_partitions


@asset(partitions_def=year_partitions)
def part_d_raw(context, gcs: GCSResource):
    year = int(context.partition_key)
    dataset_id = PART_D_DATASETS[year]

    meta_url = (
        f"https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data-viewer?size=0"
    )
    meta = httpx.get(meta_url).raise_for_status().json()["meta"]

    file_url = "https://data.cms.gov" + meta["data_file_url"]
    file_name = meta["data_file_name"]
    file_sha1 = meta["data_file_meta_data"]["csvFileSHA1"]
    file_size = meta["data_file_meta_data"]["csvFileSize"]
    file_row_cnt = meta["total_rows"]

    google_bucket_name = "cms-prescriber-data"
    destination_blob_name = file_name

    storage_client = gcs.get_client()
    bucket = storage_client.bucket(google_bucket_name)
    blob = bucket.blob(destination_blob_name)

    gcs_uri = f"gs://{google_bucket_name}/{destination_blob_name}"

    if blob.exists():
        context.log.info(f"File {destination_blob_name} already exists in GCS.")
        context.add_output_metadata(
            {
                "file_size_bytes": file_size,
                "file_sha1": file_sha1,
                "total_rows": file_row_cnt,
                "file_url": file_url,
                "file_name": file_name,
                "gcs_uri": gcs_uri,
                "skipped": True,
            }
        )
        return gcs_uri

    hasher = hashlib.sha1()
    bytes_written = 0

    with httpx.stream("GET", file_url) as r:
        r.raise_for_status()
        with blob.open("wb", chunk_size=8 * 1024 * 1024) as f:
            for chunk in r.iter_bytes(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
                hasher.update(chunk)
                bytes_written += len(chunk)
                pct = bytes_written / file_size * 100
                context.log.info(
                    f"{pct:.1f}% ({bytes_written / 1024**2:.1f} MB)",
                )

    if file_sha1 != hasher.hexdigest():
        raise ValueError("HASH DOES NOT MATCH!")

    if file_size != bytes_written:
        raise ValueError("SIZE DOES NOT MATCH!")

    context.add_output_metadata(
        {
            "file_size_bytes": bytes_written,
            "file_sha1": file_sha1,
            "total_rows": file_row_cnt,
            "file_url": file_url,
            "file_name": file_name,
            "gcs_uri": gcs_uri,
            "skipped": False,
        }
    )

    return gcs_uri
