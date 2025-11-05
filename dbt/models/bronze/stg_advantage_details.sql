{{ config(materialized='view', schema='bronze') }}

select
    ingest_run_id,
    version,
    version_season,
    season,
    advantage_id,
    advantage_type,
    clue_details,
    location_found,
    conditions,
    source_dataset,
    ingested_at
from {{ source('bronze', 'advantage_details') }};
