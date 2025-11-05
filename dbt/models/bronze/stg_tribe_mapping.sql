{{ config(materialized='view', schema='bronze') }}

select
    ingest_run_id,
    tribe_map_id,
    version,
    version_season,
    season,
    episode,
    day,
    castaway_id,
    castaway,
    tribe,
    tribe_status,
    source_dataset,
    ingested_at
from {{ source('bronze', 'tribe_mapping') }};
