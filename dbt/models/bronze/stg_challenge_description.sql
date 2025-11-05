{{ config(materialized='view', schema='bronze') }}

select
    ingest_run_id,
    version,
    version_season,
    season,
    episode,
    challenge_id,
    challenge_number,
    challenge_type,
    name,
    recurring_name,
    description,
    reward,
    additional_stipulation,
    source_dataset,
    ingested_at
from {{ source('bronze', 'challenge_description') }};
