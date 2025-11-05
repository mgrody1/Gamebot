{{ config(materialized='view', schema='bronze') }}

select
    ingest_run_id,
    advantage_movement_id,
    version,
    version_season,
    season,
    castaway,
    castaway_id,
    advantage_id,
    sequence_id,
    day,
    episode,
    event,
    played_for,
    played_for_id,
    co_castaway_ids,
    joint_play,
    multi_target_play,
    success,
    votes_nullified,
    sog_id,
    source_dataset,
    ingested_at
from {{ source('bronze', 'advantage_movement') }};
