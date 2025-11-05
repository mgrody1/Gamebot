{{ config(materialized='view', schema='bronze') }}

select
    ingest_run_id,
    vote_history_id,
    version,
    version_season,
    season,
    episode,
    castaway_id,
    castaway,
    vote_id,
    voted_out_id,
    immunity,
    vote,
    vote_event,
    vote_event_outcome,
    split_vote,
    nullified,
    tie,
    vote_order,
    sog_id,
    challenge_id,
    tribe_status,
    tribe,
    source_dataset,
    ingested_at
from {{ source('bronze', 'vote_history') }};
