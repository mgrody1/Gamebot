{{ config(materialized='view', schema='bronze') }}

select
    ingest_run_id,
    journey_id,
    version,
    version_season,
    season,
    episode,
    sog_id,
    castaway_id,
    castaway,
    reward,
    lost_vote,
    season_name,
    game_played,
    chose_to_play,
    event,
    source_dataset,
    ingested_at
from {{ source('bronze', 'journeys') }};
