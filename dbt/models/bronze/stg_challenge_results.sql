{{ config(materialized='view', schema='bronze') }}

select
    ingest_run_id,
    challenge_results_id,
    version,
    version_season,
    season,
    episode,
    sog_id,
    challenge_id,
    challenge_type,
    outcome_type,
    result,
    result_notes,
    order_of_finish,
    castaway_id,
    castaway,
    tribe,
    tribe_status,
    chosen_for_reward,
    sit_out,
    source_dataset,
    ingested_at
from {{ source('bronze', 'challenge_results') }};
