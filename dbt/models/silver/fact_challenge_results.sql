{{ config(materialized='table', schema='silver') }}

with source as (
    select
        challenge_results_id,
        version_season,
        castaway_id,
        challenge_id,
        episode,
        sog_id,
        challenge_type,
        outcome_type,
        result,
        result_notes,
        chosen_for_reward,
        sit_out,
        order_of_finish
    from {{ source('bronze', 'challenge_results') }}
),
challenge_map as (
    select version_season, challenge_id, challenge_key
    from {{ ref('dim_challenge') }}
),
episode_map as (
    select version_season, episode_in_season, episode_key, season_key
    from {{ ref('dim_episode') }}
),
castaway_map as (
    select castaway_id, castaway_key
    from {{ ref('dim_castaway') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['source.challenge_results_id']) }} as challenge_fact_key,
    cm.castaway_key,
    em.season_key,
    em.episode_key,
    ch.challenge_key,
    null::bigint as advantage_key,
    source.castaway_id,
    source.version_season,
    source.challenge_id,
    source.sog_id,
    source.outcome_type,
    source.result,
    source.result_notes,
    source.chosen_for_reward,
    source.sit_out,
    source.order_of_finish,
    source.challenge_results_id as source_challenge_result_id,
    current_timestamp as created_at
from source
left join challenge_map ch
  on ch.version_season = source.version_season
 and ch.challenge_id = source.challenge_id
left join episode_map em
  on em.version_season = source.version_season
 and em.episode_in_season = source.episode
left join castaway_map cm on cm.castaway_id = source.castaway_id;
