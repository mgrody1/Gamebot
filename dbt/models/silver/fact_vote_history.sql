{{ config(materialized='table', schema='silver') }}

with source as (
    select
        vote_history_id,
        version_season,
        episode,
        castaway_id,
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
        tribe
    from {{ source('bronze', 'vote_history') }}
),
episode_map as (
    select version_season, episode_in_season, episode_key, season_key
    from {{ ref('dim_episode') }}
),
castaway_map as (
    select castaway_id, castaway_key
    from {{ ref('dim_castaway') }}
),
challenge_map as (
    select version_season, challenge_id, challenge_key
    from {{ ref('dim_challenge') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['source.vote_history_id']) }} as vote_fact_key,
    voter.castaway_key,
    em.season_key,
    em.episode_key,
    ch.challenge_key,
    source.castaway_id,
    target.castaway_id as target_castaway_id,
    eliminated.castaway_id as voted_out_castaway_id,
    source.version_season,
    source.episode as episode_in_season,
    source.immunity,
    source.vote,
    source.vote_event,
    source.vote_event_outcome,
    source.split_vote,
    source.nullified,
    source.tie,
    source.vote_order,
    source.sog_id,
    source.vote_history_id as source_vote_history_id,
    current_timestamp as created_at,
    source.tribe_status,
    source.tribe
from source
left join episode_map em
  on em.version_season = source.version_season
 and em.episode_in_season = source.episode
left join castaway_map voter on voter.castaway_id = source.castaway_id
left join castaway_map target on target.castaway_id = source.vote_id
left join castaway_map eliminated on eliminated.castaway_id = source.voted_out_id
left join challenge_map ch
  on ch.version_season = source.version_season
 and ch.challenge_id = source.challenge_id;
