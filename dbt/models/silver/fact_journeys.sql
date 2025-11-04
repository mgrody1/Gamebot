-- Silver fact table for journey events (vote forfeits, rewards, etc.).
{{ config(materialized='table', schema='silver') }}

with source as (
    select
        version_season,
        castaway_id,
        episode,
        sog_id,
        reward,
        lost_vote,
        season_name,
        game_played,
        chose_to_play,
        event
    from {{ source('bronze', 'journeys') }}
),
castaway_map as (
    select castaway_id, castaway_key
    from {{ ref('dim_castaway') }}
),
season_map as (
    select version_season, season_key
    from {{ ref('dim_season') }}
),
episode_map as (
    select version_season, episode_in_season, episode_key
    from {{ ref('dim_episode') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['source.version_season', 'source.castaway_id', 'source.sog_id']) }} as journey_fact_key,
    cm.castaway_key,
    sm.season_key,
    em.episode_key,
    source.castaway_id,
    source.version_season,
    source.episode as episode_in_season,
    source.sog_id,
    source.reward,
    source.lost_vote,
    source.season_name,
    source.game_played,
    source.chose_to_play,
    source.event,
    current_timestamp as created_at
from source
left join castaway_map cm on cm.castaway_id = source.castaway_id
left join season_map sm on sm.version_season = source.version_season
left join episode_map em
  on em.version_season = source.version_season
 and em.episode_in_season = source.episode;
