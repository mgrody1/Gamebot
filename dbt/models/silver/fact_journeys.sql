-- Silver fact table for journey events (vote forfeits, rewards, etc.).
{{ config(materialized='table', schema='silver') }}

with source as (
    select
        version,
        version_season,
        season,
        castaway_id,
        castaway,
        episode,
        sog_id,
        reward,
        lost_vote,
        season_name,
        game_played,
        chose_to_play,
        event,
        source_dataset
    from {{ ref('stg_journeys') }}
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
    source.version,
    source.season,
    source.castaway_id,
    source.castaway as castaway_name,
    source.version_season,
    source.episode as episode_in_season,
    source.sog_id,
    source.reward,
    source.lost_vote,
    source.season_name,
    source.game_played,
    source.chose_to_play,
    source.event,
    source.source_dataset,
    current_timestamp as created_at
from source
left join castaway_map cm on cm.castaway_id = source.castaway_id
left join season_map sm on sm.version_season = source.version_season
left join episode_map em
  on em.version_season = source.version_season
 and em.episode_in_season = source.episode;
