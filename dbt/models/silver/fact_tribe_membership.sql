{{ config(materialized='table', schema='silver') }}

with source as (
    select
        tribe_map_id,
        version_season,
        castaway_id,
        episode,
        day,
        tribe,
        tribe_status
    from {{ source('bronze', 'tribe_mapping') }}
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
    {{ dbt_utils.generate_surrogate_key(['source.tribe_map_id']) }} as tribe_fact_key,
    cm.castaway_key,
    em.season_key,
    em.episode_key,
    source.castaway_id,
    source.version_season,
    source.episode as episode_in_season,
    source.day,
    source.tribe,
    source.tribe_status,
    source.tribe_map_id as source_tribe_mapping_id,
    current_timestamp as created_at
from source
left join episode_map em
  on em.version_season = source.version_season
 and em.episode_in_season = source.episode
left join castaway_map cm on cm.castaway_id = source.castaway_id;
