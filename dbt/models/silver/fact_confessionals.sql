{{ config(materialized='table', schema='silver') }}

with source as (
    select
        confessional_id,
        version_season,
        episode,
        castaway_id,
        confessional_count,
        confessional_time,
        exp_count,
        exp_time
    from {{ source('bronze', 'confessionals') }}
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
    {{ dbt_utils.generate_surrogate_key(['source.confessional_id']) }} as confessional_fact_key,
    cm.castaway_key,
    em.season_key,
    em.episode_key,
    source.castaway_id,
    source.version_season,
    source.episode as episode_in_season,
    source.confessional_count,
    source.confessional_time,
    source.exp_count as expected_count,
    source.exp_time as expected_time,
    source.confessional_id as source_confessional_id,
    current_timestamp as created_at
from source
left join episode_map em
  on em.version_season = source.version_season
 and em.episode_in_season = source.episode
left join castaway_map cm on cm.castaway_id = source.castaway_id;
