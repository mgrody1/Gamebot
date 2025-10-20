{{ config(materialized='table', schema='silver') }}

with source as (
    select
        boot_mapping_id,
        version_season,
        castaway_id,
        episode,
        boot_mapping_order,
        n_boots,
        final_n,
        sog_id,
        tribe,
        tribe_status,
        game_status
    from {{ source('bronze', 'boot_mapping') }}
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
    {{ dbt_utils.generate_surrogate_key(['source.boot_mapping_id']) }} as boot_fact_key,
    cm.castaway_key,
    em.season_key,
    em.episode_key,
    source.castaway_id,
    source.version_season,
    source.episode as episode_in_season,
    source.boot_mapping_order,
    source.n_boots,
    source.final_n,
    source.sog_id,
    source.tribe,
    source.tribe_status,
    source.game_status,
    source.boot_mapping_id as source_boot_mapping_id,
    current_timestamp as created_at
from source
left join episode_map em
  on em.version_season = source.version_season
 and em.episode_in_season = source.episode
left join castaway_map cm on cm.castaway_id = source.castaway_id;
