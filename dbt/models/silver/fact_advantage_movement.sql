{{ config(materialized='table', schema='silver') }}

with source as (
    select
        advantage_movement_id,
        version_season,
        castaway_id,
        played_for_id,
        advantage_id,
        sequence_id,
        day,
        episode,
        event,
        success,
        votes_nullified,
        sog_id
    from {{ source('bronze', 'advantage_movement') }}
),
episode_map as (
    select version_season, episode_in_season, episode_key, season_key
    from {{ ref('dim_episode') }}
),
castaway_map as (
    select castaway_id, castaway_key
    from {{ ref('dim_castaway') }}
),
advantage_map as (
    select version_season, advantage_id, advantage_key
    from {{ ref('dim_advantage') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['source.advantage_movement_id']) }} as advantage_fact_key,
    holder.castaway_key,
    target.castaway_key as target_castaway_key,
    em.season_key,
    em.episode_key,
    adv.advantage_key,
    source.castaway_id,
    source.played_for_id as target_castaway_id,
    source.version_season,
    source.sequence_id,
    source.advantage_id,
    source.day,
    source.episode as episode_in_season,
    source.event,
    source.success,
    source.votes_nullified,
    source.sog_id,
    source.advantage_movement_id as source_advantage_movement_id,
    current_timestamp as created_at
from source
left join episode_map em
  on em.version_season = source.version_season
 and em.episode_in_season = source.episode
left join castaway_map holder on holder.castaway_id = source.castaway_id
left join castaway_map target on target.castaway_id = source.played_for_id
left join advantage_map adv
  on adv.version_season = source.version_season
 and adv.advantage_id = source.advantage_id;
