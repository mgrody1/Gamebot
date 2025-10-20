{{ config(materialized='table', schema='silver') }}

with source as (
    select
        e.version,
        e.version_season,
        e.episode as episode_in_season,
        e.episode_number_overall,
        e.episode_title,
        e.episode_label,
        e.episode_date,
        e.episode_length,
        e.viewers,
        e.imdb_rating,
        e.n_ratings
    from {{ source('bronze', 'episodes') }} e
),
season_map as (
    select version_season, season_key
    from {{ ref('dim_season') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['source.version_season', 'source.episode_in_season']) }} as episode_key,
    sm.season_key,
    source.version_season,
    source.episode_in_season,
    source.episode_number_overall,
    source.episode_title,
    source.episode_label,
    source.episode_date,
    source.episode_length,
    source.viewers,
    source.imdb_rating,
    source.n_ratings,
    current_timestamp as created_at,
    current_timestamp as updated_at
from source
left join season_map sm on sm.version_season = source.version_season;
