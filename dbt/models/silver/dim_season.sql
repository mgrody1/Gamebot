{{ config(materialized='table', schema='silver') }}

with source as (
    select
        version,
        version_season,
        season_name,
        season as season_number,
        location,
        country,
        tribe_setup,
        n_cast as cast_size,
        n_tribes as tribe_count,
        n_finalists as finalist_count,
        n_jury as jury_count,
        premiered,
        ended,
        filming_started,
        filming_ended,
        winner_id as winner_castaway_id,
        viewers_reunion,
        viewers_premiere,
        viewers_finale,
        viewers_mean,
        rank
    from {{ source('bronze', 'season_summary') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['version', 'version_season']) }} as season_key,
    version,
    version_season,
    season_name,
    season_number,
    location,
    country,
    tribe_setup,
    cast_size,
    tribe_count,
    finalist_count,
    jury_count,
    premiered,
    ended,
    filming_started,
    filming_ended,
    winner_castaway_id,
    viewers_reunion,
    viewers_premiere,
    viewers_finale,
    viewers_mean,
    rank,
    current_timestamp as created_at,
    current_timestamp as updated_at
from source;
