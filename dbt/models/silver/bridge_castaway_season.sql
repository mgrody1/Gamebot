{{ config(materialized='table', schema='silver') }}

with source as (
    select
        c.version_season,
        c.castaway_id,
        c.original_tribe,
        c.result,
        c.place,
        c.jury_status,
        c.jury,
        c.finalist,
        c.winner,
        c.result_number,
        c.acknowledge,
        c.ack_look,
        c.ack_speak,
        c.ack_gesture,
        c.ack_smile,
        c.ack_quote,
        c.ack_score
    from {{ source('bronze', 'castaways') }} c
),
castaway_map as (
    select castaway_id, castaway_key
    from {{ ref('dim_castaway') }}
),
season_map as (
    select version_season, season_key
    from {{ ref('dim_season') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['source.version_season', 'source.castaway_id']) }} as castaway_season_key,
    cm.castaway_key,
    sm.season_key,
    source.castaway_id,
    source.version_season,
    source.original_tribe,
    source.result,
    source.place,
    source.jury_status,
    source.jury,
    source.finalist,
    source.winner,
    source.result_number,
    source.acknowledge,
    source.ack_look,
    source.ack_speak,
    source.ack_gesture,
    source.ack_smile,
    source.ack_quote,
    source.ack_score,
    current_timestamp as created_at,
    current_timestamp as updated_at
from source
left join castaway_map cm on cm.castaway_id = source.castaway_id
left join season_map sm on sm.version_season = source.version_season;
