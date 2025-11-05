{{ config(materialized='table', schema='silver') }}

with source as (
    select
        c.version,
        c.version_season,
        c.season,
        c.castaway_id,
        c.original_tribe,
        c.result,
        c.place,
        c.jury_status,
        c.jury,
        c.finalist,
        c.winner,
        c.acknowledge,
        c.ack_look,
        c.ack_speak,
        c.ack_gesture,
        c.ack_smile,
        c.ack_quote,
        c.ack_score,
        c.age,
        c.city,
        c.state,
        c.episode,
        c.day,
        c.castaways_order,
        c.source_dataset,
        count(*) over (partition by c.castaway_id) as seasons_played,
        row_number() over (partition by c.castaway_id order by c.version_season) as season_sequence
    from {{ ref('stg_castaways') }} c
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
    source.version,
    source.season,
    source.original_tribe,
    source.result,
    source.place,
    source.jury_status,
    source.jury,
    source.finalist,
    source.winner,
    source.age,
    source.city,
    source.state,
    source.episode as episode_departed,
    source.day as day_departed,
    source.castaways_order,
    source.acknowledge,
    source.ack_look,
    source.ack_speak,
    source.ack_gesture,
    source.ack_smile,
    source.ack_quote,
    source.ack_score,
    source.seasons_played,
    source.season_sequence,
    (source.seasons_played > 1)::boolean as is_returnee,
    source.source_dataset,
    current_timestamp as created_at,
    current_timestamp as updated_at
from source
left join castaway_map cm on cm.castaway_id = source.castaway_id
left join season_map sm on sm.version_season = source.version_season;
