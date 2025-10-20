{{ config(materialized='table', schema='silver') }}

with source as (
    select
        jury_vote_id,
        version_season,
        castaway_id,
        finalist_id,
        vote
    from {{ source('bronze', 'jury_votes') }}
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
    {{ dbt_utils.generate_surrogate_key(['source.jury_vote_id']) }} as jury_fact_key,
    juror.castaway_key as juror_castaway_key,
    finalist.castaway_key as finalist_castaway_key,
    sm.season_key,
    source.castaway_id as juror_castaway_id,
    source.finalist_id as finalist_castaway_id,
    source.version_season,
    source.vote,
    source.jury_vote_id as source_jury_vote_id,
    current_timestamp as created_at
from source
left join castaway_map juror on juror.castaway_id = source.castaway_id
left join castaway_map finalist on finalist.castaway_id = source.finalist_id
left join season_map sm on sm.version_season = source.version_season;
