{{ config(materialized='table', schema='silver') }}

with source as (
    select
        cd.version_season,
        cd.challenge_id,
        cd.episode,
        cd.challenge_number,
        cd.challenge_type,
        cd.name,
        cd.recurring_name,
        cd.description,
        cd.reward,
        cd.additional_stipulation
    from {{ source('bronze', 'challenge_description') }} cd
)

select
    {{ dbt_utils.generate_surrogate_key(['version_season', 'challenge_id']) }} as challenge_key,
    version_season,
    challenge_id,
    episode as episode_in_season,
    challenge_number,
    challenge_type,
    name,
    recurring_name,
    description,
    reward,
    additional_stipulation,
    current_timestamp as created_at,
    current_timestamp as updated_at
from source;
