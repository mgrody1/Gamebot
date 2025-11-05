{{ config(materialized='table', schema='silver') }}

with source as (
    select
        version_season,
        challenge_id,
        episode,
        challenge_number,
        challenge_type,
        name,
        recurring_name,
        description,
        reward,
        additional_stipulation
    from {{ ref('stg_challenge_description') }}
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
