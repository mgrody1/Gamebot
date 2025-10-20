{{ config(materialized='table', schema='silver') }}

select
    {{ dbt_utils.generate_surrogate_key(['version_season', 'advantage_id']) }} as advantage_key,
    version_season,
    advantage_id,
    advantage_type,
    clue_details,
    location_found,
    conditions,
    current_timestamp as created_at,
    current_timestamp as updated_at
from {{ source('bronze', 'advantage_details') }};
