{{ config(materialized='view', schema='bronze') }}

select
    ingest_run_id,
    castaway_id,
    full_name,
    full_name_detailed,
    castaway,
    last_name,
    collar,
    date_of_birth,
    date_of_death,
    gender,
    african,
    asian,
    latin_american,
    native_american,
    bipoc,
    lgbt,
    personality_type,
    occupation,
    three_words,
    hobbies,
    pet_peeves,
    race,
    ethnicity,
    source_dataset,
    ingested_at
from {{ source('bronze', 'castaway_details') }};
