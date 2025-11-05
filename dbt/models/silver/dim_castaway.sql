{{ config(materialized='table', schema='silver') }}

with source as (
    select
        castaway_id,
        full_name,
        full_name_detailed,
        castaway,
        last_name,
        collar,
        date_of_birth,
        date_of_death,
        gender,
        african as is_african,
        asian as is_asian,
        latin_american as is_latin_american,
        native_american as is_native_american,
        bipoc as is_bipoc,
        lgbt as is_lgbt,
        personality_type,
        occupation,
        three_words,
        hobbies,
        pet_peeves,
        race,
        ethnicity
    from {{ ref('stg_castaway_details') }}
)

select
    castaway_id,
    full_name,
    full_name_detailed,
    castaway,
    last_name,
    gender,
    date_of_birth,
    date_of_death,
    collar,
    occupation,
    personality_type,
    three_words,
    hobbies,
    pet_peeves,
    race,
    ethnicity,
    is_african,
    is_asian,
    is_latin_american,
    is_native_american,
    is_bipoc,
    is_lgbt,
    current_timestamp as created_at,
    current_timestamp as updated_at
from source;
