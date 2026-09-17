{{ config(materialized='table') }}

-- Castaway profile: demographics and season background, one row per castaway-season.
SELECT DISTINCT
    c.castaway_id,
    c.version_season,
    c.version,
    c.full_name,
    c.season,
    c.castaway,
    c.age,
    c.city,
    c.state,
    cd.personality_type,
    cd.occupation,
    cd.gender,
    cd.african,
    cd.asian,
    cd.latin_american,
    cd.native_american,
    cd.bipoc,
    cd.lgbt,
    cd.race,
    cd.ethnicity,
    ss.location as season_location,
    ss.country as season_country,
    ss.tribe_setup,
    ss.full_name as season_name,
    ss.viewers_premiere,
    ss.viewers_finale,
    ss.viewers_reunion,
    ss.viewers_mean as season_avg_viewers
FROM {{ source('bronze', 'castaways') }} c
LEFT JOIN {{ source('bronze', 'castaway_details') }} cd
    ON c.castaway_id = cd.castaway_id
LEFT JOIN {{ source('bronze', 'season_summary') }} ss
    ON c.version_season = ss.version_season
