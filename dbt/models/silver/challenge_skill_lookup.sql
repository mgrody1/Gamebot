{{ config(materialized='table', schema='silver') }}

with flags as (
    select
        version_season,
        challenge_id,
        balance,
        balance_ball,
        balance_beam,
        endurance,
        fire,
        food,
        knowledge,
        memory,
        mud,
        obstacle_blindfolded,
        obstacle_cargo_net,
        obstacle_chopping,
        obstacle_combination_lock,
        obstacle_digging,
        obstacle_knots,
        obstacle_padlocks,
        precision,
        precision_catch,
        precision_roll_ball,
        precision_slingshot,
        precision_throw_balls,
        precision_throw_coconuts,
        precision_throw_rings,
        precision_throw_sandbags,
        puzzle,
        puzzle_slide,
        puzzle_word,
        race,
        strength,
        turn_based,
        water,
        water_paddling,
        water_swim
    from {{ source('bronze', 'challenge_description') }}
),
unnested AS (
    select distinct skill
    from flags,
    lateral unnest(ARRAY[
        ('balance', balance),
        ('balance_ball', balance_ball),
        ('balance_beam', balance_beam),
        ('endurance', endurance),
        ('fire', fire),
        ('food', food),
        ('knowledge', knowledge),
        ('memory', memory),
        ('mud', mud),
        ('obstacle_blindfolded', obstacle_blindfolded),
        ('obstacle_cargo_net', obstacle_cargo_net),
        ('obstacle_chopping', obstacle_chopping),
        ('obstacle_combination_lock', obstacle_combination_lock),
        ('obstacle_digging', obstacle_digging),
        ('obstacle_knots', obstacle_knots),
        ('obstacle_padlocks', obstacle_padlocks),
        ('precision', precision),
        ('precision_catch', precision_catch),
        ('precision_roll_ball', precision_roll_ball),
        ('precision_slingshot', precision_slingshot),
        ('precision_throw_balls', precision_throw_balls),
        ('precision_throw_coconuts', precision_throw_coconuts),
        ('precision_throw_rings', precision_throw_rings),
        ('precision_throw_sandbags', precision_throw_sandbags),
        ('puzzle', puzzle),
        ('puzzle_slide', puzzle_slide),
        ('puzzle_word', puzzle_word),
        ('race', race),
        ('strength', strength),
        ('turn_based', turn_based),
        ('water', water),
        ('water_paddling', water_paddling),
        ('water_swim', water_swim)
    ]) AS skill_tuple(skill, active)
    WHERE COALESCE(skill_tuple.active, false)
)

select
    {{ dbt_utils.generate_surrogate_key(['skill']) }} as skill_key,
    skill as skill_name,
    null::text as skill_category,
    current_timestamp as created_at,
    current_timestamp as updated_at
from unnested;
