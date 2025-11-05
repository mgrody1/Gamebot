{{ config(materialized='table', schema='silver') }}

-- Challenge performance and skill analysis
with challenge_base as (
    select
        cr.castaway_id,
        cr.version_season,
        cr.episode,
        cr.challenge_id,
        cd.challenge_type,
        cd.name as challenge_name,
        cd.recurring_name,
        lower(cr.result) as result_lower,
        case when lower(cr.result) like '%win%' then 1 else 0 end as won_flag,
        cr.chosen_for_reward,
        cr.sit_out,
        cr.order_of_finish,
        cr.team,
        cr.tribe_status,
        -- Challenge skill flags from bronze.challenge_description
        cd.balance,
        cd.balance_ball,
        cd.balance_beam,
        cd.endurance,
        cd.fire,
        cd.food,
        cd.knowledge,
        cd.memory,
        cd.mud,
        cd.obstacle_blindfolded,
        cd.obstacle_cargo_net,
        cd.obstacle_chopping,
        cd.obstacle_combination_lock,
        cd.obstacle_digging,
        cd.obstacle_knots,
        cd.obstacle_padlocks,
        cd.precision,
        cd.precision_catch,
        cd.precision_roll_ball,
        cd.precision_slingshot,
        cd.precision_throw_balls,
        cd.precision_throw_coconuts,
        cd.precision_throw_rings,
        cd.precision_throw_sandbags,
        cd.puzzle,
        cd.puzzle_slide,
        cd.puzzle_word,
        cd.race,
        cd.strength,
        cd.turn_based,
        cd.water,
        cd.water_paddling,
        cd.water_swim
    from {{ source('bronze', 'challenge_results') }} cr
    join {{ source('bronze', 'challenge_description') }} cd
        on cr.version_season = cd.version_season
        and cr.challenge_id = cd.challenge_id
)

select
    {{ dbt_utils.generate_surrogate_key(['castaway_id', 'version_season', 'challenge_id']) }} as challenge_performance_key,
    castaway_id,
    version_season,
    episode,
    challenge_id,
    challenge_type,
    challenge_name,
    recurring_name,
    result_lower as result,
    won_flag,
    chosen_for_reward,
    sit_out,
    order_of_finish,
    team,
    tribe_status,
    case when tribe_status ilike '%merge%' then 'post_merge' else 'pre_merge' end as merge_phase,
    case when challenge_type ilike '%individual%' then 'individual'
         when challenge_type ilike '%tribe%' or challenge_type ilike '%team%' then 'team'
         else 'other' end as challenge_format,
    -- Skill performance tracking
    case when won_flag = 1 and balance = true then 1 else 0 end as balance_win,
    case when won_flag = 1 and endurance = true then 1 else 0 end as endurance_win,
    case when won_flag = 1 and knowledge = true then 1 else 0 end as knowledge_win,
    case when won_flag = 1 and memory = true then 1 else 0 end as memory_win,
    case when won_flag = 1 and precision = true then 1 else 0 end as precision_win,
    case when won_flag = 1 and puzzle = true then 1 else 0 end as puzzle_win,
    case when won_flag = 1 and race = true then 1 else 0 end as race_win,
    case when won_flag = 1 and strength = true then 1 else 0 end as strength_win,
    case when won_flag = 1 and water = true then 1 else 0 end as water_win,
    -- Skill participation tracking (regardless of outcome)
    case when balance = true then 1 else 0 end as balance_participated,
    case when endurance = true then 1 else 0 end as endurance_participated,
    case when knowledge = true then 1 else 0 end as knowledge_participated,
    case when memory = true then 1 else 0 end as memory_participated,
    case when precision = true then 1 else 0 end as precision_participated,
    case when puzzle = true then 1 else 0 end as puzzle_participated,
    case when race = true then 1 else 0 end as race_participated,
    case when strength = true then 1 else 0 end as strength_participated,
    case when water = true then 1 else 0 end as water_participated,
    current_timestamp as created_at
from challenge_base
