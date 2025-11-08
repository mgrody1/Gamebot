{{ config(materialized='table', schema='gold') }}

-- Non-edit features for pure gameplay analysis
-- This model excludes all edit-based features to focus on actual game performance

with castaway_seasons_raw as (
    select
        c.castaway_id,
        c.version_season,
        c.result,
        c.place,
        c.winner,
        c.finalist,
        c.jury,
        c.original_tribe,
        c.age,
        row_number() over (partition by c.castaway_id, c.version_season order by c.episode desc) as rn
    from {{ source('bronze', 'castaways') }} c
    where c.result is not null
),

castaway_seasons as (
    select
        castaway_id,
        version_season,
        result,
        place,
        winner,
        finalist,
        jury,
        original_tribe,
        age
    from castaway_seasons_raw
    where rn = 1
),

-- Aggregate challenge performance
challenge_stats as (
    select
        castaway_id,
        version_season,
        count(*) as total_challenges,
        sum(won_flag) as challenges_won,
        sum(case when challenge_format = 'individual' then won_flag else 0 end) as individual_wins,
        sum(case when challenge_format = 'team' then won_flag else 0 end) as team_wins,
        sum(case when merge_phase = 'pre_merge' then won_flag else 0 end) as pre_merge_wins,
        sum(case when merge_phase = 'post_merge' then won_flag else 0 end) as post_merge_wins,
        sum(chosen_for_reward::int) as reward_selections,
        sum(sit_out::int) as challenge_sitouts,
        -- Skill-based performance
        sum(balance_win) as balance_wins,
        sum(endurance_win) as endurance_wins,
        sum(knowledge_win) as knowledge_wins,
        sum(memory_win) as memory_wins,
        sum(precision_win) as precision_wins,
        sum(puzzle_win) as puzzle_wins,
        sum(race_win) as race_wins,
        sum(strength_win) as strength_wins,
        sum(water_win) as water_wins,
        -- Skill participation rates
        sum(balance_participated)::numeric / nullif(count(*), 0) as balance_participation_rate,
        sum(endurance_participated)::numeric / nullif(count(*), 0) as endurance_participation_rate,
        sum(puzzle_participated)::numeric / nullif(count(*), 0) as puzzle_participation_rate,
        sum(strength_participated)::numeric / nullif(count(*), 0) as strength_participation_rate,
        -- Win rates by category
        sum(case when challenge_format = 'individual' then won_flag else 0 end)::numeric /
            nullif(sum(case when challenge_format = 'individual' then 1 else 0 end), 0) as individual_win_rate,
        sum(case when challenge_format = 'team' then won_flag else 0 end)::numeric /
            nullif(sum(case when challenge_format = 'team' then 1 else 0 end), 0) as team_win_rate
    from {{ ref('challenge_performance') }}
    group by castaway_id, version_season
),

-- Aggregate advantage strategy
advantage_stats as (
    select
        castaway_id,
        version_season,
        sum(case when event_category = 'found' then 1 else 0 end) as advantages_found,
        sum(case when event_category = 'played' then 1 else 0 end) as advantages_played,
        sum(case when advantage_category = 'idol' and event_category = 'found' then 1 else 0 end) as idols_found,
        sum(case when advantage_category = 'idol' and event_category = 'played' then 1 else 0 end) as idols_played,
        sum(played_successfully) as advantages_played_successfully,
        sum(played_unsuccessfully) as advantages_played_unsuccessfully,
        sum(played_for_self) as advantages_played_for_self,
        sum(played_for_others) as advantages_played_for_others,
        -- Success rates
        sum(played_successfully)::numeric / nullif(sum(case when event_category = 'played' then 1 else 0 end), 0) as advantage_success_rate,
        sum(case when advantage_category = 'idol' then played_successfully else 0 end)::numeric /
            nullif(sum(case when advantage_category = 'idol' and event_category = 'played' then 1 else 0 end), 0) as idol_success_rate
    from {{ ref('advantage_strategy') }}
    group by castaway_id, version_season
),

-- Aggregate voting behavior
vote_stats as (
    select
        castaway_id,
        version_season,
        count(*) as total_votes_cast,
        sum(vote_correct) as votes_correct,
        sum(vote_incorrect) as votes_incorrect,
        sum(case when merge_phase = 'pre_merge' then 1 else 0 end) as pre_merge_tribals,
        sum(case when merge_phase = 'post_merge' then 1 else 0 end) as post_merge_tribals,
        sum(case when merge_phase = 'pre_merge' then vote_correct else 0 end) as pre_merge_correct_votes,
        sum(case when merge_phase = 'post_merge' then vote_correct else 0 end) as post_merge_correct_votes,
        sum(in_majority_alliance::int) as majority_alliance_votes,
        sum(voting_alone::int) as lone_wolf_votes,
        avg(alignment_ratio) as avg_vote_alignment,
        -- Vote accuracy rates
        sum(vote_correct)::numeric / nullif(count(*), 0) as vote_accuracy_rate,
        sum(case when merge_phase = 'pre_merge' then vote_correct else 0 end)::numeric /
            nullif(sum(case when merge_phase = 'pre_merge' then 1 else 0 end), 0) as pre_merge_accuracy_rate,
        sum(case when merge_phase = 'post_merge' then vote_correct else 0 end)::numeric /
            nullif(sum(case when merge_phase = 'post_merge' then 1 else 0 end), 0) as post_merge_accuracy_rate,
        -- Alliance behavior rates
        sum(in_majority_alliance::int)::numeric / nullif(count(*), 0) as majority_alliance_rate,
        sum(voting_alone::int)::numeric / nullif(count(*), 0) as lone_wolf_rate
    from {{ ref('vote_dynamics') }}
    group by castaway_id, version_season
),

-- Get votes received (from vote_dynamics target analysis)
votes_received_stats as (
    select
        target_id as castaway_id,
        version_season,
        count(*) as total_votes_received,
        sum(case when merge_phase = 'pre_merge' then 1 else 0 end) as pre_merge_votes_received,
        sum(case when merge_phase = 'post_merge' then 1 else 0 end) as post_merge_votes_received
    from {{ ref('vote_dynamics') }}
    where target_id is not null
    group by target_id, version_season
),

-- Social positioning aggregates
social_stats as (
    select
        castaway_id,
        version_season,
        avg(same_gender_ratio) as avg_same_gender_ratio,
        avg(lgbt_similarity_ratio) as avg_lgbt_similarity_ratio,
        avg(bipoc_similarity_ratio) as avg_bipoc_similarity_ratio,
        avg(case when merge_phase = 'post_merge' then original_tribe_proportion else null end) as avg_original_tribe_strength,
        sum(case when gender_status = 'gender_minority' then 1 else 0 end)::numeric / nullif(count(*), 0) as gender_minority_rate,
        sum(case when racial_status = 'racial_minority' then 1 else 0 end)::numeric / nullif(count(*), 0) as racial_minority_rate,
        sum(case when lgbt_status = 'lgbt_minority' then 1 else 0 end)::numeric / nullif(count(*), 0) as lgbt_minority_rate,
        sum(case when original_tribe_status = 'minority' then 1 else 0 end)::numeric /
            nullif(sum(case when merge_phase = 'post_merge' then 1 else 0 end), 0) as original_tribe_minority_rate
    from {{ ref('social_positioning') }}
    group by castaway_id, version_season
),

-- Jury performance (for finalists)
jury_stats as (
    select
        finalist_id as castaway_id,
        version_season,
        count(*) as jury_votes_received,
        sum(same_original_tribe) as jury_votes_from_original_tribe,
        sum(same_original_tribe)::numeric / nullif(count(*), 0) as original_tribe_jury_support_rate
    from {{ ref('jury_analysis') }}
    group by finalist_id, version_season
)

select
    {{ generate_surrogate_key(['cs.castaway_id', 'cs.version_season']) }} as ml_features_key,
    cs.castaway_id,
    cs.version_season,

    -- Target variables
    case when cs.winner then 1 else 0 end as target_winner,
    case when cs.finalist then 1 else 0 end as target_finalist,
    case when cs.jury then 1 else 0 end as target_jury,
    cs.place::numeric::int as target_placement,

    -- Castaway profile features (from castaway_profile)
    cp.gender,
    cp.race,
    cp.ethnicity,
    cp.occupation,
    cp.personality_type,
    cp.age as current_age,
    cp.african::int as is_african,
    cp.asian::int as is_asian,
    cp.latin_american::int as is_latin_american,
    cp.native_american::int as is_native_american,
    cp.bipoc::int as is_bipoc,
    cp.lgbt::int as is_lgbt,

    -- Season context (non-edit features only)
    sc.season_number,
    sc.season_era,
    sc.is_new_era_format::int,
    sc.season_recency_weight,
    sc.cast_size,
    sc.finalist_count,
    sc.jury_count,
    sc.male_ratio as season_male_ratio,
    sc.female_ratio as season_female_ratio,
    sc.bipoc_ratio as season_bipoc_ratio,
    sc.returnee_ratio as season_returnee_ratio,
    sc.average_age as season_average_age,
    sc.has_edge_of_extinction::int,
    sc.has_redemption_island::int,
    sc.has_tribe_swap::int,
    sc.all_newbie_cast::int,
    sc.all_returnee_cast::int,
    sc.mixed_returnee_cast::int,

    -- Challenge performance features
    coalesce(ch.total_challenges, 0) as total_challenges,
    coalesce(ch.challenges_won, 0) as challenges_won,
    coalesce(ch.individual_wins, 0) as individual_wins,
    coalesce(ch.team_wins, 0) as team_wins,
    coalesce(ch.pre_merge_wins, 0) as pre_merge_wins,
    coalesce(ch.post_merge_wins, 0) as post_merge_wins,
    coalesce(ch.reward_selections, 0) as reward_selections,
    coalesce(ch.challenge_sitouts, 0) as challenge_sitouts,
    coalesce(ch.individual_win_rate, 0) as individual_win_rate,
    coalesce(ch.team_win_rate, 0) as team_win_rate,

    -- Challenge skill features
    coalesce(ch.balance_wins, 0) as balance_wins,
    coalesce(ch.endurance_wins, 0) as endurance_wins,
    coalesce(ch.knowledge_wins, 0) as knowledge_wins,
    coalesce(ch.memory_wins, 0) as memory_wins,
    coalesce(ch.precision_wins, 0) as precision_wins,
    coalesce(ch.puzzle_wins, 0) as puzzle_wins,
    coalesce(ch.race_wins, 0) as race_wins,
    coalesce(ch.strength_wins, 0) as strength_wins,
    coalesce(ch.water_wins, 0) as water_wins,
    coalesce(ch.balance_participation_rate, 0) as balance_participation_rate,
    coalesce(ch.endurance_participation_rate, 0) as endurance_participation_rate,
    coalesce(ch.puzzle_participation_rate, 0) as puzzle_participation_rate,
    coalesce(ch.strength_participation_rate, 0) as strength_participation_rate,

    -- Advantage strategy features
    coalesce(adv.advantages_found, 0) as advantages_found,
    coalesce(adv.advantages_played, 0) as advantages_played,
    coalesce(adv.idols_found, 0) as idols_found,
    coalesce(adv.idols_played, 0) as idols_played,
    coalesce(adv.advantages_played_successfully, 0) as advantages_played_successfully,
    coalesce(adv.advantages_played_unsuccessfully, 0) as advantages_played_unsuccessfully,
    coalesce(adv.advantages_played_for_self, 0) as advantages_played_for_self,
    coalesce(adv.advantages_played_for_others, 0) as advantages_played_for_others,
    coalesce(adv.advantage_success_rate, 0) as advantage_success_rate,
    coalesce(adv.idol_success_rate, 0) as idol_success_rate,

    -- Voting behavior features
    coalesce(vote.total_votes_cast, 0) as total_votes_cast,
    coalesce(vote.votes_correct, 0) as votes_correct,
    coalesce(vote.votes_incorrect, 0) as votes_incorrect,
    coalesce(vote.pre_merge_tribals, 0) as pre_merge_tribals_attended,
    coalesce(vote.post_merge_tribals, 0) as post_merge_tribals_attended,
    coalesce(vote.pre_merge_correct_votes, 0) as pre_merge_correct_votes,
    coalesce(vote.post_merge_correct_votes, 0) as post_merge_correct_votes,
    coalesce(vote.majority_alliance_votes, 0) as majority_alliance_votes,
    coalesce(vote.lone_wolf_votes, 0) as lone_wolf_votes,
    coalesce(vote.avg_vote_alignment, 0) as avg_vote_alignment,
    coalesce(vote.vote_accuracy_rate, 0) as vote_accuracy_rate,
    coalesce(vote.pre_merge_accuracy_rate, 0) as pre_merge_accuracy_rate,
    coalesce(vote.post_merge_accuracy_rate, 0) as post_merge_accuracy_rate,
    coalesce(vote.majority_alliance_rate, 0) as majority_alliance_rate,
    coalesce(vote.lone_wolf_rate, 0) as lone_wolf_rate,

    -- Votes received features
    coalesce(vr.total_votes_received, 0) as total_votes_received,
    coalesce(vr.pre_merge_votes_received, 0) as pre_merge_votes_received,
    coalesce(vr.post_merge_votes_received, 0) as post_merge_votes_received,

    -- Social positioning features
    coalesce(soc.avg_same_gender_ratio, 0) as avg_same_gender_ratio,
    coalesce(soc.avg_lgbt_similarity_ratio, 0) as avg_lgbt_similarity_ratio,
    coalesce(soc.avg_bipoc_similarity_ratio, 0) as avg_bipoc_similarity_ratio,
    coalesce(soc.avg_original_tribe_strength, 0) as avg_original_tribe_strength,
    coalesce(soc.gender_minority_rate, 0) as gender_minority_rate,
    coalesce(soc.racial_minority_rate, 0) as racial_minority_rate,
    coalesce(soc.lgbt_minority_rate, 0) as lgbt_minority_rate,
    coalesce(soc.original_tribe_minority_rate, 0) as original_tribe_minority_rate,

    -- Jury performance features (for finalists only)
    coalesce(jury.jury_votes_received, 0) as jury_votes_received,
    coalesce(jury.jury_votes_from_original_tribe, 0) as jury_votes_from_original_tribe,
    coalesce(jury.original_tribe_jury_support_rate, 0) as original_tribe_jury_support_rate,

    current_timestamp as created_at

from castaway_seasons cs
left join {{ ref('castaway_profile') }} cp on cp.castaway_id = cs.castaway_id
left join {{ ref('season_context') }} sc on sc.version_season = cs.version_season
left join challenge_stats ch on ch.castaway_id = cs.castaway_id and ch.version_season = cs.version_season
left join advantage_stats adv on adv.castaway_id = cs.castaway_id and adv.version_season = cs.version_season
left join vote_stats vote on vote.castaway_id = cs.castaway_id and vote.version_season = cs.version_season
left join votes_received_stats vr on vr.castaway_id = cs.castaway_id and vr.version_season = cs.version_season
left join social_stats soc on soc.castaway_id = cs.castaway_id and soc.version_season = cs.version_season
left join jury_stats jury on jury.castaway_id = cs.castaway_id and jury.version_season = cs.version_season
