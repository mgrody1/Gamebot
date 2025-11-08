{{ config(materialized='table', schema='silver') }}

-- Voting patterns, alliance behavior, and strategic positioning
with vote_base as (
    select
        vh.castaway_id as voter_id,
        vh.version_season,
        vh.episode,
        vh.day,
        vh.tribe,
        vh.tribe_status,
        vh.vote_id as target_id,
        vh.voted_out_id,
        vh.immunity,
        vh.vote_event,
        vh.vote_event_outcome,
        vh.split_vote,
        vh.nullified,
        vh.tie,
        vh.vote_order,
        case when tribe_status ilike '%merge%' then 'post_merge' else 'pre_merge' end as merge_phase
    from {{ source('bronze', 'vote_history') }} vh
    where vh.vote_id is not null -- exclude non-voting episodes
),

-- Calculate who voted together each episode
vote_alignment as (
    select
        v1.voter_id,
        v1.version_season,
        v1.episode,
        v1.merge_phase,
        count(*) as total_voters,
        count(*) filter (where v2.target_id = v1.target_id) as aligned_votes,
        count(*) filter (where v2.target_id = v1.target_id)::numeric / nullif(count(*), 0) as alignment_ratio
    from vote_base v1
    join vote_base v2 on v1.version_season = v2.version_season
                     and v1.episode = v2.episode
                     and v1.voter_id != v2.voter_id
    group by v1.voter_id, v1.version_season, v1.episode, v1.merge_phase
)

select
    {{ generate_surrogate_key(['vb.voter_id', 'vb.version_season', 'vb.episode']) }} as vote_dynamics_key,
    vb.voter_id as castaway_id,
    vb.version_season,
    vb.episode,
    vb.day,
    vb.tribe,
    vb.tribe_status,
    vb.merge_phase,
    vb.target_id,
    vb.voted_out_id,
    vb.immunity,
    vb.vote_event,
    vb.vote_event_outcome,
    vb.split_vote,
    vb.nullified,
    vb.tie,
    vb.vote_order,
    -- Voting outcome analysis
    case when vb.voted_out_id = vb.target_id then 1 else 0 end as vote_correct,
    case when vb.voted_out_id != vb.target_id or vb.voted_out_id is null then 1 else 0 end as vote_incorrect,
    case when vb.nullified = true then 1 else 0 end as vote_nullified,
    -- Alliance behavior
    va.total_voters,
    va.aligned_votes,
    va.alignment_ratio,
    case when va.alignment_ratio >= 0.5 then 1 else 0 end as in_majority_alliance,
    case when va.alignment_ratio < 0.3 then 1 else 0 end as voting_alone,
    -- Special voting scenarios
    case when vb.split_vote = 'split' then 1 else 0 end as split_vote_scenario,
    case when vb.tie = true then 1 else 0 end as tie_vote_scenario,
    case when vb.immunity is not null and length(vb.immunity) > 0 then 1 else 0 end as has_immunity,
    current_timestamp as created_at
from vote_base vb
left join vote_alignment va on va.voter_id = vb.voter_id
                           and va.version_season = vb.version_season
                           and va.episode = vb.episode
