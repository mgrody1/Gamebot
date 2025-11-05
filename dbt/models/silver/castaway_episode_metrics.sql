{{ config(materialized='table', schema='silver') }}

with challenge_metrics as (
    select
        version_season,
        castaway_id,
        episode_in_season,
        count(*) as challenge_entries,
        count(*) filter (where won_challenge) as challenge_wins,
        count(*) filter (where won_immunity) as immunity_wins,
        count(*) filter (where won_reward) as reward_wins,
        count(*) filter (where sit_out) as sits_out,
        count(*) filter (where chosen_for_reward) as chosen_for_reward_count
    from {{ ref('fact_challenge_results') }}
    group by version_season, castaway_id, episode_in_season
),
vote_metrics as (
    select
        version_season,
        castaway_id,
        episode_in_season,
        count(*) as votes_cast,
        count(*) filter (where voted_correctly) as votes_correct,
        count(*) filter (where voted_with_majority) as votes_with_majority,
        count(*) filter (where vote_nullified) as votes_nullified,
        count(*) filter (where voted_correctly_at_first_merge) as merge_vote_correct
    from {{ ref('fact_vote_history') }}
    group by version_season, castaway_id, episode_in_season
),
votes_received as (
    select
        version_season,
        target_castaway_id as castaway_id,
        episode_in_season,
        count(*) as votes_received
    from {{ ref('fact_vote_history') }}
    where target_castaway_id is not null
    group by version_season, target_castaway_id, episode_in_season
),
advantage_metrics as (
    select
        version_season,
        castaway_id,
        episode_in_season,
        count(*) as advantage_events,
        count(*) filter (where success = 'yes') as advantages_successful,
        count(*) filter (where success = 'no') as advantages_unsuccessful,
        count(*) filter (where success = 'not needed') as advantages_not_needed,
        sum(coalesce(votes_nullified, 0)) as votes_nullified_by_advantage
    from {{ ref('fact_advantage_movement') }}
    group by version_season, castaway_id, episode_in_season
),
journey_metrics as (
    select
        version_season,
        castaway_id,
        episode_in_season,
        count(*) as journeys_taken,
        count(*) filter (where lost_vote) as journeys_lost_vote
    from {{ ref('fact_journeys') }}
    group by version_season, castaway_id, episode_in_season
),
tribal_metrics as (
    select
        version_season,
        castaway_id,
        episode_in_season,
        count(*) as tribe_presence_events
    from {{ ref('fact_tribe_membership') }}
    group by version_season, castaway_id, episode_in_season
),
episode_dim as (
    select
        d.version_season,
        d.season_key,
        e.episode_key,
        e.episode_in_season
    from {{ ref('dim_episode') }} e
    join {{ ref('dim_season') }} d on d.season_key = e.season_key
),
castaway_bridge as (
    select
        castaway_id,
        version_season,
        castaway_key
    from {{ ref('bridge_castaway_season') }}
)

,base as (
    select version_season, castaway_id, episode_in_season from challenge_metrics
    union
    select version_season, castaway_id, episode_in_season from vote_metrics
    union
    select version_season, castaway_id, episode_in_season from votes_received
    union
    select version_season, castaway_id, episode_in_season from advantage_metrics
    union
    select version_season, castaway_id, episode_in_season from journey_metrics
    union
    select version_season, castaway_id, episode_in_season from tribal_metrics
)

select
    {{ dbt_utils.generate_surrogate_key(['base.version_season', 'base.castaway_id', 'base.episode_in_season']) }} as castaway_episode_metrics_key,
    cb.castaway_key,
    ed.season_key,
    ed.episode_key,
    base.version_season,
    base.castaway_id,
    base.episode_in_season,
    coalesce(challenge_metrics.challenge_entries, 0) as challenge_entries,
    coalesce(challenge_metrics.challenge_wins, 0) as challenge_wins,
    coalesce(challenge_metrics.immunity_wins, 0) as immunity_wins,
    coalesce(challenge_metrics.reward_wins, 0) as reward_wins,
    coalesce(challenge_metrics.sits_out, 0) as sit_outs,
    coalesce(challenge_metrics.chosen_for_reward_count, 0) as chosen_for_reward,
    coalesce(vote_metrics.votes_cast, 0) as votes_cast,
    coalesce(vote_metrics.votes_correct, 0) as votes_correct,
    coalesce(vote_metrics.votes_with_majority, 0) as votes_with_majority,
    coalesce(vote_metrics.votes_nullified, 0) as votes_nullified_against,
    coalesce(vote_metrics.merge_vote_correct, 0) as merge_vote_correct,
    coalesce(votes_received.votes_received, 0) as votes_received,
    coalesce(advantage_metrics.advantage_events, 0) as advantage_events,
    coalesce(advantage_metrics.advantages_successful, 0) as advantages_successful,
    coalesce(advantage_metrics.advantages_unsuccessful, 0) as advantages_unsuccessful,
    coalesce(advantage_metrics.advantages_not_needed, 0) as advantages_not_needed,
    coalesce(advantage_metrics.votes_nullified_by_advantage, 0) as votes_nullified_by_advantage,
    coalesce(journey_metrics.journeys_taken, 0) as journeys_taken,
    coalesce(journey_metrics.journeys_lost_vote, 0) as journeys_lost_vote,
    coalesce(tribal_metrics.tribe_presence_events, 0) as tribe_presence_events,
    current_timestamp as created_at,
    current_timestamp as updated_at
from base
left join challenge_metrics on
    challenge_metrics.version_season = base.version_season
    and challenge_metrics.castaway_id = base.castaway_id
    and challenge_metrics.episode_in_season = base.episode_in_season
left join vote_metrics on
    vote_metrics.version_season = base.version_season
    and vote_metrics.castaway_id = base.castaway_id
    and vote_metrics.episode_in_season = base.episode_in_season
left join votes_received on
    votes_received.version_season = base.version_season
    and votes_received.castaway_id = base.castaway_id
    and votes_received.episode_in_season = base.episode_in_season
left join advantage_metrics on
    advantage_metrics.version_season = base.version_season
    and advantage_metrics.castaway_id = base.castaway_id
    and advantage_metrics.episode_in_season = base.episode_in_season
left join journey_metrics on
    journey_metrics.version_season = base.version_season
    and journey_metrics.castaway_id = base.castaway_id
    and journey_metrics.episode_in_season = base.episode_in_season
left join tribal_metrics on
    tribal_metrics.version_season = base.version_season
    and tribal_metrics.castaway_id = base.castaway_id
    and tribal_metrics.episode_in_season = base.episode_in_season
left join castaway_bridge cb on
    cb.version_season = base.version_season
    and cb.castaway_id = base.castaway_id
left join episode_dim ed on
    ed.version_season = base.version_season
    and ed.episode_in_season = base.episode_in_season;
