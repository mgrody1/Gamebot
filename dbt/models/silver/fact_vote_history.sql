{{ config(materialized='table', schema='silver') }}

with source as (
    select
        vote_history_id,
        version,
        version_season,
        season,
        episode,
        castaway_id,
        castaway,
        vote_id,
        voted_out_id,
        immunity,
        vote,
        vote_event,
        vote_event_outcome,
        split_vote,
        nullified,
        tie,
        vote_order,
        sog_id,
        challenge_id,
        tribe_status,
        tribe,
        source_dataset
    from {{ ref('stg_vote_history') }}
),
vote_aggregation as (
    select
        version_season,
        vote_event,
        vote_id,
        count(*) as vote_count,
        max(count(*)) over (partition by version_season, vote_event) as max_vote_count
    from source
    where vote_event is not null
    group by version_season, vote_event, vote_id
),
majority_flags as (
    select
        version_season,
        vote_event,
        vote_id,
        vote_count,
        (vote_count = max_vote_count) as is_majority_vote
    from vote_aggregation
),
first_merge_episode as (
    select
        version_season,
        min(episode) as first_merge_episode
    from source
    where tribe_status ilike 'Merged%'
    group by version_season
),
episode_map as (
    select version_season, episode_in_season, episode_key, season_key
    from {{ ref('dim_episode') }}
),
castaway_map as (
    select castaway_id, castaway_key
    from {{ ref('dim_castaway') }}
),
challenge_map as (
    select version_season, challenge_id, challenge_key
    from {{ ref('dim_challenge') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['source.vote_history_id']) }} as vote_fact_key,
    voter.castaway_key,
    em.season_key,
    em.episode_key,
    ch.challenge_key,
    source.version,
    source.season,
    source.castaway_id,
    target.castaway_id as target_castaway_id,
    eliminated.castaway_id as voted_out_castaway_id,
    source.version_season,
    source.episode as episode_in_season,
    source.immunity,
    source.vote,
    source.vote_event,
    source.vote_event_outcome,
    source.split_vote,
    source.nullified,
    source.tie,
    source.vote_order,
    source.sog_id,
    source.tribe_status,
    source.tribe,
    source.source_dataset,
    coalesce(mj.is_majority_vote, false) as voted_with_majority,
    (source.voted_out_id is not null and source.voted_out_id = source.vote_id) as voted_correctly,
    (source.nullified is true) as vote_nullified,
    (source.vote_event is not null) as attended_tribal,
    (fm.first_merge_episode is not null and source.episode = fm.first_merge_episode and source.tribe_status ilike 'Merged%') as is_merge_vote,
    ((fm.first_merge_episode is not null and source.episode = fm.first_merge_episode and source.tribe_status ilike 'Merged%')
        and (source.voted_out_id is not null and source.voted_out_id = source.vote_id)) as voted_correctly_at_first_merge,
    source.vote_history_id as source_vote_history_id,
    current_timestamp as created_at
from source
left join episode_map em
  on em.version_season = source.version_season
 and em.episode_in_season = source.episode
left join castaway_map voter on voter.castaway_id = source.castaway_id
left join castaway_map target on target.castaway_id = source.vote_id
left join castaway_map eliminated on eliminated.castaway_id = source.voted_out_id
left join challenge_map ch
  on ch.version_season = source.version_season
 and ch.challenge_id = source.challenge_id
left join majority_flags mj
  on mj.version_season = source.version_season
 and mj.vote_event = source.vote_event
 and mj.vote_id = source.vote_id
left join first_merge_episode fm
  on fm.version_season = source.version_season;
