{{ config(materialized='table', schema='gold') }}

-- Hybrid features combining gameplay and edit for maximum prediction accuracy
-- This includes both non-edit features and edit-based features

with edit_stats as (
    select
        castaway_id,
        version_season,
        sum(confessional_count) as total_confessional_count,
        sum(confessional_time) as total_confessional_time,
        sum(expected_confessional_count) as total_expected_confessional_count,
        sum(expected_confessional_time) as total_expected_confessional_time,
        sum(has_confessional) as episodes_with_confessionals,
        sum(high_confessional_count) as high_confessional_episodes,
        sum(high_confessional_time) as high_confessional_time_episodes,
        sum(over_edited_count) as over_edited_count_episodes,
        sum(over_edited_time) as over_edited_time_episodes,
        sum(under_edited_count) as under_edited_count_episodes,
        sum(under_edited_time) as under_edited_time_episodes,
        count(*) as total_episodes_appeared,
        -- Edit ratios and rates
        sum(confessional_count)::numeric / nullif(sum(expected_confessional_count), 0) as overall_confessional_count_ratio,
        sum(confessional_time)::numeric / nullif(sum(expected_confessional_time), 0) as overall_confessional_time_ratio,
        sum(has_confessional)::numeric / nullif(count(*), 0) as confessional_presence_rate,
        sum(over_edited_count)::numeric / nullif(count(*), 0) as over_edited_rate,
        sum(under_edited_count)::numeric / nullif(count(*), 0) as under_edited_rate,
        -- Average per episode
        avg(confessional_count) as avg_confessional_count_per_episode,
        avg(confessional_time) as avg_confessional_time_per_episode
    from {{ ref('edit_features') }}
    group by castaway_id, version_season
)

select
    nef.*,

    -- Edit features
    coalesce(edit.total_confessional_count, 0) as total_confessional_count,
    coalesce(edit.total_confessional_time, 0) as total_confessional_time,
    coalesce(edit.total_expected_confessional_count, 0) as total_expected_confessional_count,
    coalesce(edit.total_expected_confessional_time, 0) as total_expected_confessional_time,
    coalesce(edit.episodes_with_confessionals, 0) as episodes_with_confessionals,
    coalesce(edit.high_confessional_episodes, 0) as high_confessional_episodes,
    coalesce(edit.high_confessional_time_episodes, 0) as high_confessional_time_episodes,
    coalesce(edit.over_edited_count_episodes, 0) as over_edited_count_episodes,
    coalesce(edit.over_edited_time_episodes, 0) as over_edited_time_episodes,
    coalesce(edit.under_edited_count_episodes, 0) as under_edited_count_episodes,
    coalesce(edit.under_edited_time_episodes, 0) as under_edited_time_episodes,
    coalesce(edit.total_episodes_appeared, 0) as total_episodes_appeared,
    coalesce(edit.overall_confessional_count_ratio, 0) as overall_confessional_count_ratio,
    coalesce(edit.overall_confessional_time_ratio, 0) as overall_confessional_time_ratio,
    coalesce(edit.confessional_presence_rate, 0) as confessional_presence_rate,
    coalesce(edit.over_edited_rate, 0) as over_edited_rate,
    coalesce(edit.under_edited_rate, 0) as under_edited_rate,
    coalesce(edit.avg_confessional_count_per_episode, 0) as avg_confessional_count_per_episode,
    coalesce(edit.avg_confessional_time_per_episode, 0) as avg_confessional_time_per_episode,

    -- Edit visibility flags
    case when edit.total_confessional_count > 0 then 1 else 0 end as has_any_confessionals,
    case when edit.confessional_presence_rate >= 0.5 then 1 else 0 end as high_edit_presence,
    case when edit.overall_confessional_count_ratio > 1.2 then 1 else 0 end as significantly_over_edited,
    case when edit.overall_confessional_count_ratio < 0.8 then 1 else 0 end as significantly_under_edited,

    -- Combined edit + gameplay features
    case when edit.total_confessional_count > 0 and nef.challenges_won > 0 then 1 else 0 end as edit_and_challenge_success,
    case when edit.total_confessional_count > 0 and nef.advantages_found > 0 then 1 else 0 end as edit_and_advantage_success,
    case when edit.confessional_presence_rate >= 0.5 and nef.vote_accuracy_rate >= 0.7 then 1 else 0 end as edit_and_strategic_success,

    current_timestamp as updated_at

from {{ ref('ml_features_non_edit') }} nef
left join edit_stats edit on edit.castaway_id = nef.castaway_id and edit.version_season = nef.version_season
