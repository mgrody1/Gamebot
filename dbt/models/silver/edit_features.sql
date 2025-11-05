{{ config(materialized='table', schema='silver') }}

-- Edit-based features: confessionals, airtime, and production choices
select
    {{ dbt_utils.generate_surrogate_key(['castaway_id', 'version_season', 'episode']) }} as edit_features_key,
    castaway_id,
    version_season,
    episode,
    confessional_count,
    confessional_time,
    exp_count as expected_confessional_count,
    exp_time as expected_confessional_time,
    -- Calculate relative edit presence
    case when exp_count > 0
         then confessional_count::numeric / exp_count
         else null end as confessional_count_ratio,
    case when exp_time > 0
         then confessional_time::numeric / exp_time
         else null end as confessional_time_ratio,
    -- Edit visibility flags
    case when confessional_count > 0 then 1 else 0 end as has_confessional,
    case when confessional_count >= 3 then 1 else 0 end as high_confessional_count,
    case when confessional_time >= 60 then 1 else 0 end as high_confessional_time,
    -- Over/under edited relative to expectations
    case when exp_count > 0 and confessional_count > exp_count then 1 else 0 end as over_edited_count,
    case when exp_time > 0 and confessional_time > exp_time then 1 else 0 end as over_edited_time,
    case when exp_count > 0 and confessional_count < exp_count then 1 else 0 end as under_edited_count,
    case when exp_time > 0 and confessional_time < exp_time then 1 else 0 end as under_edited_time,
    current_timestamp as created_at
from {{ source('bronze', 'confessionals') }}
