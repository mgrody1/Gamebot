{{ config(materialized='table', schema='silver') }}

-- Advantage finding, playing, and strategic usage
with advantage_events as (
    select
        am.castaway_id,
        am.version_season,
        am.episode,
        am.day,
        am.advantage_id,
        am.sequence_id,
        lower(am.event) as event_lower,
        am.played_for_id,
        am.co_castaway_ids,
        am.joint_play,
        am.multi_target_play,
        lower(am.success) as success_lower,
        am.votes_nullified,
        ad.advantage_type,
        ad.clue_details,
        ad.location_found,
        ad.conditions
    from {{ source('bronze', 'advantage_movement') }} am
    join {{ source('bronze', 'advantage_details') }} ad
        on am.version_season = ad.version_season
        and am.advantage_id = ad.advantage_id
)

select
    {{ generate_surrogate_key(['castaway_id', 'version_season', 'advantage_id', 'sequence_id']) }} as advantage_strategy_key,
    castaway_id,
    version_season,
    episode,
    day,
    advantage_id,
    sequence_id,
    event_lower as event_type,
    played_for_id as target_castaway_id,
    co_castaway_ids,
    joint_play,
    multi_target_play,
    success_lower as success_outcome,
    votes_nullified,
    advantage_type,
    clue_details,
    location_found,
    conditions,
    -- Event categorization
    case when event_lower like '%found%' then 'found'
         when event_lower like '%played%' then 'played'
         when event_lower like '%received%' then 'received'
         when event_lower like '%gifted%' or event_lower like '%shared%' then 'shared'
         else 'other' end as event_category,
    -- Advantage categorization
    case when lower(advantage_type) like '%idol%' then 'idol'
         when lower(advantage_type) like '%immunity%' then 'immunity'
         when lower(advantage_type) like '%vote%' then 'vote_modifier'
         when lower(advantage_type) like '%advantage%' then 'advantage'
         else 'other' end as advantage_category,
    -- Success flags
    case when event_lower like '%played%' and success_lower = 'yes' then 1 else 0 end as played_successfully,
    case when event_lower like '%played%' and success_lower in ('no', 'not needed') then 1 else 0 end as played_unsuccessfully,
    case when event_lower like '%played%' and (played_for_id is null or played_for_id = castaway_id) then 1 else 0 end as played_for_self,
    case when event_lower like '%played%' and played_for_id is not null and played_for_id != castaway_id then 1 else 0 end as played_for_others,
    current_timestamp as created_at
from advantage_events
