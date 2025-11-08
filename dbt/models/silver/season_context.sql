{{ config(materialized='table', schema='silver') }}

-- Season-level context and meta-features
with season_base as (
    select
        version,
        version_season,
        season_name,
        season as season_number,
        location,
        country,
        tribe_setup,
        n_cast as cast_size,
        n_tribes as tribe_count,
        n_finalists as finalist_count,
        n_jury as jury_count,
        premiered,
        ended,
        filming_started,
        filming_ended,
        winner_id as winner_castaway_id,
        winner,
        runner_ups,
        final_vote,
        timeslot,
        viewers_reunion,
        viewers_premiere,
        viewers_finale,
        viewers_mean,
        rank
    from {{ source('bronze', 'season_summary') }}
),

-- Calculate season weights based on recency
season_bounds as (
    select
        min(season_number) as min_season,
        max(season_number) as max_season
    from season_base
    where season_number is not null
),

-- Get cast composition for each season
cast_demographics as (
    select
        c.version_season,
        count(*) as total_cast,
        count(*) filter (where cd.gender ilike 'male') as male_count,
        count(*) filter (where cd.gender ilike 'female') as female_count,
        count(*) filter (where cd.bipoc = true) as bipoc_count,
        count(*) filter (where cd.lgbt = true) as lgbt_count,
        count(*) filter (where cd.is_returning_player = true) as returnee_count,
        -- Calculate diversity ratios
        count(*) filter (where cd.gender ilike 'male')::numeric / nullif(count(*), 0) as male_ratio,
        count(*) filter (where cd.gender ilike 'female')::numeric / nullif(count(*), 0) as female_ratio,
        count(*) filter (where cd.bipoc = true)::numeric / nullif(count(*), 0) as bipoc_ratio,
        count(*) filter (where cd.lgbt = true)::numeric / nullif(count(*), 0) as lgbt_ratio,
        count(*) filter (where cd.is_returning_player = true)::numeric / nullif(count(*), 0) as returnee_ratio,
        -- Age statistics
        avg(c.age) filter (where c.age is not null) as average_age,
        min(c.age) filter (where c.age is not null) as min_age,
        max(c.age) filter (where c.age is not null) as max_age,
        stddev(c.age) filter (where c.age is not null) as age_stddev
    from {{ source('bronze', 'castaways') }} c
    join (
        select
            castaway_id,
            gender,
            bipoc,
            lgbt,
            (count(*) over (partition by castaway_id)) > 1 as is_returning_player
        from {{ source('bronze', 'castaway_details') }}
    ) cd on cd.castaway_id = c.castaway_id
    group by c.version_season
)

select
    {{ generate_surrogate_key(['sb.version_season']) }} as season_context_key,
    sb.version,
    sb.version_season,
    sb.season_name,
    sb.season_number,
    sb.location,
    sb.country,
    sb.tribe_setup,
    sb.cast_size,
    sb.tribe_count,
    sb.finalist_count,
    sb.jury_count,
    sb.premiered,
    sb.ended,
    sb.filming_started,
    sb.filming_ended,
    sb.winner_castaway_id,
    sb.winner,
    sb.runner_ups,
    sb.final_vote,
    sb.timeslot,
    sb.viewers_reunion,
    sb.viewers_premiere,
    sb.viewers_finale,
    sb.viewers_mean,
    sb.rank,
    -- Season era classification
    case when sb.season_number <= 10 then 'early'
         when sb.season_number <= 20 then 'middle'
         when sb.season_number <= 30 then 'modern'
         when sb.season_number <= 40 then 'new_school'
         else 'new_era' end as season_era,
    case when sb.cast_size is not null and sb.cast_size <= 18 then true else false end as is_new_era_format,
    -- Season weight for ML models (more recent = higher weight)
    case when bounds.max_season > bounds.min_season
         then 1 - (bounds.max_season - sb.season_number)::numeric / nullif(bounds.max_season - bounds.min_season, 0)
         else 1 end as season_recency_weight,
    -- Twist detection
    case when lower(sb.tribe_setup) like '%edge%' or lower(sb.tribe_setup) like '%extinction%' then true else false end as has_edge_of_extinction,
    case when lower(sb.tribe_setup) like '%redemption%' then true else false end as has_redemption_island,
    case when lower(sb.tribe_setup) like '%swap%' then true else false end as has_tribe_swap,
    case when lower(sb.tribe_setup) like '%merge%' then true else false end as has_merge_twist,
    -- Cast composition features
    cd.total_cast,
    cd.male_count,
    cd.female_count,
    cd.bipoc_count,
    cd.lgbt_count,
    cd.returnee_count,
    cd.male_ratio,
    cd.female_ratio,
    cd.bipoc_ratio,
    cd.lgbt_ratio,
    cd.returnee_ratio,
    cd.average_age,
    cd.min_age,
    cd.max_age,
    cd.age_stddev,
    -- Diversity flags
    case when cd.bipoc_ratio >= 0.4 then true else false end as high_diversity_cast,
    case when cd.returnee_ratio > 0 and cd.returnee_ratio < 1 then true else false end as mixed_returnee_cast,
    case when cd.returnee_ratio = 1 then true else false end as all_returnee_cast,
    case when cd.returnee_ratio = 0 then true else false end as all_newbie_cast,
    current_timestamp as created_at
from season_base sb
cross join season_bounds bounds
left join cast_demographics cd on cd.version_season = sb.version_season
