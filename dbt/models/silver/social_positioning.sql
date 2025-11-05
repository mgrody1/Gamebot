{{ config(materialized='table', schema='silver') }}

-- Social positioning, tribe dynamics, and composition analysis
with tribe_episodes as (
    select
        tm.castaway_id,
        tm.version_season,
        tm.episode,
        tm.day,
        tm.tribe,
        tm.tribe_status,
        case when tm.tribe_status ilike '%merge%' then 'post_merge' else 'pre_merge' end as merge_phase
    from {{ source('bronze', 'tribe_mapping') }} tm
),

-- Get castaway demographics for composition analysis
castaway_demo as (
    select
        castaway_id,
        gender,
        african,
        asian,
        latin_american,
        native_american,
        bipoc,
        lgbt,
        race,
        ethnicity
    from {{ source('bronze', 'castaway_details') }}
),

-- Calculate tribe composition for each episode
tribe_composition as (
    select
        te.version_season,
        te.episode,
        te.tribe,
        te.merge_phase,
        count(*) as tribe_size,
        count(*) filter (where cd.gender ilike 'male') as male_count,
        count(*) filter (where cd.gender ilike 'female') as female_count,
        count(*) filter (where cd.african = true) as african_count,
        count(*) filter (where cd.asian = true) as asian_count,
        count(*) filter (where cd.latin_american = true) as latin_american_count,
        count(*) filter (where cd.native_american = true) as native_american_count,
        count(*) filter (where cd.bipoc = true) as bipoc_count,
        count(*) filter (where cd.lgbt = true) as lgbt_count,
        count(*) filter (where cd.gender ilike 'male')::numeric / nullif(count(*), 0) as male_ratio,
        count(*) filter (where cd.gender ilike 'female')::numeric / nullif(count(*), 0) as female_ratio,
        count(*) filter (where cd.african = true)::numeric / nullif(count(*), 0) as african_ratio,
        count(*) filter (where cd.asian = true)::numeric / nullif(count(*), 0) as asian_ratio,
        count(*) filter (where cd.latin_american = true)::numeric / nullif(count(*), 0) as latin_american_ratio,
        count(*) filter (where cd.native_american = true)::numeric / nullif(count(*), 0) as native_american_ratio,
        count(*) filter (where cd.bipoc = true)::numeric / nullif(count(*), 0) as bipoc_ratio,
        count(*) filter (where cd.lgbt = true)::numeric / nullif(count(*), 0) as lgbt_ratio
    from tribe_episodes te
    join castaway_demo cd on cd.castaway_id = te.castaway_id
    group by te.version_season, te.episode, te.tribe, te.merge_phase
),

-- Get original tribes for analysis
original_tribes as (
    select
        castaway_id,
        version_season,
        original_tribe
    from (
        select
            c.castaway_id,
            c.version_season,
            c.original_tribe,
            row_number() over (partition by c.castaway_id, c.version_season order by c.episode) as rn
        from {{ source('bronze', 'castaways') }} c
        where c.original_tribe is not null
    ) ranked
    where rn = 1
),

-- Calculate original tribe representation post-merge
original_tribe_composition as (
    select
        te.version_season,
        te.episode,
        te.tribe,
        ot.original_tribe,
        count(*) as original_tribe_members,
        count(*) over (partition by te.version_season, te.episode, te.tribe) as total_tribe_members
    from tribe_episodes te
    join original_tribes ot on ot.castaway_id = te.castaway_id
                           and ot.version_season = te.version_season
    where te.merge_phase = 'post_merge'
    group by te.version_season, te.episode, te.tribe, ot.original_tribe
)

select
    {{ dbt_utils.generate_surrogate_key(['te.castaway_id', 'te.version_season', 'te.episode', 'te.tribe']) }} as social_positioning_key,
    te.castaway_id,
    te.version_season,
    te.episode,
    te.day,
    te.tribe,
    te.tribe_status,
    te.merge_phase,
    -- Individual demographics in context
    cd.gender,
    cd.african,
    cd.asian,
    cd.latin_american,
    cd.native_american,
    cd.bipoc,
    cd.lgbt,
    cd.race,
    cd.ethnicity,
    -- Tribe composition
    tc.tribe_size,
    tc.male_count,
    tc.female_count,
    tc.african_count,
    tc.asian_count,
    tc.latin_american_count,
    tc.native_american_count,
    tc.bipoc_count,
    tc.lgbt_count,
    tc.male_ratio,
    tc.female_ratio,
    tc.african_ratio,
    tc.asian_ratio,
    tc.latin_american_ratio,
    tc.native_american_ratio,
    tc.bipoc_ratio,
    tc.lgbt_ratio,
    -- Individual positioning within tribe
    case when cd.gender ilike 'male' then tc.male_ratio else tc.female_ratio end as same_gender_ratio,
    case when cd.lgbt = true then tc.lgbt_ratio else (1 - tc.lgbt_ratio) end as lgbt_similarity_ratio,
    case when cd.bipoc = true then tc.bipoc_ratio else (1 - tc.bipoc_ratio) end as bipoc_similarity_ratio,
    -- Original tribe analysis (post-merge only)
    ot.original_tribe,
    otc.original_tribe_members,
    otc.total_tribe_members,
    case when te.merge_phase = 'post_merge' and otc.original_tribe_members is not null
         then otc.original_tribe_members::numeric / nullif(otc.total_tribe_members, 0)
         else null end as original_tribe_proportion,
    -- Minority/majority status
    case when te.merge_phase = 'post_merge' and otc.original_tribe_members is not null
         then case when otc.original_tribe_members::numeric / nullif(otc.total_tribe_members, 0) < 0.5
                   then 'minority' else 'majority' end
         else null end as original_tribe_status,
    case when cd.gender ilike 'male' and tc.male_ratio < 0.5 then 'gender_minority'
         when cd.gender ilike 'female' and tc.female_ratio < 0.5 then 'gender_minority'
         else 'gender_majority' end as gender_status,
    case when cd.bipoc = true and tc.bipoc_ratio < 0.5 then 'racial_minority'
         when cd.bipoc = false and tc.bipoc_ratio > 0.5 then 'racial_minority'
         else 'racial_majority' end as racial_status,
    case when cd.lgbt = true and tc.lgbt_ratio < 0.5 then 'lgbt_minority'
         when cd.lgbt = false and tc.lgbt_ratio > 0.5 then 'lgbt_minority'
         else 'lgbt_majority' end as lgbt_status,
    current_timestamp as created_at
from tribe_episodes te
join castaway_demo cd on cd.castaway_id = te.castaway_id
join tribe_composition tc on tc.version_season = te.version_season
                          and tc.episode = te.episode
                          and tc.tribe = te.tribe
left join original_tribes ot on ot.castaway_id = te.castaway_id
                             and ot.version_season = te.version_season
left join original_tribe_composition otc on otc.version_season = te.version_season
                                        and otc.episode = te.episode
                                        and otc.tribe = te.tribe
                                        and otc.original_tribe = ot.original_tribe
