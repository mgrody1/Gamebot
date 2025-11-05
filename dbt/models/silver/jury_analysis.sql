{{ config(materialized='table', schema='silver') }}

-- Jury voting patterns and relationships
with jury_base as (
    select
        jv.castaway_id as juror_id,
        jv.version_season,
        jv.finalist_id,
        jv.vote as voted_for_finalist_name,
        -- Get the actual winner for analysis
        ss.winner_id as actual_winner_id
    from {{ source('bronze', 'jury_votes') }} jv
    join {{ source('bronze', 'season_summary') }} ss on ss.version_season = jv.version_season
),

-- Get original tribes for jury analysis
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
)

select
    {{ dbt_utils.generate_surrogate_key(['jb.juror_id', 'jb.version_season', 'jb.finalist_id']) }} as jury_analysis_key,
    jb.juror_id as castaway_id,
    jb.version_season,
    jb.finalist_id,
    jb.voted_for_finalist_name,
    jb.actual_winner_id,
    -- Jury vote analysis
    case when jb.finalist_id = jb.actual_winner_id then 1 else 0 end as voted_for_winner,
    case when jb.finalist_id != jb.actual_winner_id then 1 else 0 end as voted_against_winner,
    -- Original tribe relationships
    juror_ot.original_tribe as juror_original_tribe,
    finalist_ot.original_tribe as finalist_original_tribe,
    case when juror_ot.original_tribe = finalist_ot.original_tribe then 1 else 0 end as same_original_tribe,
    case when juror_ot.original_tribe != finalist_ot.original_tribe then 1 else 0 end as different_original_tribe,
    current_timestamp as created_at
from jury_base jb
left join original_tribes juror_ot on juror_ot.castaway_id = jb.juror_id
                                   and juror_ot.version_season = jb.version_season
left join original_tribes finalist_ot on finalist_ot.castaway_id = jb.finalist_id
                                      and finalist_ot.version_season = jb.version_season
