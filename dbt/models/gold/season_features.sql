{{ config(materialized='table', schema='gold') }}

with current_snapshot as (
    select snapshot_id
    from {{ ref('feature_snapshots') }}
    order by run_started_at desc nulls last
    limit 1
),
season_bounds AS (
    SELECT MIN(season_number) AS min_season, MAX(season_number) AS max_season
    FROM {{ ref('dim_season') }}
),
season_returnee_ratio AS (
    SELECT
        season_key,
        SUM(CASE WHEN bcs.original_tribe IS NOT NULL AND cb.is_returning_player THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0) AS returnee_ratio
    FROM {{ ref('bridge_castaway_season') }} bcs
    JOIN (
        SELECT castaway_id, COUNT(*) > 1 AS is_returning_player
        FROM {{ ref('bridge_castaway_season') }}
        GROUP BY castaway_id
    ) cb ON cb.castaway_id = bcs.castaway_id
    GROUP BY season_key
),
season_misc AS (
    SELECT
        ds.season_key,
        ds.version_season,
        ds.season_number,
        ds.tribe_setup,
        ds.cast_size,
        ds.finalist_count,
        ds.jury_count,
        CASE
            WHEN bounds.max_season > bounds.min_season THEN 1 - (bounds.max_season - ds.season_number)::NUMERIC / NULLIF(bounds.max_season - bounds.min_season, 0)
            ELSE 1
        END AS season_weight,
        (ds.cast_size IS NOT NULL AND ds.cast_size <= 18) AS is_new_era,
        ds.tribe_setup ILIKE '%Edge%' AS twist_edge_of_extinction,
        ds.tribe_setup ILIKE '%Redemption%' AS twist_redemption_island
    FROM {{ ref('dim_season') }} ds
    CROSS JOIN season_bounds bounds
)

SELECT
    cs.snapshot_id,
    sm.season_key,
    sm.version_season,
    jsonb_build_object(
        'season_number', sm.season_number,
        'tribe_setup', sm.tribe_setup,
        'cast_size', sm.cast_size,
        'finalist_count', sm.finalist_count,
        'jury_count', sm.jury_count,
        'season_weight', sm.season_weight,
        'is_new_era', sm.is_new_era,
        'twist_edge_of_extinction', sm.twist_edge_of_extinction,
        'twist_redemption_island', sm.twist_redemption_island,
        'returnee_ratio', COALESCE(rr.returnee_ratio, 0)
    ) AS feature_payload
FROM season_misc sm
CROSS JOIN current_snapshot cs
LEFT JOIN season_returnee_ratio rr ON rr.season_key = sm.season_key;
