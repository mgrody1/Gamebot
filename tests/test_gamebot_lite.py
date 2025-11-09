"""Smoke tests for the packaged gamebot-lite snapshot."""

from gamebot_lite import duckdb_query, load_table


def test_castaway_details_has_rows():
    df = load_table("castaway_details", layer="bronze")
    assert not df.empty
    assert "castaway_id" in df.columns


def test_duckdb_query_runs():
    result = duckdb_query("""
        SELECT
        c.*,
        bo.boot_order_position AS order_voted_out,
        'ABSOLUTELY' AS is_legendary_first_boot
        FROM boot_order AS bo
        INNER JOIN (
        SELECT
            COALESCE(
            cd.full_name,
            cd.full_name_detailed,
            TRIM(concat_ws(' ', cd.castaway, cd.last_name))
            ) AS castaway_name,
            cd.castaway_id,
            c.version_season,
            cd.personality_type,
            cd.occupation,
            cd.pet_peeves,
            c.confessional_count AS first_ep_confessional_count,
            c.confessional_time AS first_ep_confessional_time,
        FROM castaway_details AS cd
        INNER JOIN confessionals AS c
            ON cd.castaway_id = c.castaway_id
        WHERE c.episode = 1
        ) AS c
        ON bo.castaway_id = cd.castaway_id
            AND bo.version_season = cd.version_season
        WHERE (
        cd.castaway_name LIKE '%Zane%' OR
        cd.castaway_name LIKE '%Jelinsky%' OR
        cd.castaway_name LIKE '%Francesca%' OR
        cd.castaway_name LIKE '%Reem%'
        )
        AND bo.boot_order_position = 1
        ORDER BY cd.castaway_name
    """)
    assert not result.empty
    assert {
        "castaway_name",
        "order_voted_out",
        "is_legendary_first_boot",
        "first_ep_confessional_count",
        "first_ep_confessional_time",
    }.issubset(result.columns)
