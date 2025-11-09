from gamebot_lite import duckdb_query, load_table
from gamebot_lite.client import GamebotClient


def test_duckdb_query_split_vote():
    result = duckdb_query(
        """
                SELECT
                    version_season,
                    COUNT(episode) as count_split_vote_tribals
                FROM vote_history
                WHERE split_vote IS NOT NULL
                    AND split_vote != 'No'
                GROUP BY version_season
                ORDER BY version_season
                """
    )
    assert not result.empty
    assert "version_season" in result.columns
    assert "count_split_vote_tribals" in result.columns


def test_duckdb_query_jury_analysis():
    result = duckdb_query(
        """
                WITH finalist_confessionals AS (
                    SELECT
                        c.castaway_id,
                        c.version_season,
                        c.castaway,
                        SUM(conf.confessional_count) as total_confessionals,
                        SUM(conf.confessional_time) as total_screen_time
                    FROM castaways c
                    JOIN confessionals conf
                        ON c.castaway_id = conf.castaway_id
                        AND c.version_season = conf.version_season
                    WHERE c.finalist = 1
                    GROUP BY c.castaway_id, c.version_season, c.castaway
                ),
                jury_vote_counts AS (
                    SELECT
                        finalist_id,
                        version_season,
                        COUNT(*) as votes_received
                    FROM jury_votes
                    GROUP BY finalist_id, version_season
                )
                SELECT
                    fc.version_season,
                    fc.castaway,
                    fc.total_confessionals,
                    fc.total_screen_time,
                    COALESCE(jv.votes_received, 0) as jury_votes
                FROM finalist_confessionals fc
                LEFT JOIN jury_vote_counts jv
                    ON fc.castaway_id = jv.finalist_id
                    AND fc.version_season = jv.version_season
                ORDER BY fc.version_season, jury_votes DESC
                """
    )
    assert not result.empty
    assert "castaway" in result.columns
    assert "jury_votes" in result.columns


def test_duckdb_query_gold_layer():
    # Use a valid gold table: ml_features_hybrid
    result = duckdb_query(
        """
        SELECT
            *
        FROM ml_features_hybrid
        ORDER BY castaway_id
        LIMIT 10
        """
    )
    assert not result.empty
    assert "castaway_id" in result.columns


def test_duckdb_query_invalid_table():
    import pytest

    with pytest.raises(Exception):
        duckdb_query("SELECT * FROM not_a_real_table LIMIT 1")


def test_schema_introspection_utilities(capsys):
    import pathlib

    # Use the default packaged SQLite path
    client = GamebotClient(
        pathlib.Path(__file__).parent.parent
        / "gamebot_lite"
        / "data"
        / "gamebot.sqlite"
    )
    tables = client.list_tables()
    assert "castaway_details" in tables
    # Capture output of show_table_schema
    client.show_table_schema("castaway_details")
    captured = capsys.readouterr()
    assert "castaway_id" in captured.out


"""Smoke tests for the packaged gamebot-lite snapshot."""


def test_castaway_details_has_rows():
    df = load_table("castaway_details", layer="bronze")
    assert not df.empty
    assert "castaway_id" in df.columns


def test_duckdb_query_runs():
    result = duckdb_query("""
        SELECT
            sub.castaway_name,
            sub.castaway_id_details,
            sub.personality_type,
            sub.occupation,
            sub.pet_peeves,
            sub.first_ep_confessional_count,
            sub.first_ep_confessional_time,
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
                cd.castaway_id AS castaway_id_details,
                cd.personality_type,
                cd.occupation,
                cd.pet_peeves,
                c.confessional_count AS first_ep_confessional_count,
                c.confessional_time AS first_ep_confessional_time
            FROM castaway_details cd
            INNER JOIN confessionals c
                ON cd.castaway_id = c.castaway_id
            WHERE c.episode = 1
        ) AS sub
        ON bo.castaway_id = sub.castaway_id_details
        WHERE (
            sub.castaway_name LIKE '%Zane%' OR
            sub.castaway_name LIKE '%Jelinsky%' OR
            sub.castaway_name LIKE '%Francesca%' OR
            sub.castaway_name LIKE '%Reem%'
        )
        AND bo.boot_order_position = 1
        ORDER BY sub.castaway_name
    """)
    assert not result.empty
    assert {
        "castaway_name",
        "order_voted_out",
        "is_legendary_first_boot",
        "first_ep_confessional_count",
        "first_ep_confessional_time",
    }.issubset(result.columns)
