"""Smoke tests for the packaged gamebot-lite snapshot."""

import pytest

from gamebot_lite import DEFAULT_SQLITE_PATH, duckdb_query, load_table
from gamebot_lite.catalog import friendly_tables_for_layer
from gamebot_lite.client import GamebotClient


def test_duckdb_query_split_vote():
    pytest.importorskip("duckdb")
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
    pytest.importorskip("duckdb")
    result = duckdb_query(
        """
                WITH finalist_confessionals AS (
                    SELECT
                        c.castaway_id,
                        c.version_season,
                        c.castaway,
                        c.winner,
                        SUM(conf.confessional_count) as total_confessionals,
                        SUM(conf.confessional_time) as total_screen_time
                    FROM castaways c
                    JOIN confessionals conf
                        ON c.castaway_id = conf.castaway_id
                        AND c.version_season = conf.version_season
                    WHERE c.finalist = 1
                    GROUP BY c.castaway_id, c.version_season, c.castaway, c.winner
                ),
                jury_vote_counts AS (
                    -- One row per juror x finalist; vote = '1.0' marks the ballot cast.
                    SELECT
                        finalist_id,
                        version_season,
                        COUNT(*) as votes_received
                    FROM jury_votes
                    WHERE vote = '1.0'
                    GROUP BY finalist_id, version_season
                )
                SELECT
                    fc.version_season,
                    fc.castaway,
                    fc.winner,
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
    # Every season's winner received strictly the most jury votes.
    for season, finalists in result.groupby("version_season"):
        winner_votes = finalists.loc[finalists["winner"], "jury_votes"]
        others = finalists.loc[~finalists["winner"], "jury_votes"]
        assert len(winner_votes) == 1, season
        assert (winner_votes.iloc[0] > others).all(), season


def test_duckdb_query_gold_layer():
    pytest.importorskip("duckdb")
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
    duckdb = pytest.importorskip("duckdb")

    with pytest.raises(duckdb.CatalogException, match="not_a_real_table"):
        duckdb_query("SELECT * FROM not_a_real_table LIMIT 1")


def test_schema_introspection_utilities(capsys):
    client = GamebotClient(DEFAULT_SQLITE_PATH)
    tables = client.list_tables()
    assert "castaway_details" in tables
    # Capture output of show_table_schema
    client.show_table_schema("castaway_details")
    captured = capsys.readouterr()
    assert "castaway_id" in captured.out


def test_list_tables_by_layer():
    client = GamebotClient(DEFAULT_SQLITE_PATH)
    assert client.list_tables(layer="silver") == sorted(
        friendly_tables_for_layer("silver")
    )
    assert client.list_tables(layer="metadata") == ["gamebot_ingestion_metadata"]
    assert "ml_features_hybrid" in client.list_tables(layer="gold")
    assert "ml_features_hybrid" not in client.list_tables(layer="bronze")
    with pytest.raises(ValueError, match="Unknown layer"):
        client.list_tables(layer="platinum")


def test_load_table_metadata_prefix():
    df = load_table("metadata.gamebot_ingestion_metadata")
    assert not df.empty
    assert df.attrs["gamebot_layer"] == "metadata"


def test_castaway_details_has_rows():
    df = load_table("castaway_details", layer="bronze")
    assert not df.empty
    assert "castaway_id" in df.columns


def test_duckdb_query_runs():
    pytest.importorskip("duckdb")
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


def test_duckdb_query_layer_prefixes():
    pytest.importorskip("duckdb")
    result = duckdb_query(
        """
        SELECT bo.version_season, cd.full_name, g.target_winner
        FROM bronze.boot_order AS bo
        JOIN bronze.castaway_details AS cd ON cd.castaway_id = bo.castaway_id
        JOIN gold.ml_features_non_edit AS g
            ON g.castaway_id = bo.castaway_id AND g.version_season = bo.version_season
        JOIN silver.season_context AS sc ON sc.version_season = bo.version_season
        LIMIT 5
        """
    )
    assert len(result) == 5
    assert not duckdb_query("SELECT * FROM metadata.gamebot_ingestion_metadata").empty
