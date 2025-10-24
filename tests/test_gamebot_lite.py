"""Smoke tests for the packaged gamebot-lite snapshot."""

from gamebot_lite import duckdb_query, load_table


def test_castaway_details_has_rows():
    df = load_table("castaway_details", layer="bronze")
    assert not df.empty
    assert "castaway_id" in df.columns


def test_duckdb_query_runs():
    result = duckdb_query(
        """
        SELECT season_name, COUNT(*) AS castaways
        FROM silver.castaway_season_profile
        GROUP BY season_name
        ORDER BY castaways DESC
        LIMIT 5
        """
    )
    assert not result.empty
    assert {"season_name", "castaways"}.issubset(result.columns)
