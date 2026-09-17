"""Data integrity checks on the packaged SQLite snapshot."""

import pytest

from gamebot_lite import load_table

GOLD_TABLES = ["ml_features_non_edit", "ml_features_hybrid"]
KEY = ["castaway_id", "version_season"]


@pytest.mark.parametrize("table", GOLD_TABLES)
def test_gold_has_one_row_per_castaway_season(table):
    df = load_table(table, layer="gold")
    assert not df.duplicated(subset=KEY).any()


def test_gold_matches_castaways_grain():
    gold = load_table("ml_features_non_edit", layer="gold")
    castaways = load_table("castaways", layer="bronze")
    expected = castaways.dropna(subset=["result"]).drop_duplicates(subset=KEY)
    assert len(gold) == len(expected)


def test_castaway_profile_has_one_row_per_castaway_season():
    df = load_table("castaway_profile", layer="silver")
    assert not df.duplicated(subset=KEY).any()


def test_challenge_wins_are_counted():
    perf = load_table("challenge_performance", layer="silver")
    results = load_table("challenge_results", layer="bronze")
    upstream_wins = results["result"].str.lower().str.startswith("won").sum()
    assert perf["won_flag"].sum() > 0
    assert perf["won_flag"].sum() <= upstream_wins


def test_challenge_format_splits_individual_and_team():
    perf = load_table("challenge_performance", layer="silver")
    assert {"individual", "team"} <= set(perf["challenge_format"].unique())


def test_gold_challenge_features_are_populated():
    gold = load_table("ml_features_non_edit", layer="gold")
    assert gold["challenges_won"].sum() > 0
    assert gold["individual_wins"].sum() > 0
    assert gold["team_wins"].sum() > 0
