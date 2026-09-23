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


def test_castaway_profile_season_name_matches_season_summary():
    profile = load_table("castaway_profile", layer="silver")
    seasons = load_table("season_summary", layer="bronze")
    expected = seasons.set_index("version_season")["season_name"]
    actual = profile.drop_duplicates("version_season").set_index("version_season")[
        "season_name"
    ]
    assert actual.equals(expected.loc[actual.index])


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


def test_jury_votes_received_counts_ballots():
    """KNOWN_ISSUES #8: a finalist's jury votes are the ballots cast for them, not the number of jurors."""
    gold = load_table("ml_features_non_edit", layer="gold")
    ballots = load_table("jury_votes", layer="bronze")
    cast = ballots[ballots["vote"].astype(str) == "1.0"]
    expected = cast.groupby(["finalist_id", "version_season"]).size()
    got = gold.set_index(KEY)["jury_votes_received"].fillna(0)
    for (finalist, season), n in expected.items():
        assert got.get((finalist, season), 0) == n, (finalist, season)
    assert got.sum() == len(cast)


def test_voted_for_winner_is_a_vote():
    """KNOWN_ISSUES #9: voted_for_winner is 1 only on the row where the juror's ballot went to the winner."""
    ja = load_table("jury_analysis", layer="silver")
    assert (
        ja["voted_for_winner"] <= (ja["voted_for_finalist_name"].astype(str) == "1.0")
    ).all()
    assert (
        ja["voted_against_winner"]
        <= (ja["voted_for_finalist_name"].astype(str) == "1.0")
    ).all()
    assert ((ja["voted_for_winner"] + ja["voted_against_winner"]) <= 1).all()
    # a juror's flags add up to their ballots in bronze: one each, except SA0077 in SA05, whom survivoR records with two
    ballots = load_table("jury_votes", layer="bronze")
    cast = (
        ballots[ballots["vote"].astype(str) == "1.0"]
        .groupby(["castaway_id", "version_season"])
        .size()
    )
    per_juror = (
        ja.groupby(["castaway_id", "version_season"])[
            ["voted_for_winner", "voted_against_winner"]
        ]
        .sum()
        .sum(axis=1)
    )
    assert per_juror.equals(
        cast.reindex(per_juror.index).fillna(0).astype(per_juror.dtype)
    )
