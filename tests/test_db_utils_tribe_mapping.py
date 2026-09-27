"""_repair_tribe_mapping_ids: one castaway_id stamped on a whole tribe is re-resolved by name, per season."""

import pandas as pd

import gamebot_core.db_utils as du


def test_misfiled_group_moves_to_the_season_whose_cast_it_is(monkeypatch):
    ref = pd.DataFrame({"castaway_id": ["US0550", "US0009", "US0555", "US0763", "US0768", "US0753"],
                        "castaway": ["Christian", "Jenna", "Mike", "Jenna", "Mike", "Alexis"],
                        "version_season": ["US50", "US50", "US50", "US51", "US51", "US51"]})
    monkeypatch.setattr(du, "fetch_existing_keys", lambda table, conn, cols: ref[cols])
    df = pd.DataFrame({"castaway_id": ["US0550"] * 3 + ["US0550"] * 2 + ["US0550"],
                       "castaway": ["Jenna", "Mike", "Alexis", "Zed", "Mike", "Christian"],
                       "version_season": ["US50"] * 6, "season": [50.0] * 6,
                       "episode": [1, 1, 1, 1, 1, 2], "tribe": ["Toka"] * 3 + ["Savu"] * 2 + ["Cila"], "day": [3] * 6})
    out = du._repair_tribe_mapping_ids(df, None)
    # Jenna and Mike are US50 names too, but only US51 holds all three: the group moves there, with US51 ids
    assert list(out.castaway_id) == ["US0763", "US0768", "US0753", "US0550"]
    assert list(out.version_season) == ["US51", "US51", "US51", "US50"] and out.season.iloc[0] == 51.0
    assert not out.duplicated(subset=["castaway_id", "version_season", "episode", "tribe", "day"]).any()   # Zed's group dropped


def test_clean_data_is_untouched(monkeypatch):
    monkeypatch.setattr(du, "fetch_existing_keys", lambda *a: (_ for _ in ()).throw(AssertionError("no lookup needed")))
    df = pd.DataFrame({"castaway_id": ["A", "B"], "castaway": ["a", "b"], "version_season": ["US50"] * 2,
                       "episode": [1, 1], "tribe": ["T", "T"], "day": [3, 3]})
    assert du._repair_tribe_mapping_ids(df, None) is df
