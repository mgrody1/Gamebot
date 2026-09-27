"""Families 4-7 (scripts/nlp_edit_families.py): confessional counting, first / last / late confessionals, topics,
style, the boot model's leakage rule, and the end-to-end rows on the leakage fixture."""

import csv
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
pytest.importorskip("vaderSentiment")
import nlp_edit_families as ed  # noqa: E402
from nlp_features import Line  # noqa: E402

CAST = {"A", "B", "C"}


def _ln(t, spk, text="hello there friend", conf=False, dur=1.5):
    return Line(t, t + dur, text, int(conf), speaker=spk, domain="confessional" if conf else "field")


def test_airtime_confessionals_and_placement():
    lines = [_ln(0, "A", conf=True), _ln(2, "A", conf=True),        # one confessional in two lines
             _ln(10, "B"), _ln(12, "C"), _ln(14, "B"),               # a camp scene with B and C
             _ln(40, "B", conf=True),                                # B's confessional
             _ln(90, "C", conf=True, dur=4)]                         # C's, in the last quarter (75-100)
    st = ed.window_stats(lines, CAST, 0.0, 100.0)
    rows = ed.derive(st, sorted(CAST), "audio", tonight=True, closer="B", prior=ed.Stats())
    assert st.c["A"]["conf_n"] == 1 and st.c["B"]["conf_n"] == 1 and st.tot["conf_n"] == 3
    assert rows["A"]["conf_first"] == 1 and rows["C"]["conf_last"] == 1 and rows["B"]["conf_last"] == 0
    assert rows["C"]["conf_late_share"] == 1.0 and rows["A"]["conf_late_share"] == 0
    assert rows["B"]["closer_prev"] == 1 and rows["A"]["closer_prev"] == 0
    assert rows["B"]["scene_n"] == 1 and rows["A"]["scene_n"] == 0 and rows["B"]["scene_share"] == 1.0
    assert rows["A"]["conf_share"] == pytest.approx(3.0 / 8.5) and rows["A"]["camp_share"] == 0
    assert sum(r["air_share"] for r in rows.values()) == pytest.approx(1.0)


def test_topics_and_style():
    talk = ["we need the numbers to blindside him at tribal"] * 3 + ["my wife and kids are home"] * 2
    lines = [_ln(i * 3, "A", t) for i, t in enumerate(talk)]
    st = ed.window_stats(lines, CAST)
    r = ed.derive(st, sorted(CAST), "audio", tonight=False)["A"]
    assert r["top_strategy"] == pytest.approx(0.6) and r["top_backstory"] == pytest.approx(0.4) and r["top_camp"] == 0
    assert 0 < r["top_entropy"] < 1
    s = ed.style(["the cat sat on the mat"] * 10)
    assert s["sty_gzip"] < 0.5 and s["sty_mattr"] < 0.3                # repetitive speech compresses and repeats
    fresh = ed.style(["alpha beta gamma delta epsilon zeta eta theta iota kappa"] * 5, prior_vocab={"alpha"} | {f"w{i}" for i in range(120)})
    assert fresh["sty_novelty"] == pytest.approx(0.9)


def test_boot_model_only_learns_from_finished_seasons():
    bm = ed.BootModel()
    up, down = np.array([1.0, 0.0]), np.array([0.0, 1.0])
    for _ in range(25):
        bm.note(1, {"all": up}, ["i am safe tonight"])
        bm.note(0, {"all": down}, ["we won immunity"])
    assert bm.score({"all": up}, []) == {}                    # the season is not over: nothing fitted yet
    bm.season_over()
    s_up, s_down = bm.score({"all": up}, [])["boot_sim_all"], bm.score({"all": down}, [])["boot_sim_all"]
    assert s_up > 0 > s_down


def test_end_to_end_on_the_leakage_fixture(tmp_path):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_nlp_features import _dbs
    subs, gb = _dbs(tmp_path)
    labels = tmp_path / "labels.csv"
    labels.write_text("version_season,episode,start_s,end_s,domain,speaker_id,source,confidence\n")
    out = tmp_path / "ed.csv"
    assert ed.main(["--subs", str(subs), "--gamebot", str(gb), "--labels", str(labels), "--out", str(out), "--no-embed"]) == 0
    rows = list(csv.DictReader(open(out)))
    assert {r["window"] for r in rows} == {"tonight", "prior"} and set(ed.FEATURES) <= set(rows[0])
    bob1 = next(r for r in rows if r["window"] == "tonight" and r["episode"] == "1" and r["k"] == "1" and r["castaway_id"] == "X2")
    assert bob1["air_lines"] == "1"                            # "Alice is a threat." before the first cut
    bob2 = next(r for r in rows if r["window"] == "prior" and r["episode"] == "2" and r["castaway_id"] == "X2")
    assert bob2["air_lines"] == "1"                            # episode 1 only; nothing of episode 2
