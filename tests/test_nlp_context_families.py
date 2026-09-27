"""Families 18-24 (scripts/nlp_context_families.py): named while absent, two-person scenes, ironic cuts, the music
bed against the window, airtime history, narrating others and asking about the vote."""

import csv
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
pytest.importorskip("vaderSentiment")
import nlp_context_families as cx  # noqa: E402
from nlp_features import HOST, Line  # noqa: E402

CAST = {"A", "B", "C", "D"}
TRIBE = {"A": "red", "B": "red", "C": "red", "D": "blue"}


def _ln(t, spk, text="hi there", conf=False, named=()):
    return Line(t, t + 1.5, text, int(conf), speaker=spk, domain="confessional" if conf else "field", named=frozenset(named))


def test_behind_their_back_and_dyads():
    lines = [_ln(0, "A", "we vote out Cara tonight", named={"C"}), _ln(2, "B", "yes, Cara goes", named={"C"}),   # C absent
             _ln(30, "C", "hey"), _ln(32, "A", "Cara, you good?", named={"C"}), _ln(34, HOST, "come on in"),      # C present
             _ln(60, "B", "hello"), _ln(62, "D", "hi")]                                                         # B-D dyad
    w = cx.window_stats(lines, CAST, TRIBE)
    r = cx.derive(w, sorted(CAST), TRIBE, "audio")
    assert r["C"]["bb_absent_n"] == 2 and r["C"]["bb_absent_share"] == pytest.approx(2 / 3)
    assert r["C"]["bb_absent_vote_n"] == 1 and r["C"]["bb_absent_vote_share"] == 1.0 and r["C"]["bb_absent_namers"] == 2
    assert w.tot["dyads"] == 2                        # A-B, then B-D (the host scene with A and C does not count)
    assert r["B"]["dy_n"] == 2 and r["B"]["dy_partners"] == 2 and r["C"]["dy_n"] == 0
    assert r["C"]["dy_left_out"] == 0                 # only one red-red dyad happened


def test_ironic_cut_and_questions():
    lines = [_ln(0, "A", "I'm safe, nobody's coming for me", conf=True),
             _ln(40, "B", "tonight we vote Alice", named={"A"}),
             _ln(300, "C", "who are we voting for?"), _ln(302, "C", "ok")]
    r = cx.derive(cx.window_stats(lines, CAST, TRIBE), sorted(CAST), TRIBE, "audio")
    assert r["A"]["ic_claims"] == 1 and r["A"]["ic_hits"] == 1 and r["A"]["ic_min_gap"] == pytest.approx(38.5)
    assert r["C"]["ask_n"] == 1 and r["B"]["ask_n"] == 0


def test_music_relative_and_trajectory():
    rows = [(0, "A", {"mvr": -5.0, "low_share": 0.6, "minor": 0.2, "tonal": 0.5})] + \
           [(i, "B", {"mvr": -15.0, "low_share": 0.2, "minor": -0.1, "tonal": 0.5}) for i in range(1, 6)]
    m = cx.music_rel(rows, "A")
    assert m["mu_n"] == 1 and m["mu_minor"] > 0 and m["mu_mvr"] > 0 and m["mu_low"] > 0
    t = cx.trajectory([(0.1, 2), (0.0, 0), (0.0, 0), (0.05, 1)], 0.3)
    assert t["tr_zero_streak"] == 2 and t["tr_zero_now"] == 0 and t["tr_spike"] > 5 and t["tr_eps"] == 4
    assert t["tr_slope"] == pytest.approx(np.polyfit(range(4), [0.1, 0, 0, 0.05], 1)[0])


def test_end_to_end_on_the_leakage_fixture(tmp_path):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_nlp_features import _dbs
    subs, gb = _dbs(tmp_path)
    labels = tmp_path / "labels.csv"
    labels.write_text("version_season,episode,start_s,end_s,domain,speaker_id,source,confidence\n")
    out = tmp_path / "cx.csv"
    assert cx.main(["--subs", str(subs), "--gamebot", str(gb), "--labels", str(labels), "--out", str(out),
                    "--music", str(tmp_path / "none.csv")]) == 0
    rows = list(csv.DictReader(open(out)))
    assert {r["window"] for r in rows} == {"tonight", "prior"} and set(cx.FEATURES) <= set(rows[0])
