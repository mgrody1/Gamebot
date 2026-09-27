"""Families 26-27 (scripts/nlp_recap_host.py): recap appearances, and host lines outside tribal council."""

import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
pytest.importorskip("vaderSentiment")
import nlp_recap_host as rh  # noqa: E402
from nlp_features import HOST, Line  # noqa: E402
from nlp_interaction_families import council_segment, host_attention  # noqa: E402


def _ln(t, spk, text="hi", named=(), dur=2.0):
    return Line(t, t + dur, text, 0, speaker=spk, domain="field", named=frozenset(named))


def test_recap_shares():
    lines = [_ln(0, "A", "I run this", dur=4), _ln(5, "B", "A is a threat", named={"A"}), _ln(8, HOST, "previously")]
    r = rh.recap_features(lines, ["A", "B", "C"])
    assert r["A"]["rc_share"] == pytest.approx(4 / 6) and r["A"]["rc_named"] == 1 and r["A"]["rc_named_share"] == 1.0
    assert r["C"]["rc_any"] == 0 and r["B"]["rc_any"] == 1 and r["C"]["rc_n_lines"] == 3


def test_host_outside_tribal_skips_the_council():
    wl = [_ln(0, HOST, "B is struggling on the puzzle", named={"B"}), _ln(10, HOST, "C, how are you?", named={"C"}),
          _ln(900, HOST, "welcome to tribal"), _ln(920, HOST, "A, worried?", named={"A"}), _ln(925, "A", "no")]
    seg = set(id(x) for x in council_segment(wl, 1000))
    per, tot = host_attention([x for x in wl if id(x) not in seg], {"A", "B", "C"})
    assert tot["host_lines"] == 2 and per[("B", "named")] == 1 and per[("C", "q")] == 1 and per[("A", "named")] == 0


def test_end_to_end_on_the_leakage_fixture(tmp_path):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_nlp_features import _dbs
    subs, gb = _dbs(tmp_path)
    labels = tmp_path / "labels.csv"
    labels.write_text("version_season,episode,start_s,end_s,domain,speaker_id,source,confidence\n")
    out = tmp_path / "rh.csv"
    assert rh.main(["--subs", str(subs), "--gamebot", str(gb), "--labels", str(labels), "--out", str(out),
                    "--cues", str(tmp_path / "none.csv")]) == 0
    rows = list(csv.DictReader(open(out)))
    t = [r for r in rows if r["window"] == "tonight" and r["episode"] == "1"]
    assert t and all(r["rc_n_lines"] == "1" for r in t)      # the fixture's recap line ("Previously: ...")
    assert next(r for r in t if r["castaway_id"] == "X1")["rc_named"] == "1"


def test_cue_relative_to_the_window():
    rows = [(i, "A" if i < 4 else "B", {k: (0.5 if (k == "goofy" and i < 4) else 0.1) for k in rh.CUE_KEYS}) for i in range(12)]
    r = rh.cue_rel(rows, ["A", "B", "C"])
    assert r["A"]["mc_goofy"] > 0 > r["B"]["mc_goofy"] and r["A"]["mc_n"] == 4 and "mc_goofy" not in r["C"]
    assert rh.cue_rel(rows[:5], ["A"]) == {"A": {}}
