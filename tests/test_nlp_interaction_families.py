"""Families 11-17 (scripts/nlp_interaction_families.py): style matching, idea uptake, the floor and cut-ins,
duplicity, the tribal council segment and host questions, laughter tags, voice against the player's own baseline."""

import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
pytest.importorskip("vaderSentiment")
import nlp_interaction_families as it  # noqa: E402
from nlp_features import HOST, Line  # noqa: E402

CAST = {"A", "B", "C"}
TONE = {"great": 0.8, "awful": -0.8}


def tone(ln):
    return next((v for k, v in TONE.items() if k in ln.text), 0.0)


def _ln(t, spk, text, conf=False, named=(), dur=1.5):
    return Line(t, t + dur, text, int(conf), speaker=spk, domain="confessional" if conf else "field", named=frozenset(named))


def test_uptake_floor_and_cut_ins():
    lines = [_ln(0, "A", "we should split the votes between them tonight"),
             _ln(2, "B", "I think we split votes, yeah --", dur=3),
             _ln(5.2, "C", "Wait, hold on"),                                       # cuts in right after "--"
             _ln(7, "B", "okay split votes it is")]
    w = it.window_stats(lines, CAST, tone, None)
    r = it.derive(w, sorted(CAST), {c: "x" for c in CAST}, "audio")
    assert r["A"]["uptake_n"] == 1 and r["B"]["echo_n"] == 1 and r["A"]["uptake_share"] == 1.0   # "split votes"
    assert r["C"]["cut_in_rate"] is None and w.c["C"]["cut_in"] == 1 and w.c["B"]["cut_off"] == 1
    assert r["B"]["floor_share"] > r["C"]["floor_share"] and r["B"]["floor_rel"] > 1 > r["C"]["floor_rel"]


def test_style_matching_needs_words_on_both_sides():
    same = "I think that we are really going to do it and it is the plan for us"
    lines = [_ln(i * 2, "A" if i % 2 == 0 else "B", same) for i in range(8)]
    r = it.derive(it.window_stats(lines, CAST, tone, None), sorted(CAST), {"A": "x", "B": "x", "C": "y"}, "audio")
    assert r["A"]["lsm_mean"] == pytest.approx(1.0) and r["A"]["lsm_tribe"] == pytest.approx(1.0) and r["C"]["lsm_mean"] is None


def test_duplicity_is_nice_at_camp_and_awful_in_confessional():
    lines = [_ln(0, "A", "you are great", named={"B"}), _ln(2, "B", "thanks"), _ln(4, "A", "great day"),
             _ln(30, "A", "B is awful", conf=True, named={"B"})]
    r = it.derive(it.window_stats(lines, CAST, tone, None), sorted(CAST), {c: "x" for c in CAST}, "audio")
    assert r["A"]["dup_out"] == pytest.approx(1.6) and r["B"]["dup_in"] == pytest.approx(1.6) and r["B"]["dup_in_n"] == 1


def test_council_segment_and_host_questions():
    lines = [_ln(0, HOST, "come on in"), _ln(300, HOST, "welcome to tribal"),     # 300 s gap: tribal starts at 300
             _ln(320, HOST, "A, are you worried?", named={"A"}), _ln(323, "A", "no"),
             _ln(340, HOST, "how is camp?"), _ln(343, "B", "fine"),
             _ln(360, HOST, "C looks nervous", named={"C"})]
    seg = it.council_segment(lines, 400)
    assert seg[0].start == 300
    per, tot = it.host_attention(seg, CAST)
    assert tot["host_q"] == 2 and per[("A", "q")] == 1 and per[("B", "q")] == 1 and per[("C", "named")] == 1


def test_laughs_attach_to_the_line():
    lines = [_ln(0, "A", "that is so funny"), _ln(10, "B", "no")]
    w = it.window_stats(lines, CAST, tone, [(1.0, "(laughs)"), (2.5, "(laughter)"), (30, "(laughs)")])
    r = it.derive(w, sorted(CAST), {c: "x" for c in CAST}, "audio")
    assert r["A"]["laugh_n"] == 2 and r["A"]["laugh_self_n"] == 1 and r["A"]["laugh_group_n"] == 1 and r["B"]["laugh_n"] == 0
    assert it.derive(it.window_stats(lines, CAST, tone, None), sorted(CAST), {}, "audio")["A"]["laugh_n"] is None


def test_voice_against_own_baseline():
    base = [{"f0_med": 200.0 + i, "f0_range_st": 5.0, "rms_db": -20.0, "rate_wps": 3.0} for i in range(-5, 5)]
    z = it.voice_z([{"f0_med": 220.0, "f0_range_st": 5.0, "rms_db": -20.0, "rate_wps": None}], base)
    assert z["v_n"] == 1 and z["v_pitch_z"] > 5 and "v_rate_z" not in z
    assert "v_pitch_z" not in it.voice_z([{"f0_med": 220.0, "f0_range_st": 5, "rms_db": -20, "rate_wps": 3}], base[:3])


def test_end_to_end_on_the_leakage_fixture(tmp_path):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_nlp_features import _dbs
    subs, gb = _dbs(tmp_path)
    labels = tmp_path / "labels.csv"
    labels.write_text("version_season,episode,start_s,end_s,domain,speaker_id,source,confidence\n")
    out = tmp_path / "it.csv"
    assert it.main(["--subs", str(subs), "--gamebot", str(gb), "--labels", str(labels), "--out", str(out),
                    "--no-laughs", "--voice", str(tmp_path / "none.csv")]) == 0
    rows = list(csv.DictReader(open(out)))
    assert {r["window"] for r in rows} == {"tonight", "prior"} and set(it.FEATURES) <= set(rows[0])
