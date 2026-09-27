"""Families 8-10 (scripts/nlp_speaker_families.py): scenes and exchanges, the agenda record, tribe similarity, and
the leakage rule (a council's result only counts for later councils)."""

import csv
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
pytest.importorskip("vaderSentiment")
import nlp_speaker_families as fam  # noqa: E402
from nlp_features import Line  # noqa: E402

CAST = {"A", "B", "C", "D"}


def _ln(t, spk, text="hello there", domain=None, named=()):
    return Line(t, t + 1.5, text, int(domain == "confessional"), speaker=spk, domain=domain, named=frozenset(named))


def test_scenes_and_exchanges():
    lines = [_ln(0, "A"), _ln(2, "B"), _ln(4, "A"), _ln(6, "HOST"), _ln(8, "C"),   # A-B twice; HOST breaks C from A
             _ln(10, "D", domain="confessional"),                                   # a confessional ends the scene
             _ln(12, "C"), _ln(30, "B")]                                            # 16 s pause: new scene, no C-B
    w = fam.window_stats(lines, None, CAST)
    assert w.edges[frozenset("AB")] == 2 and sum(w.edges.values()) == 2
    assert w.n_scenes == 1 and w.scenes["A"] == 1 and w.scenes["C"] == 1 and w.scenes["D"] == 0
    assert w.turns["D"] == 0 and w.turns["C"] == 2


def test_agenda_credits_only_after_the_vote():
    ag = fam.Agenda()
    talk = [_ln(0, "A", "vote out Cara", named={"C"}), _ln(5, "B", "we target Cara", named={"C"}),
            _ln(9, "B", "or vote Dan", named={"D"})]
    w = fam.window_stats(talk, None, CAST)
    before = fam.derive(w, ag, sorted(CAST), {c: "x" for c in CAST}, "audio")
    assert before["A"]["ag_first_n"] == 0 and before["C"]["ag_named_vt"] == 2 and before["C"]["ag_first_named"] == 1
    ag.record(talk, CAST, "C")                    # Cara went: A named her first, B's talk was 1 of 2 on target
    assert ag.first["A"] == 1 and ag.first["B"] == 0 and ag.vt_hit["B"] == 1 and ag.vt_total["B"] == 2
    after = fam.derive(fam.window_stats([_ln(0, "A", "vote Dan", named={"D"})], None, CAST), ag, sorted(CAST),
                       {c: "x" for c in CAST}, "audio")
    assert after["D"]["ag_power_named"] == pytest.approx(ag.power("A")) and ag.power("A") > ag.power("B")


def test_tribe_outlier():
    rng = np.random.default_rng(0)
    base, odd = rng.normal(size=16), rng.normal(size=16)
    vec = {"A": base, "B": base + 0.1 * rng.normal(size=16), "C": base + 0.1 * rng.normal(size=16), "D": odd}
    bucket = {c: [np.tile(v, (3, 1)).sum(0), 3] for c, v in vec.items()}
    s = fam.semantic(bucket, sorted(CAST), {c: "red" for c in CAST})
    assert s["D"]["rank"] == 0 and s["D"]["dev"] < 0 < s["A"]["dev"]
    assert s["D"]["tribe"] < min(s[c]["tribe"] for c in "ABC")


def test_end_to_end_on_the_leakage_fixture(tmp_path):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from test_nlp_features import _dbs
    subs, gb = _dbs(tmp_path)
    labels = tmp_path / "labels.csv"
    labels.write_text("version_season,episode,start_s,end_s,domain,speaker_id,source,confidence\n")
    out = tmp_path / "fam.csv"
    fake = lambda key, texts: np.eye(len(texts), 8, dtype=np.float32)  # noqa: E731
    assert fam.main(["--subs", str(subs), "--gamebot", str(gb), "--labels", str(labels), "--out", str(out)], embed=fake) == 0
    rows = list(csv.DictReader(open(out)))
    assert {r["window"] for r in rows} == {"tonight", "prior"}
    t1 = [r for r in rows if r["window"] == "tonight" and r["episode"] == "1" and r["k"] == "1"]
    assert all(r["ag_first_n"] == "0" for r in t1)          # no council has happened yet at the first cut
    assert set(fam.FEATURES) <= set(rows[0])
