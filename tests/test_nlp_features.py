"""The leakage rule for scripts/nlp_features.py: a council's `tonight` window stops at its cut, `prior` never sees the
episode itself, recaps count nowhere, and a second council starts at the first one's tally."""

import csv
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
pytest.importorskip("vaderSentiment")
import nlp_features  # noqa: E402


def _dbs(tmp: Path) -> tuple[Path, Path]:
    gb = tmp / "gamebot.sqlite"
    g = sqlite3.connect(gb)
    g.executescript("""
    create table season_summary (version text, version_season text, season int);
    create table castaways (castaway_id text, castaway text, version_season text);
    create table castaway_details (castaway_id text, full_name text, full_name_detailed text);
    create table boot_mapping (version_season text, episode int, castaway_id text, tribe text, boot_mapping_order int);
    insert into season_summary values ('US', 'US99', 99);
    insert into castaways values ('X1', 'Alice', 'US99'), ('X2', 'Bob', 'US99'), ('X3', 'Cara', 'US99');
    insert into castaway_details values ('X1', 'Alice Ames', null), ('X2', 'Bob Burns', null), ('X3', 'Cara Cole', null);
    """)
    for ep in (1, 2):
        g.executemany("insert into boot_mapping values ('US99', ?, ?, ?, 1)",
                      [(ep, "X1", "Red"), (ep, "X2", "Red"), (ep, "X3", "Blue")])
    g.commit()
    subs = tmp / "subtitles.sqlite"
    s = sqlite3.connect(subs)
    s.executescript("""
    create table cues (version_season text, episode int, idx int, start_s real, end_s real, text text, sdh_name text,
                       speaker_id text, italic int, turn int, sound int);
    create table recaps (version_season text, episode int, recap_end_s real, method text);
    create table tribals (version_season text, episode int, k int, castaway_id text, castaway text, cut_s real,
                          cut_method text, tally_s real, boot_named_after_tally int);
    insert into recaps values ('US99', 1, 10, 'x'), ('US99', 2, 10, 'x');
    insert into tribals values ('US99', 1, 1, 'X3', 'Cara', 100, 'time to vote', 150, 1),
                               ('US99', 1, 2, 'X1', 'Alice', 300, 'time to vote', 350, 1);
    """)
    cues = [
        (1, 0, 5, 6, "Previously: Alice and Cara", None, None),          # recap: counts nowhere
        (1, 1, 20, 22, "Alice is a threat.", "BOB", "X2"),               # Bob names Alice, before the first cut
        (1, 2, 50, 52, "Cara is dangerous.", None, None),                 # unknown speaker names Cara
        (1, 3, 120, 122, "Bob. Bob. Bob.", None, None),                   # after cut 1, before tally 1: nowhere tonight
        (1, 4, 200, 202, "Alice has to go.", None, None),                 # second council's window (150-300)
        (1, 5, 320, 322, "Cara, Cara.", None, None),                      # after cut 2
        (2, 0, 30, 32, "Bob again.", None, None),                         # episode 2: never in episode 2's prior
    ]
    s.executemany("insert into cues values ('US99', ?, ?, ?, ?, ?, ?, ?, 0, 0, 0)", cues)
    s.commit()
    return subs, gb


def _run(tmp: Path) -> list[dict]:
    subs, gb = _dbs(tmp)
    labels = tmp / "labels.csv"
    labels.write_text("version_season,episode,start_s,end_s,domain,speaker_id,source,confidence\n")
    out = tmp / "out.csv"
    assert nlp_features.main(["--subs", str(subs), "--gamebot", str(gb), "--labels", str(labels), "--out", str(out)]) == 0
    with open(out) as f:
        return list(csv.DictReader(f))


def _row(rows, window, ep, cid, k=""):
    return next(r for r in rows if r["window"] == window and r["episode"] == str(ep) and r["castaway_id"] == cid and r["k"] == str(k))


def test_tonight_stops_at_the_cut_and_prior_never_sees_the_episode(tmp_path):
    rows = _run(tmp_path)
    # council 1: recap end (10) to cut (100): Alice once (named by Bob), Cara once, Bob never
    assert _row(rows, "tonight", 1, "X1", 1)["m_n"] == "1"
    assert _row(rows, "tonight", 1, "X3", 1)["m_n"] == "1"
    assert _row(rows, "tonight", 1, "X2", 1)["m_n"] == "0"
    assert _row(rows, "tonight", 1, "X1", 1)["m_namers"] == "1"          # Bob, a known speaker
    assert _row(rows, "tonight", 1, "X2", 1)["own_lines"] == "1"
    # council 2: the first tally (150) to the second cut (300): only "Alice has to go."
    assert _row(rows, "tonight", 1, "X1", 2)["m_n"] == "1"
    assert _row(rows, "tonight", 1, "X2", 2)["m_n"] == "0" and _row(rows, "tonight", 1, "X3", 2)["m_n"] == "0"
    # prior of episode 1 is empty; prior of episode 2 is all of episode 1's body and nothing of episode 2
    assert all(_row(rows, "prior", 1, c)["m_n"] == "0" for c in ("X1", "X2", "X3"))
    assert _row(rows, "prior", 2, "X1")["m_n"] == "2"                    # the recap line is not counted
    assert _row(rows, "prior", 2, "X2")["m_n"] == "1"                    # "Bob. Bob. Bob." once; "Bob again." never
    assert _row(rows, "prior", 2, "X3")["m_n"] == "2"


def test_windows_chain_councils():
    r = lambda k, cut, tally: {"k": k, "cut_s": cut, "tally_s": tally}  # noqa: E731
    assert nlp_features.windows(10.0, [r(2, 300, 350), r(1, 100, 150)]) == [(1, 10.0, 100), (2, 150, 300)]
    assert nlp_features.windows(10.0, [r(1, None, 150), r(2, 300, None)]) == [(2, 150, 300)]
