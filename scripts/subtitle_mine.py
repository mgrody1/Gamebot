#!/usr/bin/env python3
"""
What the subtitles say without knowing who is speaking: a text layer beside the warehouse.

    python3 scripts/subtitle_mine.py                        # from Gamebot/; stdlib only
    python3 scripts/subtitle_mine.py --speakers PATH        # another survspk.sqlite

Reads the survivor-speakers database (subtitle cues for every US episode, read-only) and the warehouse's
castaways, and writes data_cache/subtitles/subtitles.sqlite (git-ignored, and outside gamebot_lite/data so the
package never ships it):

  cues      the cues, consecutive duplicates dropped, with the subtitle's own speaker name resolved to a castaway
            where the captioner wrote one (S1-2, S5-9, S40-50 name the cast), italics (off-camera voice-over,
            almost always a confessional), dash turns and sound captions
  recaps    where the "previously on" recap ends, so last week's names do not count toward this week
  tribals   each vote in an episode: when Probst sends them to vote (the cut), when he goes to tally, and whether
            the voted-out castaway's name is heard in the reading that follows (a self-check on the pairing)
  mentions  per castaway and vote: how often their name is said between the recap and the cut, how often in a
            line about voting, idols or blindsides, and how often in a confessional line; plus whole-episode counts
  quotes    lines the captioner attributed by name, joined with the unnamed cues that follow until the speaker
            changes, with the checks a quote needs before it is trivia

and data_cache/subtitles/subtitle_mentions.csv, the mentions table alone (counts, no text), for the boot model.

The subtitles are the show's dialogue: subtitles.sqlite stays on this machine and out of the site's export.
Only counts, model outputs and single short attributed quotes are published.
"""

from __future__ import annotations

import collections
import csv
import json
import re
import shutil
import sqlite3
import tempfile
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STARGAZER = ROOT.parent
SPEAKERS = STARGAZER / "survivor_audio/db/survspk.sqlite"
ALIASES = STARGAZER / "survivor-speakers/config/aliases.yaml"
GAMEBOT = ROOT / "gamebot_lite/data/gamebot.sqlite"
OUT = ROOT / "data_cache/subtitles/subtitles.sqlite"          # git-ignored and outside the packaged gamebot_lite/data
CSV = ROOT / "data_cache/subtitles/subtitle_mentions.csv"

TIME_TO_VOTE = re.compile(r"time to vote|it is time to vote|go vote\b", re.I)
TALLY = re.compile(r"tally the votes|read the votes|go get the votes|i'?ll read (them|the)", re.I)
VERDICT = re.compile(r"tribe has spoken", re.I)
PREVIOUSLY = re.compile(r"previously on", re.I)
TARGET = re.compile(r"\b(vote|voted|voting|votes|target|targets|blindside|blindsided|get rid of|send (him|her|them) home|"
                     r"out next|go home|goes home|going home|idol|flip|flipping|alliance|numbers)\b", re.I)
# names that are also words a sentence can start with: counted only when not the first word of a line
WORDLIKE = {"Will", "Chase", "Hope", "Grant", "Rich", "Dawn", "Summer", "Sugar", "Bill", "Frank", "Christian", "Joe",
            "Sandy", "Ken", "Mark", "Ray", "Rob", "Jean", "Angie", "Mike", "Dan", "Pete", "Sierra", "Brandon", "Jay"}


def aliases() -> dict[str, dict[str, str]]:
    """The speaker pipeline's per-season caption names (aliases.yaml, seasons: section), read without yaml."""
    out, season = {}, None
    if not ALIASES.exists():
        return out
    for line in ALIASES.read_text().splitlines():
        m = re.match(r"^  (US\d\d):\s*$", line)
        if m:
            season = m.group(1); out[season] = {}
            continue
        m = re.match(r"^    ([A-Z0-9 .'#-]+):\s*(\S+)", line)
        if season and m:
            out[season][m.group(1).strip()] = m.group(2)
    return out


def norm(tok: str) -> str:
    t = re.sub(r"\([^)]*\)", "", tok.upper())
    t = re.sub(r"[^A-Z0-9.'\- #]", " ", t)
    return re.sub(r"\s+", " ", t).strip(" .-")


def main() -> int:
    args = sys.argv[1:]
    speakers = Path(args[args.index("--speakers") + 1]) if "--speakers" in args else SPEAKERS
    src = sqlite3.connect(f"file:{speakers}?mode=ro", uri=True)
    gb = sqlite3.connect(f"file:{GAMEBOT}?mode=ro", uri=True)
    alias = aliases()
    # built in a temporary file and copied into place: SQLite's journal does not work on every mounted folder
    work = Path(tempfile.mkdtemp()) / "subtitles.sqlite"
    out = sqlite3.connect(work)
    out.executescript("""
    create table cues (version_season text, episode int, idx int, start_s real, end_s real, text text, sdh_name text,
                       speaker_id text, italic int, turn int, sound int);
    create table recaps (version_season text, episode int, recap_end_s real, method text);
    create table tribals (version_season text, episode int, k int, castaway_id text, castaway text, cut_s real,
                          cut_method text, tally_s real, boot_named_after_tally int);
    create table mentions (version_season text, episode int, k int, castaway_id text, episode_mentions int,
                           pre_mentions int, pre_target_mentions int, pre_confessional_mentions int, pre_cues int);
    create table quotes (version_season text, episode int, start_s real, speaker_id text, speaker text, text text,
                         n_words int, n_cues int, italic_share real, names_anyone int, clean_start int, clean_end int);
    """)
    stats = collections.Counter()
    gaps = []                                     # time_to_vote -> tally, for the fallback cut
    pending = []                                  # tribals whose cut needs the fallback, filled after the gap is known

    seasons = [r[0] for r in gb.execute("select version_season from season_summary where version = 'US' order by season")]
    for vs in seasons:
        cast = gb.execute("""select c.castaway_id, c.castaway, d.full_name, d.full_name_detailed from castaways c
                             join castaway_details d using (castaway_id) where c.version_season = ?""", (vs,)).fetchall()
        name_of = {cid: short for cid, short, *_ in cast}
        # caption names -> castaway: the season's aliases, then short name, full name, first name when unique
        resolve, firsts = {}, collections.defaultdict(set)
        for cid, short, full, full_d in cast:
            resolve[norm(short)] = cid
            for f in (full, full_d):
                if f:
                    resolve[norm(f)] = cid
                    firsts[norm(f).split(" ")[0]].add(cid)
        for tok, cids in firsts.items():
            if len(cids) == 1:
                resolve.setdefault(tok, next(iter(cids)))
        for tok, cid in alias.get(vs, {}).items():
            resolve[norm(tok)] = cid if cid.startswith("US") else None
        # spoken names -> castaway: short name, first name, aliases; a token two castaways share counts for neither
        tokens = collections.defaultdict(set)
        for cid, short, full, full_d in cast:
            for t in {short, (full or "").split(" ")[0], (full_d or "").split(" ")[0]}:
                if t and len(t) >= 2:
                    tokens[t.strip(".")].add(cid)
        for tok, cid in alias.get(vs, {}).items():
            if cid.startswith("US") and len(tok) >= 3:
                tokens[tok.title()].add(cid)
        pats = {}
        for tok, cids in tokens.items():
            if len(cids) == 1:
                pats.setdefault(next(iter(cids)), []).append(tok)
        spoken = _patterns(gb, alias, vs)

        boots_by_ep = collections.defaultdict(list)
        for cid, ep, order in gb.execute("""select castaway_id, episode, castaways_order from castaways
                                            where version_season = ? and result like '%voted out%' order by castaways_order""", (vs,)):
            boots_by_ep[ep].append(cid)

        for (ep,) in src.execute("select distinct episode from cues where version_season = ? order by episode", (vs,)):
            rows, prev = [], None
            for idx, start, end, lines in src.execute("select idx, start_s, end_s, lines from cues where version_season = ? and episode = ? order by idx", (vs, ep)):
                L = json.loads(lines)
                key = json.dumps([l.get("text") for l in L])
                if key == prev:
                    stats["duplicate cues dropped"] += 1
                    continue
                prev = key
                name = next((l["sdh_name"] for l in L if l.get("sdh_name")), None)
                text = " ".join(l["text"] for l in L if l.get("text") and not l.get("is_sound")).strip()
                rows.append({"idx": idx, "start": start, "end": end, "text": text, "sdh": name,
                             "speaker": resolve.get(norm(name)) if name else None,
                             "italic": int(any(l.get("is_italic") for l in L if l.get("text"))),
                             "turn": int(any(l.get("is_turn") for l in L)), "sound": int(all(l.get("is_sound") for l in L))})
            out.executemany("insert into cues values (?,?,?,?,?,?,?,?,?,?,?)",
                            [(vs, ep, r["idx"], r["start"], r["end"], r["text"], r["sdh"], r["speaker"], r["italic"], r["turn"], r["sound"]) for r in rows])
            stats["cues kept"] += len(rows)

            # the recap: up to last week's verdict if it is replayed in the opening minutes
            recap_end, method = 0.0, "none"
            if ep > 1:
                early = [r for r in rows if r["start"] < 240]
                v = [r for r in early if VERDICT.search(r["text"]) or TALLY.search(r["text"])]
                if v:
                    recap_end, method = v[-1]["end"], "verdict"
                elif any(PREVIOUSLY.search(r["text"]) for r in early[:6]):
                    recap_end, method = 75.0, "previously-on"
            out.execute("insert into recaps values (?,?,?,?)", (vs, ep, recap_end, method))
            body = [r for r in rows if r["start"] >= recap_end]

            # the votes: each "go tally" after the recap, paired in order with the episode's boots
            tallies = []
            for r in body:
                if TALLY.search(r["text"]) and (not tallies or r["start"] - tallies[-1]["start"] > 90):
                    tallies.append(r)
            boots = boots_by_ep.get(ep, [])
            # pair each boot with the first unused tally whose reading names them (a tie brings a second tally
            # for the same vote); when none names them, the next unused tally in order
            used, lo = -1, recap_end
            for k, cid in enumerate(boots):
                heard = lambda t: cid in spoken and any(_hit(spoken[cid], r["text"]) for r in body if t["start"] <= r["start"] <= t["start"] + 240)
                cand = [i for i in range(used + 1, len(tallies)) if heard(tallies[i])]
                i = cand[0] if cand else (used + 1 if used + 1 < len(tallies) else None)
                if i is None:
                    stats["votes with no tally found"] += 1
                    out.execute("insert into tribals values (?,?,?,?,?,?,?,?,?)", (vs, ep, k + 1, cid, name_of.get(cid), None, "none", None, 0))
                    continue
                t, named = tallies[i], int(bool(cand))
                # the call to vote: the last one after the previous vote's tally; for a tie, the first vote's call
                calls = [r for r in body if lo < r["start"] < t["start"] and TIME_TO_VOTE.search(r["text"])]
                tie = len(calls) > 1 and any(TALLY.search(r["text"]) for r in body if calls[0]["start"] < r["start"] < calls[-1]["start"])
                if tie:
                    calls = calls[:1]
                used, lo = i, t["start"]
                if calls:
                    if not tie:
                        gaps.append(t["start"] - calls[-1]["start"])
                    cut, how = calls[-1]["start"], "time to vote"
                else:
                    cut, how = None, "tally minus gap"
                    pending.append((vs, ep, k + 1, t["start"]))
                out.execute("insert into tribals values (?,?,?,?,?,?,?,?,?)", (vs, ep, k + 1, cid, name_of.get(cid), cut, how, t["start"], named))
                stats["votes found"] += 1
                stats["votes with the boot named after the tally"] += named

            # quotes: a captioner's name starts an utterance; unnamed cues follow until a turn or a new name
            cur, buf, italics, start = None, [], [], None
            def flush():
                if cur and buf:
                    text = re.sub(r"\s+", " ", " ".join(buf)).strip()
                    words = len(text.split())
                    anyone = int(any(_hit(p, text) for p in spoken.values()))
                    out.execute("insert into quotes values (?,?,?,?,?,?,?,?,?,?,?,?)",
                                (vs, ep, start, cur, name_of.get(cur), text, words, len(buf), sum(italics) / len(italics), anyone,
                                 int(bool(re.match(r"^[A-Z\"']", text))), int(bool(re.search(r"[.!?]\"?$", text)))))
            for r in body:
                if r["sdh"] or r["turn"]:
                    flush()
                    cur, buf, italics, start = (r["speaker"] if r["sdh"] else None), [], [], r["start"]
                if r["text"] and not r["sound"]:
                    buf.append(r["text"]); italics.append(r["italic"])
                if len(buf) > 8:                  # long enough; a quote is short, and long runs hide speaker changes
                    flush(); cur, buf, italics = None, [], []
            flush()

        stats["seasons"] += 1
        print(vs, flush=True, end=" ")

    # the fallback cut: the median gap between "time to vote" and "go tally" across the library
    gap = statistics.median(gaps) if gaps else 120.0
    for vs, ep, k, tally in pending:
        out.execute("update tribals set cut_s = ? where version_season = ? and episode = ? and k = ?", (tally - gap, vs, ep, k))

    # mentions per castaway and vote: the recap's end to the vote's cut
    for vs, ep, k, cut in out.execute("select version_season, episode, k, cut_s from tribals where cut_s is not null").fetchall():
        recap_end = out.execute("select recap_end_s from recaps where version_season = ? and episode = ?", (vs, ep)).fetchone()[0]
        cues = out.execute("select text, italic from cues where version_season = ? and episode = ? and start_s >= ? and start_s < ? and sound = 0",
                           (vs, ep, recap_end, cut)).fetchall()
        full = out.execute("select text from cues where version_season = ? and episode = ? and start_s >= ?", (vs, ep, recap_end)).fetchall()
        cast = gb.execute("select castaway_id from boot_mapping where version_season = ? and episode = ? group by castaway_id", (vs, ep)).fetchall()
        pats = _patterns(gb, alias, vs)
        for (cid,) in cast:
            p = pats.get(cid)
            if not p:
                continue
            pre = [(t, it) for t, it in cues if _hit(p, t)]
            out.execute("insert into mentions values (?,?,?,?,?,?,?,?,?)",
                        (vs, ep, k, cid, sum(1 for (t,) in full if _hit(p, t)), len(pre), sum(1 for t, _ in pre if TARGET.search(t)),
                         sum(1 for _, it in pre if it), len(cues)))
    out.commit()
    out.close()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(work, OUT)
    out = sqlite3.connect(work)
    with CSV.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["version_season", "episode", "k", "castaway_id", "episode_mentions", "pre_mentions", "pre_target_mentions", "pre_confessional_mentions", "pre_cues"])
        w.writerows(out.execute("select * from mentions order by version_season, episode, k, castaway_id"))
    q = out.execute("select count(*), sum(italic_share >= 0.8 and names_anyone = 0 and n_words between 10 and 25 and clean_start and clean_end and speaker_id is not null) from quotes").fetchone()
    print(f"\ngap time-to-vote -> tally: median {gap:.0f} s over {len(gaps)} votes")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print(f"  quotes: {q[0]} attributed utterances, {q[1]} pass the trivia filters")
    print(f"wrote {OUT} and {CSV}")
    return 0


_PAT_CACHE: dict = {}


def _patterns(gb, alias, vs):
    if vs in _PAT_CACHE:
        return _PAT_CACHE[vs]
    tokens = collections.defaultdict(set)
    for cid, short, full, full_d in gb.execute("""select c.castaway_id, c.castaway, d.full_name, d.full_name_detailed from castaways c
                                                  join castaway_details d using (castaway_id) where c.version_season = ?""", (vs,)):
        for t in {short, (full or "").split(" ")[0], (full_d or "").split(" ")[0]}:
            if t and len(t) >= 2:
                tokens[t.strip(".")].add(cid)
    for tok, cid in alias.get(vs, {}).items():
        if cid.startswith("US") and len(tok) >= 3:
            tokens[tok.title()].add(cid)
    by = collections.defaultdict(list)
    for tok, cids in tokens.items():
        if len(cids) == 1:
            by[next(iter(cids))].append(tok)
    _PAT_CACHE[vs] = {cid: [(t, re.compile(r"(?<![A-Za-z'])" + re.escape(t) + r"(?![A-Za-z])", re.I)) for t in sorted(ts, key=len, reverse=True)]
                      for cid, ts in by.items()}
    return _PAT_CACHE[vs]


def _hit(pats, text: str) -> bool:
    """A name said in the line. Matching ignores case (S21-39 captions are all capitals). A name that is also a
    common word (Will, Chase, Hope) does not count as a line's first word, and in an all-capitals line it counts
    only when punctuation or the line's end follows it, as in a vote reading ("THAT'S TWO VOTES, WILL.")."""
    caps = text.isupper()
    for tok, p in pats:
        for m in p.finditer(text):
            if tok in WORDLIKE:
                if m.start() == 0:
                    continue
                if caps and not re.match(r"\s*([.,!?;:]|$)", text[m.end():]):
                    continue
            return True
    return False


if __name__ == "__main__":
    sys.exit(main())
