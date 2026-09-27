"""Families 18-24 of the NLP plan (the "Gamebot NLP features plan" doc, batch 6): what speaker labels show about a
player's place in the social game and in the edit. Same rows, windows, leakage rule and attribution as
nlp_features.py (see nlp_episodes.py); scenes as in nlp_interaction_families.py.

  family 18  behind their back   mentions of a player at camp in a scene they do not speak in, against mentions in
                                 scenes they speak in; vote talk about them while absent; how many people do it
  family 19  one-on-one time     two-person camp scenes (no host): how many, with how many partners, share of the
                                 window's; left out = none while their tribemates had two or more
  family 20  ironic cuts         their confessional claims (safe, in control, trusting someone) followed within 2
                                 minutes by someone else's vote talk naming them: count, share, seconds to the cut
  family 21  music bed           under their confessionals tonight, against everyone's confessionals in the window:
                                 music-to-voice level, low-end share, minor-key score (scripts/music_features.py)
  family 22  airtime trajectory  their share of each earlier episode's talk: mean, spread, slope, episodes with no
                                 confessional in a row; tonight's share against that history
  family 23  narrating others    share of their confessional lines about another player, and about the vote
  family 24  asking who's going  their questions about the vote or the plan, per line

Writes numbers only: data_cache/nlp/nlp_context_families.csv.

    uv run --with vaderSentiment python scripts/nlp_context_families.py [--seasons ...] [--music data_cache/nlp/music_utts.csv]
"""

from __future__ import annotations

import collections
import csv
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402
from nlp_episodes import div, episodes, write_rows  # noqa: E402
from nlp_features import HOST, Line, sm  # noqa: E402
from nlp_interaction_families import scenes  # noqa: E402

OUT = nf.ROOT / "data_cache/nlp/nlp_context_families.csv"
MUSIC = nf.ROOT / "data_cache/nlp/music_utts.csv"
IRONY_S = 120.0
CLAIM = re.compile("|".join(f"(?:{rx.pattern})" for rx in (nf.SAFE, nf.CONTROL, nf.TRUSTCLAIM)), re.I)
ASK = re.compile(r"\b(who are we (voting|writing|getting rid)|who('?s| is) (going|getting|the target)|what('?s| is) the plan|"
                 r"what are we doing|who do (we|you) (want|vote)|are we (voting|doing|still)|is it me|am i (safe|good|going)|"
                 r"what('?s| is) going on|where('?s| is) the vote|who('?re| are) you voting|what do you (want|think) (to do|we should))\b", re.I)
MUSIC_KEYS = {"mvr": "mu_mvr", "low_share": "mu_low", "minor": "mu_minor", "tonal": "mu_tonal"}


@dataclass
class Win:
    c: dict = field(default_factory=lambda: collections.defaultdict(collections.Counter))
    namers_absent: dict = field(default_factory=lambda: collections.defaultdict(set))
    partners: dict = field(default_factory=lambda: collections.defaultdict(set))
    gaps: dict = field(default_factory=lambda: collections.defaultdict(list))
    tot: collections.Counter = field(default_factory=collections.Counter)
    tribe_dyads: collections.Counter = field(default_factory=collections.Counter)

    def add(self, o: "Win") -> None:
        for k, v in o.c.items():
            self.c[k].update(v)
        for src, dst in ((o.namers_absent, self.namers_absent), (o.partners, self.partners)):
            for k, v in src.items():
                dst[k] |= v
        for k, v in o.gaps.items():
            self.gaps[k].extend(v)
        self.tot.update(o.tot)
        self.tribe_dyads.update(o.tribe_dyads)


def window_stats(lines: list[Line], cast: set[str], tribe: dict) -> Win:
    w = Win()
    for sc in scenes(lines, cast):
        speakers = {ln.speaker for ln in sc if ln.speaker is not None}
        cs = speakers & cast
        # family 18: who is named while they are not in the scene
        for ln in sc:
            if ln.speaker is None:
                continue
            vote = bool(sm.TARGET.search(ln.text))
            for x in ln.named - {ln.speaker}:
                if x not in cast:
                    continue
                if x in speakers:
                    w.c[x]["bb_present"] += 1
                else:
                    w.c[x]["bb_absent"] += 1
                    w.tot["bb_absent"] += 1
                    w.namers_absent[x].add(ln.speaker)
                    if vote:
                        w.c[x]["bb_absent_vote"] += 1
                        w.tot["bb_absent_vote"] += 1
        # family 19: two-person scenes
        if len(cs) == 2 and HOST not in speakers and len(speakers) == 2:
            a, b = sorted(cs)
            w.tot["dyads"] += 1
            for x, y in ((a, b), (b, a)):
                w.c[x]["dyads"] += 1
                w.partners[x].add(y)
            if tribe.get(a) is not None and tribe.get(a) == tribe.get(b):
                w.tribe_dyads[tribe[a]] += 1
    for i, ln in enumerate(lines):
        if ln.speaker not in cast:
            continue
        x = w.c[ln.speaker]
        x["lines"] += 1
        conf = ln.domain == "confessional"
        # family 23: narrating others in confessional
        if conf:
            x["conf_lines"] += 1
            x["conf_other"] += bool(ln.named - {ln.speaker})
            x["conf_vote"] += bool(sm.TARGET.search(ln.text))
        # family 24: asking about the vote
        if "?" in ln.text and ASK.search(ln.text):
            x["ask"] += 1
        # family 20: a claim, then someone else's vote talk about them
        if conf and CLAIM.search(ln.text):
            x["ic_claims"] += 1
            for m in lines[i + 1:]:
                if m.start - ln.end > IRONY_S:
                    break
                if m.speaker != ln.speaker and ln.speaker in m.named and sm.TARGET.search(m.text):
                    x["ic_hits"] += 1
                    w.gaps[ln.speaker].append(max(0.0, m.start - ln.end))
                    break
        x["air_secs"] += max(0.0, ln.end - ln.start)
        w.tot["air_secs"] += max(0.0, ln.end - ln.start)
        w.c[ln.speaker]["conf_n"] += int(conf)
    return w


# ----------------------------------------------------------------------------- music (family 21)

def load_music(path: Path) -> dict:
    out = collections.defaultdict(list)
    if not path.exists():
        return out
    with open(path) as f:
        for r in csv.DictReader(f):
            if r["domain"] != "confessional":
                continue
            vals = {k: float(r[k]) if r[k] not in ("", "nan", None) else None for k in MUSIC_KEYS}
            out[(r["version_season"], int(r["episode"]))].append((float(r["sub_start"]), r["speaker_id"], vals))
    return out


def music_rel(rows: list[tuple], cid: str) -> dict:
    """Their confessionals' bed against everyone's in the window (mean difference)."""
    out = {}
    for k, name in MUSIC_KEYS.items():
        mine = [v[k] for _, s, v in rows if s == cid and v[k] is not None]
        allv = [v[k] for _, s, v in rows if v[k] is not None]
        if mine and len(allv) >= 5:
            out[name] = float(np.mean(mine) - np.mean(allv))
    out["mu_n"] = sum(1 for _, s, _v in rows if s == cid)
    return out


# ----------------------------------------------------------------------------- trajectory (family 22)

def trajectory(hist: list[tuple[float, int]], tonight_share: float | None) -> dict:
    """hist = [(air share, confessionals)] of each earlier episode they were in."""
    out = {"tr_eps": len(hist)}
    if not hist:
        return out
    s = np.array([h[0] for h in hist])
    out["tr_mean"], out["tr_sd"] = float(s.mean()), float(s.std())
    if len(s) >= 3:
        out["tr_slope"] = float(np.polyfit(np.arange(len(s)), s, 1)[0])
    run = best = 0
    for _, n in hist:
        run = run + 1 if n == 0 else 0
        best = max(best, run)
    out["tr_zero_streak"], out["tr_zero_now"] = best, run
    if tonight_share is not None and len(s) >= 2:
        out["tr_spike"] = (tonight_share - s.mean()) / max(s.std(), 0.01)
    return out


FEATURES = [
    "bb_absent_n", "bb_absent_share", "bb_absent_vote_n", "bb_absent_vote_share", "bb_absent_namers",   # 18
    "dy_n", "dy_share", "dy_partners", "dy_left_out",                                                    # 19
    "ic_claims", "ic_hits", "ic_rate", "ic_min_gap",                                                     # 20
    "mu_n", "mu_mvr", "mu_low", "mu_minor", "mu_tonal",                                                  # 21
    "tr_eps", "tr_mean", "tr_sd", "tr_slope", "tr_zero_streak", "tr_zero_now", "tr_spike",               # 22
    "nr_other_share", "nr_vote_share",                                                                   # 23
    "ask_n", "ask_rate",                                                                                 # 24
    "attr_source",
]


def derive(w: Win, cast: list[str], tribe: dict, source: str) -> dict[str, dict]:
    out = {}
    for c in cast:
        x = w.c.get(c, collections.Counter())
        mates_dyads = w.tribe_dyads.get(tribe.get(c), 0)
        out[c] = {
            "bb_absent_n": x["bb_absent"], "bb_absent_share": div(x["bb_absent"], x["bb_absent"] + x["bb_present"]),
            "bb_absent_vote_n": x["bb_absent_vote"], "bb_absent_vote_share": div(x["bb_absent_vote"], w.tot["bb_absent_vote"]),
            "bb_absent_namers": len(w.namers_absent.get(c, ())),
            "dy_n": x["dyads"], "dy_share": div(x["dyads"], w.tot["dyads"]), "dy_partners": len(w.partners.get(c, ())),
            "dy_left_out": int(x["dyads"] == 0 and mates_dyads >= 2) if w.tot["dyads"] else None,
            "ic_claims": x["ic_claims"], "ic_hits": x["ic_hits"], "ic_rate": div(x["ic_hits"], x["ic_claims"]),
            "ic_min_gap": min(w.gaps[c]) if w.gaps.get(c) else None,
            "nr_other_share": div(x["conf_other"], x["conf_lines"]) if x["conf_lines"] >= 3 else None,
            "nr_vote_share": div(x["conf_vote"], x["conf_lines"]) if x["conf_lines"] >= 3 else None,
            "ask_n": x["ask"], "ask_rate": div(100 * x["ask"], x["lines"]) if x["lines"] >= 5 else None,
            "attr_source": source,
        }
    return out


def main(args: list[str]) -> int:
    t0 = time.time()
    out_path = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    music = load_music(Path(args[args.index("--music") + 1]) if "--music" in args else MUSIC)
    rows_out: list[dict] = []
    n_ep = 0
    prior = None
    for ep in episodes(args):
        if ep.first_of_season:
            prior, hist = Win(), collections.defaultdict(list)
        cast, cs = ep.cast, set(ep.cast)
        mrows = music.get((ep.vs, ep.ep), [])
        prow = derive(prior, cast, ep.tribe, ep.source)
        for c in cast:
            prow[c].update(trajectory(hist[c], None))
            rows_out.append({"window": "prior", "version_season": ep.vs, "episode": ep.ep, "k": None, "castaway_id": c, **prow[c]})
        for k, a, b in ep.windows:
            wl = ep.window(a, b)
            w = window_stats(wl, cs, ep.tribe)
            rows = derive(w, cast, ep.tribe, ep.source)
            wm = [r for r in mrows if a <= r[0] < b]
            for c in cast:
                share = div(w.c[c]["air_secs"], w.tot["air_secs"]) if c in w.c else (0.0 if w.tot["air_secs"] else None)
                rows[c].update(trajectory(hist[c], share))
                if wm:
                    rows[c].update(music_rel(wm, c))
                rows_out.append({"window": "tonight", "version_season": ep.vs, "episode": ep.ep, "k": k, "castaway_id": c, **rows[c]})
        whole = window_stats(ep.lines, cs, ep.tribe)
        prior.add(whole)
        if whole.tot["air_secs"] > 0 and ep.source != "none":
            for c in cast:
                x = whole.c.get(c, collections.Counter())
                hist[c].append((x["air_secs"] / whole.tot["air_secs"], x["conf_n"]))
        n_ep += 1
    write_rows(out_path, FEATURES, rows_out)
    print(f"\n{n_ep} episodes, {len(rows_out)} rows in {time.time() - t0:.0f} s -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
