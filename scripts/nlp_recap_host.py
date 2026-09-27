"""Families 26-27 of the NLP plan (the "Gamebot NLP features plan" doc, batch 7): two more places the edit shows
its hand before the vote. Same rows and leakage rule as nlp_features.py (see nlp_episodes.py).

  family 26  "previously on"   the recap at the top of the episode: lines and seconds each castaway speaks in it,
                               their share of the recap's cast talk, and lines that name them. The recap airs first,
                               so it counts for every council in the episode
  family 27  host outside      Jeff's lines outside tribal council (challenges, arrivals, the first-episode mat chat):
             tribal            share of those lines that name them, share of his questions aimed at them; the same
                               over earlier episodes; and their share in episode 1, carried through the season
  family 28  music cues        the kind of music under their lines (scripts/music_cues.py: goofy "dodo music",
                               ominous, tense, sad, triumphant, upbeat, eerie), each as their mean against the mean
                               under everyone's lines in the window; the same over earlier episodes

Speakers in the recap come from the recap's own audio labels (survspk segment 'recap'), else caption names.
Writes numbers only: data_cache/nlp/nlp_recap_host.csv.

    uv run --with vaderSentiment python scripts/nlp_recap_host.py [--seasons ...]
"""

from __future__ import annotations

import collections
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402
from nlp_episodes import div, episodes, write_rows  # noqa: E402
from nlp_features import HOST, Line, sm  # noqa: E402
from nlp_interaction_families import council_segment, host_attention  # noqa: E402

OUT = nf.ROOT / "data_cache/nlp/nlp_recap_host.csv"
CUES_CSV = nf.ROOT / "data_cache/nlp/music_cues.csv"
CUE_KEYS = ["goofy", "ominous", "tense", "sad", "triumphant", "upbeat", "eerie"]


def load_cues(path: Path) -> dict:
    """(vs, ep) -> [(time on the subtitle clock, speaker, {cue: prob})] for cast lines."""
    import csv

    out = collections.defaultdict(list)
    if not path.exists():
        return out
    with open(path) as f:
        for r in csv.DictReader(f):
            if r["speaker_id"] == "HOST":
                continue
            out[(r["version_season"], int(r["episode"]))].append(
                (float(r["sub_start"]), r["speaker_id"], {k: float(r[f"cue_{k}"]) for k in CUE_KEYS}))
    return out


def cue_rel(rows: list[tuple], cast: list[str]) -> dict[str, dict]:
    """Each castaway's mean cue probabilities against the mean under all cast lines in `rows`."""
    if len(rows) < 10:
        return {c: {} for c in cast}
    allm = {k: sum(v[k] for _, _, v in rows) / len(rows) for k in CUE_KEYS}
    out = {}
    for c in cast:
        mine = [v for _, s_, v in rows if s_ == c]
        out[c] = {"mc_n": len(mine), **({f"mc_{k}": sum(v[k] for v in mine) / len(mine) - allm[k] for k in CUE_KEYS}
                                       if len(mine) >= 3 else {})}
    return out


def load_recap_labels() -> dict:
    """(vs, ep) -> [(start, end, speaker, domain)] of trusted labels on recap lines, on the subtitle clock."""
    out = collections.defaultdict(list)
    if not nf.SPEAKERS.exists():
        return out
    con = nf.connect_ro(nf.SPEAKERS)
    for vs, ep, a, b, spk, src, conf, dom in con.execute(
            """SELECT u.version_season, u.episode, MIN(c.start_s), MAX(c.end_s), l.speaker_id, l.source,
                      COALESCE(l.confidence, 0), u.domain_hint
               FROM labels l JOIN utterances u USING (utt_id) JOIN utterance_cues uc USING (utt_id)
                    JOIN cues c ON c.cue_id = uc.cue_id
               WHERE u.segment = 'recap' AND l.speaker_id NOT IN ('UNKNOWN', 'NOSPEECH', 'OTHER')
               GROUP BY u.utt_id"""):
        if src in ("human", "sdh", "chyron") or (src == "auto" and conf >= nf.AUTO_MIN_CONF):
            out[(vs, ep)].append((a, b, HOST if spk.startswith("HOST") else spk, dom))
    for v in out.values():
        v.sort()
    return out


def recap_lines(subs, vs: str, ep: int, recap_end: float, labels: list | None, pats: dict, cast: set) -> tuple[list[Line], str]:
    cues = subs.execute("""SELECT start_s, end_s, text, sdh_name, speaker_id, italic, turn, sound FROM cues
                           WHERE version_season=? AND episode=? AND start_s < ? ORDER BY idx""", (vs, ep, recap_end)).fetchall()
    cues = [r for r in cues if r["text"] and not r["sound"]]
    lines = [Line(r["start_s"], r["end_s"], r["text"], int(r["italic"] or 0)) for r in cues]
    src = nf.attribute(lines, cues, labels) if lines else "none"
    for ln in lines:
        ln.named = frozenset(c for c, p in pats.items() if c in cast and sm._hit(p, ln.text))
    return lines, src


def recap_features(lines: list[Line], cast: list[str]) -> dict[str, dict]:
    secs, n, named = collections.Counter(), collections.Counter(), collections.Counter()
    for ln in lines:
        if ln.speaker in cast:
            secs[ln.speaker] += max(0.0, ln.end - ln.start)
            n[ln.speaker] += 1
        for c in ln.named - {ln.speaker}:
            named[c] += 1
    tot, tot_named = sum(secs.values()), sum(named.values())
    return {c: {"rc_lines": n[c], "rc_secs": round(secs[c], 2), "rc_share": div(secs[c], tot), "rc_named": named[c],
                "rc_named_share": div(named[c], tot_named), "rc_any": int(n[c] > 0 or named[c] > 0),
                "rc_n_lines": len(lines)} for c in cast}


FEATURES = ["rc_lines", "rc_secs", "rc_share", "rc_named", "rc_named_share", "rc_any", "rc_n_lines", "rc_source",
            "hc_named_share", "hc_q_share", "hc_named_n", "hc_named_share_prior", "hc_e01_share",
            "mc_n"] + [f"mc_{k}" for k in CUE_KEYS] + ["attr_source"]


def main(args: list[str]) -> int:
    t0 = time.time()
    out_path = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    subs = nf.connect_ro(Path(args[args.index("--subs") + 1]) if "--subs" in args else nf.SUBS, immutable=True)
    gb = nf.connect_ro(Path(args[args.index("--gamebot") + 1]) if "--gamebot" in args else nf.GAMEBOT)
    rlab = load_recap_labels() if "--labels" not in args else {}
    cues = load_cues(Path(args[args.index("--cues") + 1]) if "--cues" in args else CUES_CSV)
    alias = sm.aliases()
    rows_out, pats_by = [], {}
    for ep in episodes(args):
        if ep.first_of_season:
            hist_named, hist_tot, e01, cue_hist = collections.Counter(), 0, {}, []
        if ep.vs not in pats_by:
            pats_by[ep.vs] = sm._patterns(gb, alias, ep.vs)
        cast, cs = ep.cast, set(ep.cast)
        rl, rsrc = recap_lines(subs, ep.vs, ep.ep, ep.recap_end, rlab.get((ep.vs, ep.ep)), pats_by[ep.vs], cs) \
            if ep.recap_end > 0 else ([], "none")
        rc = recap_features(rl, cast)
        prior_share = {c: div(hist_named[c], hist_tot) for c in cast}
        pc = cue_rel(cue_hist, cast)
        for c in cast:
            rows_out.append({"window": "prior", "version_season": ep.vs, "episode": ep.ep, "k": None, "castaway_id": c,
                             "hc_named_share_prior": prior_share[c], "hc_e01_share": e01.get(c), **pc[c],
                             "attr_source": ep.source})
        ecues = cues.get((ep.vs, ep.ep), [])
        ep_named, ep_tot = collections.Counter(), 0
        for k, a, b in ep.windows:
            wl = ep.window(a, b)
            seg = set(id(x) for x in council_segment(wl, b))
            outside = [x for x in wl if id(x) not in seg]
            per, tot = host_attention(outside, cs)
            wc = cue_rel([r for r in ecues if a <= r[0] < b], cast)
            for c in cast:
                rows_out.append({"window": "tonight", "version_season": ep.vs, "episode": ep.ep, "k": k, "castaway_id": c,
                                 **(rc[c] if rl else {}), "rc_source": rsrc,
                                 "hc_named_share": div(per[(c, "named")], tot["host_lines"]),
                                 "hc_q_share": div(per[(c, "q")], tot["host_q"]), "hc_named_n": per[(c, "named")],
                                 "hc_named_share_prior": prior_share[c], "hc_e01_share": e01.get(c), **wc[c],
                                 "attr_source": ep.source})
        # the whole episode outside its councils joins the history
        segs = set()
        for k, a, b in ep.windows:
            segs |= set(id(x) for x in council_segment(ep.window(a, b), b))
        per, tot = host_attention([x for x in ep.lines if id(x) not in segs], cs)
        for c in cast:
            hist_named[c] += per[(c, "named")]
        hist_tot += tot["host_lines"]
        cue_hist.extend(ecues)
        if ep.first_of_season and tot["host_lines"]:
            named_tot = sum(per[(c, "named")] for c in cast)
            e01 = {c: div(per[(c, "named")], named_tot) for c in cast}
    write_rows(out_path, FEATURES, rows_out)
    print(f"\n{len(rows_out)} rows in {time.time() - t0:.0f} s -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
