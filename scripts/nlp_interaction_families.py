"""Families 11-17 of the NLP plan (the "Gamebot NLP features plan" doc, batch 5b): how players talk to each other,
and production tells. Same rows, windows, leakage rule and attribution as nlp_features.py (see nlp_episodes.py).
A camp scene is a run of camp lines less than 6 s apart, broken by a confessional; an exchange is one known speaker
followed by another inside a scene.

  family 11  style matching     function-word matching (pronouns, articles, prepositions, auxiliaries, adverbs,
                                conjunctions, negations, quantifiers) between players who share camp scenes, from
                                what each says in those scenes: mean over partners, with tribemates, lowest partner
  family 12  idea uptake        content-word pairs a player says first in the window that another player repeats
                                later in it (phrases common in earlier episodes do not count); and the reverse
  family 13  holding the floor  share of a scene's speaking time, against an equal split; cutting in right after a
                                line that trails off ("--"), being cut off
  family 14  duplicity          tone at camp near a person minus tone about them in confessional: how two-faced a
                                player is (out) and how two-faced others are about them (in)
  family 15  host attention     share of the host's questions at tribal council aimed at them (named, or the next
                                speaker), and of the host's lines there that name them
  family 16  laughter           SDH laughter tags ("(laughs)", "(laughter)") at their lines, per camp line; only on
                                subtitles that keep sound tags (survspk cues)
  family 17  voice              pitch, pitch range, loudness and speaking rate of their confessionals tonight against
                                their own earlier confessionals this season (scripts/voice_features.py makes the rows)

Writes numbers only: data_cache/nlp/nlp_interaction_families.csv.

    uv run --with vaderSentiment python scripts/nlp_interaction_families.py [--seasons ...] [--voice data_cache/nlp/voice_utts.csv]
"""

from __future__ import annotations

import collections
import csv
import json
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402
from nlp_edit_families import STOP  # noqa: E402
from nlp_episodes import div, episodes, write_rows  # noqa: E402
from nlp_features import HOST, WORD, Line  # noqa: E402

OUT = nf.ROOT / "data_cache/nlp/nlp_interaction_families.csv"
VOICE = nf.ROOT / "data_cache/nlp/voice_utts.csv"
SCENE_GAP_S = 6.0
LSM_MIN_WORDS = 25        # words each side of a pair (in scenes they share) before its style match counts
UPTAKE_COMMON = 0.15      # a word pair used in more than this share of earlier episodes is common talk, not an idea
HOST_GAP_S = 120.0        # tribal council = the run of host lines before the cut with gaps under this
HOST_MAX_S = 900.0
LAUGH_AFTER_S = 1.5
VOICE_MIN_PRIOR = 8

LSM_CATS = {
    "ppron": "i me my mine myself we us our ours ourselves you your yours yourself yourselves he him his himself she her "
             "hers herself they them their theirs themselves i'm i've i'll i'd we're we've you're he's she's they're",
    "ipron": "it its itself this that these those anything something everything nothing someone anyone everyone nobody it's that's",
    "article": "a an the",
    "prep": "to of in for on with at by from about into over after under between through during before without within",
    "auxverb": "am is are was were be been being have has had do does did will would shall should can could may might must",
    "adverb": "very really just so too also only even still already quite almost always never ever",
    "conj": "and but or because if though although while unless yet",
    "negate": "no not never nothing nobody none can't don't won't isn't aren't wasn't weren't didn't doesn't haven't "
              "hasn't couldn't shouldn't wouldn't",
    "quant": "all some many much more most few several lot lots every each both any",
}
LSM_OF = {w: k for k, v in LSM_CATS.items() for w in v.split()}
TRAIL = re.compile(r"(--|—|\.\.\.)\s*$")
LAUGH_SELF = re.compile(r"\b(laughs|laughing|chuckl\w*|giggl\w*)\b", re.I)
LAUGH_ANY = re.compile(r"laugh|chuckl|giggl", re.I)


# ----------------------------------------------------------------------------- scenes

def scenes(lines: list[Line], cast: set[str]) -> list[list[Line]]:
    out, cur, prev_end = [], [], -1e9
    for ln in lines:
        conf = ln.domain == "confessional"
        if conf or ln.start - prev_end > SCENE_GAP_S:
            if cur:
                out.append(cur)
            cur = []
        if not conf:
            cur.append(ln)
        prev_end = ln.end
    if cur:
        out.append(cur)
    return out


def bigrams(text: str) -> set[tuple[str, str]]:
    toks = [t.lower() for t in WORD.findall(text)]
    content = [t for t in toks if t not in STOP and t not in LSM_OF and len(t) > 2]
    return set(zip(content, content[1:]))


# ----------------------------------------------------------------------------- window statistics

@dataclass
class Win:
    lsm: dict = field(default_factory=lambda: collections.defaultdict(collections.Counter))   # (a, b) -> a's cats near b
    floor: dict = field(default_factory=lambda: collections.defaultdict(lambda: [0.0, 0.0, 0]))  # cid -> [share, rel, n]
    c: dict = field(default_factory=lambda: collections.defaultdict(collections.Counter))
    dconf: dict = field(default_factory=lambda: collections.defaultdict(lambda: [0.0, 0]))   # (a, b) -> tone about b
    dcamp: dict = field(default_factory=lambda: collections.defaultdict(lambda: [0.0, 0]))   # (a, b) -> tone near b
    laugh_ok: bool = False
    host: collections.Counter = field(default_factory=collections.Counter)                  # host_q, host_named, per cid
    host_tot: collections.Counter = field(default_factory=collections.Counter)
    lines: int = 0
    attr: int = 0

    def add(self, o: "Win") -> None:
        for k, v in o.lsm.items():
            self.lsm[k].update(v)
        for k, (s, r, n) in o.floor.items():
            f = self.floor[k]
            f[0] += s; f[1] += r; f[2] += n
        for k, v in o.c.items():
            self.c[k].update(v)
        for src, dst in ((o.dconf, self.dconf), (o.dcamp, self.dcamp)):
            for k, (s, n) in src.items():
                dst[k][0] += s; dst[k][1] += n
        self.laugh_ok = self.laugh_ok or o.laugh_ok
        self.lines += o.lines
        self.attr += o.attr


def window_stats(lines: list[Line], cast: set[str], tone, laughs: list[tuple[float, str]] | None,
                 common: set | None = None) -> Win:
    w = Win()
    for ln in lines:
        w.lines += 1
        w.attr += ln.speaker is not None
        if ln.speaker in cast:
            w.c[ln.speaker]["lines"] += 1
            if ln.domain == "confessional":
                for b in ln.named - {ln.speaker}:
                    if b in cast:
                        w.dconf[(ln.speaker, b)][0] += tone(ln)
                        w.dconf[(ln.speaker, b)][1] += 1
            else:
                w.c[ln.speaker]["camp_lines"] += 1
    # scenes: style matching, floor, cut-ins, tone near people
    for sc in scenes(lines, cast):
        who = [ln.speaker for ln in sc if ln.speaker in cast]
        if len(set(who)) < 2:
            continue
        secs = collections.Counter()
        for ln in sc:
            if ln.speaker in cast:
                secs[ln.speaker] += max(0.0, ln.end - ln.start)
        tot = sum(secs.values())
        n = len(secs)
        for c, s_ in secs.items():
            f = w.floor[c]
            f[0] += s_ / tot if tot else 0.0
            f[1] += (s_ / tot) * n if tot else 0.0
            f[2] += 1
        present = set(secs)
        for ln in sc:
            if ln.speaker in cast:
                t = tone(ln)
                toks = [x.lower() for x in WORD.findall(ln.text)]
                cats = collections.Counter(LSM_OF[x] for x in toks if x in LSM_OF)
                for b in present - {ln.speaker}:
                    w.dcamp[(ln.speaker, b)][0] += t
                    w.dcamp[(ln.speaker, b)][1] += 1
                    cnt = w.lsm[(ln.speaker, b)]          # what they say in scenes shared with b
                    cnt["_words"] += len(toks)
                    cnt.update(cats)
        for p, q in zip(sc, sc[1:]):
            if p.speaker in cast and q.speaker in cast and p.speaker != q.speaker:
                if TRAIL.search(p.text) and q.start - p.end < 0.5:
                    w.c[q.speaker]["cut_in"] += 1
                    w.c[p.speaker]["cut_off"] += 1
    # idea uptake: first use of a content-word pair by a player, repeated later by another player
    first: dict = {}
    for ln in lines:
        if ln.speaker is None:
            continue
        for bg in bigrams(ln.text):
            if common is not None and bg in common:
                continue
            if bg not in first:
                first[bg] = (ln.speaker, set())
            elif ln.speaker != first[bg][0] and ln.speaker in cast:
                first[bg][1].add(ln.speaker)
    for bg, (orig, users) in first.items():
        if orig in cast and users:
            w.c[orig]["uptake"] += 1
            w.c["_tot"]["uptake"] += 1
            for u in users:
                w.c[u]["echo"] += 1
    # laughter tags at their lines
    if laughs is not None:
        w.laugh_ok = True
        spoken = [ln for ln in lines if ln.speaker in cast and ln.domain != "confessional"]
        for t, raw in laughs:
            best = None
            for ln in spoken:
                if ln.start - 0.5 <= t <= ln.end + LAUGH_AFTER_S:
                    best = ln
                elif ln.start > t:
                    break
            if best is not None:
                w.c[best.speaker]["laugh"] += 1
                w.c[best.speaker]["laugh_self" if LAUGH_SELF.search(raw) else "laugh_group"] += 1
    return w


def council_segment(lines: list[Line], cut: float) -> list[Line]:
    """The host-led stretch before the cut: host lines back from the cut while the gaps stay under HOST_GAP_S."""
    host = [ln for ln in lines if ln.speaker == HOST and ln.start < cut]
    if not host:
        return []
    start = host[-1].start
    for p, q in zip(reversed(host[:-1]), reversed(host[1:])):
        if q.start - p.end > HOST_GAP_S or cut - p.start > HOST_MAX_S:
            break
        start = p.start
    return [ln for ln in lines if start <= ln.start < cut]


def host_attention(seg: list[Line], cast: set[str]) -> tuple[collections.Counter, collections.Counter]:
    per, tot = collections.Counter(), collections.Counter()
    for i, ln in enumerate(seg):
        if ln.speaker != HOST:
            continue
        named = [c for c in ln.named if c in cast]
        tot["host_lines"] += 1
        for c in named:
            per[(c, "named")] += 1
        if "?" in ln.text:
            tot["host_q"] += 1
            who = named[:1]
            if not who:
                nxt = next((x for x in seg[i + 1:i + 4] if x.speaker in cast and x.start - ln.end < 10), None)
                who = [nxt.speaker] if nxt else []
            for c in who:
                per[(c, "q")] += 1
    return per, tot


# ----------------------------------------------------------------------------- voice (family 17)

def load_voice(path: Path) -> dict:
    out = collections.defaultdict(list)
    if not path.exists():
        return out
    with open(path) as f:
        for r in csv.DictReader(f):
            if r["domain"] != "confessional":
                continue
            vals = {k: float(r[k]) if r[k] not in ("", "nan", None) else None for k in ("f0_med", "f0_range_st", "rms_db", "rate_wps")}
            out[(r["version_season"], int(r["episode"]))].append((float(r["sub_start"]), r["speaker_id"], vals))
    return out


VOICE_KEYS = {"f0_med": "v_pitch_z", "f0_range_st": "v_range_z", "rms_db": "v_loud_z", "rate_wps": "v_rate_z"}


def voice_z(tonight: list[dict], base: list[dict]) -> dict:
    out = {"v_n": len(tonight)}
    for k, name in VOICE_KEYS.items():
        t = [v[k] for v in tonight if v[k] is not None]
        b = [v[k] for v in base if v[k] is not None]
        if t and len(b) >= VOICE_MIN_PRIOR:
            sd = float(np.std(b)) or 1.0
            out[name] = (float(np.mean(t)) - float(np.mean(b))) / sd
    return out


# ----------------------------------------------------------------------------- laughs

def load_laughs() -> dict:
    """(vs, ep) -> [(time on the subtitle clock, tag)] from survspk's cues, for episodes whose subtitles keep sound tags."""
    out = collections.defaultdict(list)
    if not nf.SPEAKERS.exists():
        return out
    con = nf.connect_ro(nf.SPEAKERS)
    has_sound = set()
    for r in con.execute("SELECT version_season, episode, start_s, lines FROM cues WHERE lines LIKE '%\"is_sound\": true%'"):
        has_sound.add((r[0], r[1]))
        for it in json.loads(r[3]):
            if it.get("is_sound") and LAUGH_ANY.search(it.get("raw") or ""):
                out[(r[0], r[1])].append((float(r[2]), it["raw"]))
    return {k: sorted(out.get(k, [])) for k in has_sound}


# ----------------------------------------------------------------------------- features

FEATURES = [
    "lsm_mean", "lsm_tribe", "lsm_min", "lsm_partners",                                  # family 11
    "uptake_n", "uptake_share", "echo_n",                                                # family 12
    "floor_share", "floor_rel", "floor_scenes", "cut_in_rate", "cut_off_rate",           # family 13
    "dup_out", "dup_in", "dup_in_min", "dup_in_n",                                       # family 14
    "host_q_share", "host_named_share", "host_q_share_prior",                            # family 15
    "laugh_n", "laugh_rate", "laugh_self_n", "laugh_group_n",                            # family 16
    "v_n", "v_pitch_z", "v_range_z", "v_loud_z", "v_rate_z",                              # family 17
    "attr_share", "attr_source",
]


def lsm(a: collections.Counter, b: collections.Counter) -> float | None:
    if a["_words"] < LSM_MIN_WORDS or b["_words"] < LSM_MIN_WORDS:
        return None
    vals = []
    for k in LSM_CATS:
        pa, pb = a[k] / a["_words"], b[k] / b["_words"]
        vals.append(1 - abs(pa - pb) / (pa + pb + 1e-4))
    return float(np.mean(vals))


def derive(w: Win, cast: list[str], tribe: dict, source: str) -> dict[str, dict]:
    out = {}
    for c in cast:
        x = w.c.get(c, collections.Counter())
        pairs = {b: lsm(w.lsm[(c, b)], w.lsm[(b, c)]) for (a, b) in list(w.lsm) if a == c}
        pairs = {b: v for b, v in pairs.items() if v is not None}
        mates = [b for b in pairs if tribe.get(b) is not None and tribe.get(b) == tribe.get(c)]
        fl = w.floor.get(c)
        gaps_out = [w.dcamp[(c, b)][0] / w.dcamp[(c, b)][1] - s / n for (a, b), (s, n) in w.dconf.items()
                    if a == c and n >= 1 and w.dcamp.get((c, b), [0, 0])[1] >= 2]
        gaps_in = [w.dcamp[(a, c)][0] / w.dcamp[(a, c)][1] - s / n for (a, b), (s, n) in w.dconf.items()
                   if b == c and n >= 1 and w.dcamp.get((a, c), [0, 0])[1] >= 2]
        out[c] = {
            "lsm_mean": float(np.mean(list(pairs.values()))) if pairs else None,
            "lsm_tribe": float(np.mean([pairs[b] for b in mates])) if mates else None,
            "lsm_min": min(pairs.values()) if pairs else None, "lsm_partners": len(pairs),
            "uptake_n": x["uptake"], "uptake_share": div(x["uptake"], w.c.get("_tot", {}).get("uptake", 0)), "echo_n": x["echo"],
            "floor_share": fl[0] / fl[2] if fl and fl[2] else None, "floor_rel": fl[1] / fl[2] if fl and fl[2] else None,
            "floor_scenes": fl[2] if fl else 0,
            "cut_in_rate": div(x["cut_in"], x["camp_lines"]) if x["camp_lines"] >= 3 else None,
            "cut_off_rate": div(x["cut_off"], x["camp_lines"]) if x["camp_lines"] >= 3 else None,
            "dup_out": float(np.mean(gaps_out)) if gaps_out else None,
            "dup_in": float(np.mean(gaps_in)) if gaps_in else None,
            "dup_in_min": min(gaps_in) if gaps_in else None, "dup_in_n": len(gaps_in),
            "laugh_n": x["laugh"] if w.laugh_ok else None,
            "laugh_rate": div(100 * x["laugh"], x["camp_lines"]) if w.laugh_ok and x["camp_lines"] >= 3 else None,
            "laugh_self_n": x["laugh_self"] if w.laugh_ok else None, "laugh_group_n": x["laugh_group"] if w.laugh_ok else None,
            "attr_share": div(w.attr, w.lines), "attr_source": source,
        }
    return out


def main(args: list[str]) -> int:
    t0 = time.time()
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    vader = SentimentIntensityAnalyzer()

    def tone(ln: Line) -> float:
        if ln.tone is None:
            ln.tone = vader.polarity_scores(ln.text)["compound"]
        return ln.tone

    out_path = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    voice = load_voice(Path(args[args.index("--voice") + 1]) if "--voice" in args else VOICE)
    laughs = load_laughs() if "--no-laughs" not in args else {}
    df = collections.Counter()          # word pair -> earlier episodes it was said in (all seasons, past only)
    n_seen = 0
    rows_out: list[dict] = []
    n_ep = 0
    prior = None
    for ep in episodes(args):
        if ep.first_of_season:
            prior = Win()
            host_hist = collections.defaultdict(list)
            vbase = collections.defaultdict(list)
        cast, cs = ep.cast, set(ep.cast)
        eps_bg = set()
        for ln in ep.lines:
            eps_bg |= bigrams(ln.text)
        common = {bg for bg in eps_bg if n_seen >= 20 and df[bg] > UPTAKE_COMMON * n_seen}
        lg = laughs.get((ep.vs, ep.ep))
        vrows = voice.get((ep.vs, ep.ep), [])
        prow = derive(prior, cast, ep.tribe, ep.source)
        for c in cast:
            prow[c]["host_q_share_prior"] = float(np.mean(host_hist[c])) if host_hist[c] else None
            rows_out.append({"window": "prior", "version_season": ep.vs, "episode": ep.ep, "k": None, "castaway_id": c, **prow[c]})
        for k, a, b in ep.windows:
            wl = ep.window(a, b)
            wlg = [(t, r) for t, r in lg if a <= t < b] if lg is not None else None
            w = window_stats(wl, cs, tone, wlg, common)
            rows = derive(w, cast, ep.tribe, ep.source)
            per, tot = host_attention(council_segment(wl, b), cs)
            for c in cast:
                rows[c]["host_q_share"] = div(per[(c, "q")], tot["host_q"])
                rows[c]["host_named_share"] = div(per[(c, "named")], tot["host_lines"])
                rows[c]["host_q_share_prior"] = float(np.mean(host_hist[c])) if host_hist[c] else None
                if vrows:
                    tv = [v for t, s_, v in vrows if s_ == c and a <= t < b]
                    rows[c].update(voice_z(tv, vbase[c]))
                rows_out.append({"window": "tonight", "version_season": ep.vs, "episode": ep.ep, "k": k, "castaway_id": c, **rows[c]})
            if tot["host_q"]:
                for c in cast:
                    host_hist[c].append(per[(c, "q")] / tot["host_q"])
        prior.add(window_stats(ep.lines, cs, tone, lg, common))
        for t, s_, v in vrows:
            vbase[s_].append(v)
        df.update(eps_bg)
        n_seen += 1
        n_ep += 1
    write_rows(out_path, FEATURES, rows_out)
    print(f"\n{n_ep} episodes, {len(rows_out)} rows in {time.time() - t0:.0f} s -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
