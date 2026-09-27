"""NLP features at the castaway x season x episode grain, for the boot model (plan: the "Gamebot NLP features plan" doc).

Batch 1, from the subtitle text (`subtitle_mine.py`'s subtitles.sqlite) and, where they exist, audio speaker labels
(survivor-speakers' survspk.sqlite):

  family 1  being talked about   mentions, target talk, confessional mentions, distinct speakers naming them,
                                 own tribe vs other tribe, the host naming them, in-degree and PageRank of the
                                 who-names-whom network
  family 2  directed sentiment   VADER tone of the lines that name them; threat and trust words in those lines
  family 3  edit tells           in their own lines: safety, control, trust claims, hedging vs certainty, worry,
                                 "I" vs "we", own tone (only where a speaker is known)

Two windows per row (the leakage rule):
  tonight  episode e from the end of the recap to the council's cut ("time to vote", or the median gap before
           "go tally the votes"); a second council in the episode starts at the first council's tally
  prior    every body line of episodes 1..e-1 (recaps dropped)

Speakers: an audio label (human, SDH, name card, or auto at confidence >= 0.6) wins when the episode has them;
otherwise a caption name (SDH "NAME:") and the lines that follow it until a turn marker, a pause over 3 s, or six
cues. Every row says how much of its window had a known speaker (`attr_share`) and from where (`attr_source`).

Writes numbers only: data_cache/nlp/nlp_player_episode.csv (git-ignored). Nothing here copies dialogue.

    uv run --with vaderSentiment python scripts/nlp_features.py [--labels speaker_labels.csv] [--seasons US45,US46]
"""

from __future__ import annotations

import collections
import csv
import math
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import subtitle_mine as sm  # noqa: E402  (name patterns, target words, aliases: the same matching as the mentions)

ROOT = Path(__file__).resolve().parents[1]
STARGAZER = ROOT.parent
SUBS = ROOT / "data_cache/subtitles/subtitles.sqlite"
GAMEBOT = ROOT / "gamebot_lite/data/gamebot.sqlite"
SPEAKERS = STARGAZER / "survivor_audio/db/survspk.sqlite"
OUT = ROOT / "data_cache/nlp/nlp_player_episode.csv"

HOST = "HOST"
HOST_NAMES = {"PROBST", "JEFF", "JEFF PROBST"}
INHERIT_MAX_CUES = 6
INHERIT_MAX_GAP_S = 3.0
AUTO_MIN_CONF = 0.6

# family 2: what others say about someone
THREAT = re.compile(r"\b(threat|threats|threatening|dangerous|snake|liar|lying|lied|can'?t trust|cannot trust|backstab\w*|"
                    r"sneaky|shady|coming for|gunning for|big move|win (this|the) game|jury threat|challenge beast|"
                    r"too good|has to go|needs? to go|got to go|gotta go)\b", re.I)
TRUST = re.compile(r"\b(trust|trusted|loyal|loyalty|ride or die|final (two|three|3|2)|number one|closest ally|"
                   r"love (him|her)|my girl|my guy|got my back|have my back|protect)\b", re.I)
# family 3: a player's own lines
SAFE = re.compile(r"\b(i'?m safe|we'?re safe|i am safe|nobody'?s (coming|gunning) for me|no one'?s (coming|gunning) for me|"
                  r"not going (home|anywhere)|i'?m not worried|not worried|feel(ing)? (good|safe|great|comfortable)|"
                  r"i'?m good|we'?re good|i'?m fine|nothing to worry)\b", re.I)
CONTROL = re.compile(r"\b(running (this|the) game|in control|calling the shots|pulling (the )?strings|i'?m the swing|"
                     r"in the middle of (everything|it all)|my position|in a great position|in a good position|"
                     r"i run|i'?m running|puppet master|mastermind)\b", re.I)
TRUSTCLAIM = re.compile(r"\b(i trust|i do trust|trust (him|her|them) (completely|100|fully)|would never (vote|write)|"
                        r"loyal to me|has my back|have my back)\b", re.I)
HEDGE = re.compile(r"\b(i think|maybe|i guess|not sure|kind of|kinda|sort of|hopefully|i hope|probably|might)\b", re.I)
CERTAIN = re.compile(r"\b(definitely|for sure|guarantee\w*|100 percent|hundred percent|no doubt|absolutely|"
                     r"without a doubt|certainly|i know)\b", re.I)
WORRY = re.compile(r"\b(worried|scared|nervous|paranoid|on the bottom|in trouble|target on my back|i'?m next|"
                   r"on the chopping block|in danger|vulnerable)\b", re.I)
I_WORDS = re.compile(r"\b(i|i'm|i've|i'll|i'd|me|my|mine|myself)\b", re.I)
WE_WORDS = re.compile(r"\b(we|we're|we've|we'll|we'd|us|our|ours|ourselves)\b", re.I)
WORD = re.compile(r"[A-Za-z']+")


@dataclass
class Line:
    start: float
    end: float
    text: str
    italic: int
    speaker: str | None = None       # castaway id, HOST, or None
    domain: str | None = None        # "confessional" | "field" from the audio labels; None when unknown
    named: frozenset = frozenset()   # castaway ids named in the line
    tone: float | None = None        # VADER compound, computed when needed


@dataclass
class Tally:
    """Raw counts for one castaway in one window; sums across episodes give the prior window."""
    c: collections.Counter = field(default_factory=collections.Counter)
    namers: set = field(default_factory=set)


def connect_ro(path: Path, immutable: bool = False) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{path}?mode=ro{'&immutable=1' if immutable else ''}", uri=True)
    con.row_factory = sqlite3.Row
    return con


def load_audio_labels(args) -> dict[tuple[str, int], list[tuple[float, float, str, str | None]]]:
    """(vs, ep) -> [(start, end, speaker, domain)] of trusted audio labels, from a CSV export or the survspk DB.

    Times are on the subtitle clock (the span of the utterance's cues), the clock this script's cues and council cuts
    use. survspk's own utterance times are on the audio clock since align maps cues to where the words are heard;
    the two differ by seconds on loosely timed subtitle files (US31) and by up to 12 s where a subtitle comes from a
    different cut (US47 E11)."""
    rows: list[tuple] = []
    if "--labels" in args:
        with open(args[args.index("--labels") + 1]) as f:
            for r in csv.DictReader(f):
                rows.append((r["version_season"], int(r["episode"]), float(r["start_s"]), float(r["end_s"]), r["speaker_id"],
                             r["source"], float(r["confidence"] or 0), r.get("domain") or None))
    elif SPEAKERS.exists():
        con = connect_ro(SPEAKERS)
        rows = [tuple(r) for r in con.execute(
            """SELECT u.version_season, u.episode, MIN(c.start_s), MAX(c.end_s), l.speaker_id, l.source,
                      COALESCE(l.confidence, 0), u.domain_hint
               FROM labels l JOIN utterances u USING (utt_id) JOIN utterance_cues uc USING (utt_id)
                    JOIN cues c ON c.cue_id = uc.cue_id
               WHERE u.segment = 'body' AND l.speaker_id NOT IN ('UNKNOWN', 'NOSPEECH', 'OTHER')
               GROUP BY u.utt_id""")]
    out: dict = collections.defaultdict(list)
    for vs, ep, a, b, spk, src, conf, dom in rows:
        if src in ("human", "sdh", "chyron") or (src == "auto" and conf >= AUTO_MIN_CONF):
            out[(vs, ep)].append((a, b, HOST if spk.startswith("HOST") else spk, dom))
    for v in out.values():
        v.sort()
    return out


def attribute(lines: list[Line], cues: list[sqlite3.Row], audio: list[tuple] | None) -> str:
    """Set each line's speaker; returns the source used for the episode."""
    if audio:
        j = 0
        for ln in lines:
            while j < len(audio) and audio[j][1] < ln.start:
                j += 1
            best, dom, ov = None, None, 0.0
            k = j
            while k < len(audio) and audio[k][0] <= ln.end:
                o = min(ln.end, audio[k][1]) - max(ln.start, audio[k][0])
                if o > ov:
                    best, dom, ov = audio[k][2], (audio[k][3] if len(audio[k]) > 3 else None), o
                k += 1
            ln.speaker, ln.domain = best, dom
        return "audio"
    cur, left, last_end = None, 0, -1e9
    for ln, r in zip(lines, cues):
        name = (r["sdh_name"] or "").upper().strip()
        if r["speaker_id"] or name in HOST_NAMES:
            cur, left = (r["speaker_id"] or HOST), INHERIT_MAX_CUES
        elif r["turn"] or r["sdh_name"] or left <= 0 or ln.start - last_end > INHERIT_MAX_GAP_S:
            cur, left = None, 0          # a turn, a name we cannot resolve, or the run ran out: unknown from here
        ln.speaker = cur
        left -= 1
        last_end = ln.end
    return "captions" if any(r["speaker_id"] for r in cues) else "none"


def tally_window(lines: list[Line], cast: set[str], tribe: dict[str, str], tone) -> tuple[dict[str, Tally], collections.Counter, dict]:
    """Per-castaway counts for one window, the window's totals, and its who-names-whom edge weights."""
    t = {cid: Tally() for cid in cast}
    tot = collections.Counter()
    edges: collections.Counter = collections.Counter()
    for ln in lines:
        tot["cues"] += 1
        tot["attr_cues"] += ln.speaker is not None
        named = [c for c in ln.named if c in cast and c != ln.speaker]
        target = bool(sm.TARGET.search(ln.text))
        for cid in named:
            x = t[cid].c
            tot["mentions"] += 1
            tot["target_mentions"] += target
            x["m_n"] += 1
            x["m_target_n"] += target
            x["m_conf_n"] += ln.italic
            x["m_threat_n"] += bool(THREAT.search(ln.text))
            x["m_trust_n"] += bool(TRUST.search(ln.text))
            s = tone(ln)
            x["m_tone_sum"] += s
            x["m_tone_n"] += 1
            x["m_neg_n"] += s <= -0.05
            x["m_pos_n"] += s >= 0.05
            if ln.speaker == HOST:
                x["m_host_n"] += 1
            elif ln.speaker in cast:
                t[cid].namers.add(ln.speaker)
                edges[(ln.speaker, cid)] += 1
                same = tribe.get(ln.speaker) is not None and tribe.get(ln.speaker) == tribe.get(cid)
                x["m_own_tribe_n" if same else "m_other_tribe_n"] += 1
                x["m_known_speaker_n"] += 1
        if ln.speaker in cast:
            x = t[ln.speaker].c
            words = WORD.findall(ln.text)
            x["own_lines"] += 1
            x["own_words"] += len(words)
            x["own_secs"] += max(0.0, ln.end - ln.start)
            x["own_conf_lines"] += ln.italic
            for key, rx in (("own_safe_n", SAFE), ("own_control_n", CONTROL), ("own_trustclaim_n", TRUSTCLAIM),
                            ("own_hedge_n", HEDGE), ("own_certain_n", CERTAIN), ("own_worry_n", WORRY)):
                x[key] += len(rx.findall(ln.text))
            x["own_i_n"] += len(I_WORDS.findall(ln.text))
            x["own_we_n"] += len(WE_WORDS.findall(ln.text))
            s = tone(ln)
            x["own_tone_sum"] += s
            x["own_neg_n"] += s <= -0.05
            tot["own_lines_all"] += 1
            tot["own_secs_all"] += max(0.0, ln.end - ln.start)
    return t, tot, edges


def pagerank(nodes: list[str], edges: dict, d: float = 0.85, iters: int = 60) -> dict[str, float]:
    """Weighted PageRank on the who-names-whom graph (an edge speaker -> named); dangling mass spread evenly."""
    if not nodes:
        return {}
    n = len(nodes)
    pr = {v: 1.0 / n for v in nodes}
    out_w = collections.Counter()
    for (a, b), w in edges.items():
        out_w[a] += w
    for _ in range(iters):
        nxt = {v: (1 - d) / n for v in nodes}
        dangling = sum(pr[v] for v in nodes if out_w[v] == 0)
        for v in nodes:
            nxt[v] += d * dangling / n
        for (a, b), w in edges.items():
            if a in pr and b in nxt:
                nxt[b] += d * pr[a] * w / out_w[a]
        pr = nxt
    return pr


FEATURES = [
    # family 1
    "m_n", "m_share", "m_target_n", "m_target_share", "m_conf_share", "m_namers", "m_own_tribe_share", "m_host_n",
    "m_indegree_share", "m_pagerank",
    # family 2
    "m_tone_mean", "m_neg_share", "m_pos_share", "m_threat_rate", "m_trust_rate",
    # family 3
    "own_lines", "own_words", "own_secs_share", "own_conf_share", "own_safe_rate", "own_control_rate",
    "own_trustclaim_rate", "own_hedge_rate", "own_certain_rate", "own_worry_rate", "own_we_ratio", "own_i_rate",
    "own_tone_mean", "own_neg_share",
    # coverage
    "attr_share", "attr_source", "n_cues",
]


def derive(x: collections.Counter, namers: set, tot: collections.Counter, indeg: float, tot_indeg: float, pr: float | None,
           source: str) -> dict:
    div = lambda a, b: (a / b) if b else None  # noqa: E731
    per100 = lambda k: div(100 * x[k], x["own_words"]) if x["own_words"] >= 20 else None  # noqa: E731
    return {
        "m_n": x["m_n"], "m_share": div(x["m_n"], tot["mentions"]), "m_target_n": x["m_target_n"],
        "m_target_share": div(x["m_target_n"], tot["target_mentions"]), "m_conf_share": div(x["m_conf_n"], x["m_n"]),
        "m_namers": len(namers), "m_own_tribe_share": div(x["m_own_tribe_n"], x["m_known_speaker_n"]),
        "m_host_n": x["m_host_n"], "m_indegree_share": div(indeg, tot_indeg), "m_pagerank": pr,
        "m_tone_mean": div(x["m_tone_sum"], x["m_tone_n"]), "m_neg_share": div(x["m_neg_n"], x["m_tone_n"]),
        "m_pos_share": div(x["m_pos_n"], x["m_tone_n"]), "m_threat_rate": div(x["m_threat_n"], x["m_n"]),
        "m_trust_rate": div(x["m_trust_n"], x["m_n"]),
        "own_lines": x["own_lines"], "own_words": x["own_words"], "own_secs_share": div(x["own_secs"], tot["own_secs_all"]),
        "own_conf_share": div(x["own_conf_lines"], x["own_lines"]),
        "own_safe_rate": per100("own_safe_n"), "own_control_rate": per100("own_control_n"),
        "own_trustclaim_rate": per100("own_trustclaim_n"), "own_hedge_rate": per100("own_hedge_n"),
        "own_certain_rate": per100("own_certain_n"), "own_worry_rate": per100("own_worry_n"),
        "own_we_ratio": div(x["own_we_n"], x["own_we_n"] + x["own_i_n"]), "own_i_rate": per100("own_i_n"),
        "own_tone_mean": div(x["own_tone_sum"], x["own_lines"]), "own_neg_share": div(x["own_neg_n"], x["own_lines"]),
        "attr_share": div(tot["attr_cues"], tot["cues"]), "attr_source": source, "n_cues": tot["cues"],
    }


def windows(recap_end: float, councils: list[sqlite3.Row]) -> list[tuple[int, float, float]]:
    """(k, start, end) of each council's `tonight` window: from the recap's end (or the previous council's tally) to
    the council's cut. Councils without a cut get none."""
    out, lo = [], recap_end
    for r in sorted(councils, key=lambda r: r["k"]):
        if r["cut_s"] is not None and r["cut_s"] > lo:
            out.append((r["k"], lo, r["cut_s"]))
        if r["tally_s"] is not None:
            lo = max(lo, r["tally_s"])
    return out


def main(args: list[str]) -> int:
    t0 = time.time()
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    vader = SentimentIntensityAnalyzer()

    def tone(ln: Line) -> float:
        if ln.tone is None:
            ln.tone = vader.polarity_scores(ln.text)["compound"]
        return ln.tone

    subs = connect_ro(Path(args[args.index("--subs") + 1]) if "--subs" in args else SUBS, immutable=True)
    gb = connect_ro(Path(args[args.index("--gamebot") + 1]) if "--gamebot" in args else GAMEBOT)
    out_path = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    only = set(args[args.index("--seasons") + 1].split(",")) if "--seasons" in args else None
    audio = load_audio_labels(args)
    alias = sm.aliases()
    seasons = [r[0] for r in gb.execute("SELECT version_season FROM season_summary WHERE version = 'US' ORDER BY season")]
    rows_out: list[dict] = []
    n_ep = 0
    for vs in seasons:
        if only and vs not in only:
            continue
        pats = sm._patterns(gb, alias, vs)
        eps = [r[0] for r in subs.execute("SELECT DISTINCT episode FROM cues WHERE version_season=? ORDER BY episode", (vs,))]
        prior = collections.defaultdict(Tally)          # castaway -> summed counts over earlier episodes
        prior_tot = collections.Counter()
        prior_edges: collections.Counter = collections.Counter()
        prior_src: set[str] = set()
        for ep in eps:
            bm = gb.execute("""SELECT castaway_id, tribe FROM boot_mapping WHERE version_season=? AND episode=?
                               ORDER BY boot_mapping_order""", (vs, ep)).fetchall()
            if not bm:
                continue
            cast = {r["castaway_id"] for r in bm}
            tribe = {r["castaway_id"]: r["tribe"] for r in bm}
            rec = subs.execute("SELECT recap_end_s FROM recaps WHERE version_season=? AND episode=?", (vs, ep)).fetchone()
            recap_end = float(rec[0]) if rec and rec[0] is not None else 0.0
            cues = subs.execute("""SELECT start_s, end_s, text, sdh_name, speaker_id, italic, turn, sound FROM cues
                                   WHERE version_season=? AND episode=? AND start_s >= ? ORDER BY idx""",
                                (vs, ep, recap_end)).fetchall()
            cues = [r for r in cues if r["text"] and not r["sound"]]
            lines = [Line(r["start_s"], r["end_s"], r["text"], int(r["italic"] or 0)) for r in cues]
            source = attribute(lines, cues, audio.get((vs, ep)))
            for ln in lines:
                ln.named = frozenset(cid for cid, p in pats.items() if cid in cast and sm._hit(p, ln.text))

            # prior window: everything before this episode
            pr = pagerank(sorted(cast), {k: v for k, v in prior_edges.items() if k[0] in cast and k[1] in cast})
            indeg = collections.Counter()
            for (a, b), w in prior_edges.items():
                if a in cast and b in cast:
                    indeg[b] += w
            tot_in = sum(indeg.values())
            for cid in sorted(cast):
                p = prior.get(cid, Tally())
                rows_out.append({"window": "prior", "version_season": vs, "episode": ep, "k": None, "castaway_id": cid,
                                 **derive(p.c, p.namers, prior_tot, indeg[cid], tot_in, pr.get(cid) if prior_edges else None,
                                          "+".join(sorted(prior_src)) or "none")})

            # tonight windows: one per council with a cut
            councils = subs.execute("SELECT k, cut_s, tally_s FROM tribals WHERE version_season=? AND episode=?", (vs, ep)).fetchall()
            for k, a, b in windows(recap_end, councils):
                w = [ln for ln in lines if a <= ln.start < b]
                t, tot, edges = tally_window(w, cast, tribe, tone)
                pr_t = pagerank(sorted(cast), edges)
                indeg_t = collections.Counter()
                for (x, y), wt in edges.items():
                    indeg_t[y] += wt
                tin = sum(indeg_t.values())
                for cid in sorted(cast):
                    rows_out.append({"window": "tonight", "version_season": vs, "episode": ep, "k": k, "castaway_id": cid,
                                     **derive(t[cid].c, t[cid].namers, tot, indeg_t[cid], tin, pr_t.get(cid) if edges else None, source)})

            # this whole episode joins the prior of the next ones
            t, tot, edges = tally_window(lines, cast, tribe, tone)
            for cid, x in t.items():
                prior[cid].c.update(x.c)
                prior[cid].namers |= x.namers
            prior_tot.update(tot)
            prior_edges.update(edges)
            prior_src.add(source)
            n_ep += 1
        print(vs, end=" ", flush=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["window", "version_season", "episode", "k", "castaway_id"] + FEATURES
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows_out:
            w.writerow({c: (round(r[c], 5) if isinstance(r[c], float) and math.isfinite(r[c]) else r[c]) for c in cols})
    n_t = sum(r["window"] == "tonight" for r in rows_out)
    print(f"\n{n_ep} episodes, {len(rows_out)} rows ({n_t} tonight, {len(rows_out) - n_t} prior) in {time.time() - t0:.0f} s -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
