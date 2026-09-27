"""Families 4-7 of the NLP plan (the "Gamebot NLP features plan" doc, batches 2 and 3): how the edit presents each
player, from who speaks when. Same rows, windows, leakage rule and attribution as nlp_features.py (see
nlp_episodes.py).

  family 4  airtime and placement   seconds and lines spoken, confessional and camp share of the window, number of
                                    confessionals, gave the first / last confessional before the cut, share of the
                                    late confessionals, closed the previous episode (the last confessional after its
                                    vote by someone still playing), camp scenes spoken in
  family 5  topics                  share of their lines on strategy, alliances, advantages, challenges, camp life,
                                    backstory, conflict, bonding; spread over topics; strategy vs their prior
  family 6  compressibility, style  gzip ratio of their words, vocabulary variety (moving-window type-token ratio),
                                    words per line, questions, share of tonight's words new to them this season
  family 7  sounds like a boot      how close their lines tonight sit to earlier seasons' boots on the night they went:
                                    sentence embeddings (boot minus non-boot mean direction) and a word log-odds
                                    score, both fitted on earlier seasons only

Writes numbers only: data_cache/nlp/nlp_edit_families.csv.

    uv run --with vaderSentiment --with sentence-transformers python scripts/nlp_edit_families.py [--seasons ...] [--no-embed]
"""

from __future__ import annotations

import collections
import gzip
import math
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402
from nlp_episodes import div, episodes, write_rows  # noqa: E402
from nlp_features import WORD, Line  # noqa: E402

OUT = nf.ROOT / "data_cache/nlp/nlp_edit_families.csv"
CONF_GAP_S = 3.0          # a confessional continues across lines less than this apart
SCENE_GAP_S = 6.0         # a pause longer than this ends a camp scene
LATE_FRAC = 0.25          # "late" = the last quarter of the window
MIN_WORDS = 40            # words a player needs in a window before style features count
MIN_LINES = 3

TOPICS = {
    "strategy": r"vote|votes|voting|voted|blindside\w*|target\w*|numbers|majority|swing|flip\w*|plan|plans|move|moves|"
                r"strateg\w*|position|power|jury|endgame|end game|final (two|three|2|3)|resume|threat\w*|tribal",
    "alliance": r"alliance\w*|allies|ally|aligned|loyal\w*|trust\w*|together|side|core|pair|duo|trio|bloc",
    "advantage": r"idol\w*|advantage\w*|clue\w*|beware|extra vote|steal a vote|shot in the dark|knowledge is power|"
                 r"hidden immunity|journey",
    "challenge": r"challenge\w*|immunity|reward\w*|puzzle\w*|compet\w*|win|won|winning|lose|lost|losing|beat|physical",
    "camp": r"fire|shelter|rice|food|hungry|starv\w*|rain\w*|cold|water|coconut\w*|fish\w*|sleep\w*|camp|beach|tired|"
            r"exhausted|chop\w*|wood",
    "backstory": r"family|wife|husband|kids|son|daughter|mom|dad|mother|father|grandma|grandpa|parents|home|job|career|"
                 r"grew up|growing up|my life|cancer|died|passed away|faith|god|church",
    "conflict": r"fight\w*|argu\w*|attack\w*|drama|annoy\w*|frustrat\w*|angry|mad|rude|disrespect\w*|lazy|bossy|"
                r"yell\w*|scream\w*|pissed|irritat\w*|snake|liar|lying",
    "bonding": r"friend\w*|love|loved|hug\w*|fun|bond\w*|connect\w*|care|heart|sweet|awesome|amazing|beautiful|"
               r"laugh\w*|joke\w*|sister|brother",
}
TOPIC_RX = {k: re.compile(rf"\b({v})\b", re.I) for k, v in TOPICS.items()}
STOP = set("""a an the and or but if so to of in on at by for with from about as is are was were be been being am i
me my we us our you your he him his she her they them their it its this that these those there here what which who
whom when where why how not no do does did have has had will would can could should just like yeah oh okay ok um uh
gonna wanna got get go going know think really right all some any one two out up down then than too very""".split())


@dataclass
class Stats:
    """Additive counts for one window; the prior window is the sum over earlier episodes."""
    c: dict = field(default_factory=lambda: collections.defaultdict(collections.Counter))   # cid -> counter
    text: dict = field(default_factory=lambda: collections.defaultdict(list))               # cid -> own lines' text
    tot: collections.Counter = field(default_factory=collections.Counter)
    first_conf: str | None = None
    last_conf: str | None = None
    n_scenes: int = 0

    def add(self, o: "Stats") -> None:
        for cid, x in o.c.items():
            self.c[cid].update(x)
        for cid, t in o.text.items():
            self.text[cid].extend(t)
        self.tot.update(o.tot)
        self.n_scenes += o.n_scenes


def window_stats(lines: list[Line], cast: set[str], a: float | None = None, b: float | None = None) -> Stats:
    st = Stats()
    late_from = (a + (1 - LATE_FRAC) * (b - a)) if a is not None and b is not None else None
    conf_spk, conf_end = None, -1e9
    scene: set[str] = set()
    prev_end = -1e9

    def close_scene():
        nonlocal scene
        if len(scene) >= 2:
            st.n_scenes += 1
            for c in scene:
                st.c[c]["scene_n"] += 1
        scene = set()

    for ln in lines:
        conf = ln.domain == "confessional"
        dur = max(0.0, ln.end - ln.start)
        st.tot["lines"] += 1
        st.tot["attr"] += ln.speaker is not None
        if conf or ln.start - prev_end > SCENE_GAP_S:
            close_scene()
        if ln.speaker in cast:
            x = st.c[ln.speaker]
            words = WORD.findall(ln.text)
            x["air_secs"] += dur
            x["air_lines"] += 1
            x["words"] += len(words)
            x["questions"] += "?" in ln.text
            st.tot["air_secs"] += dur
            if conf:
                x["conf_secs"] += dur
                st.tot["conf_secs"] += dur
                if conf_spk != ln.speaker or ln.start - conf_end > CONF_GAP_S:
                    x["conf_n"] += 1
                    st.tot["conf_n"] += 1
                if st.first_conf is None:
                    st.first_conf = ln.speaker
                st.last_conf = ln.speaker
                conf_spk, conf_end = ln.speaker, ln.end
                if late_from is not None and ln.start >= late_from:
                    x["late_conf_secs"] += dur
                    st.tot["late_conf_secs"] += dur
            else:
                x["camp_secs"] += dur
                st.tot["camp_secs"] += dur
                scene.add(ln.speaker)
            for k, rx in TOPIC_RX.items():
                x["top_" + k] += bool(rx.search(ln.text))
            st.text[ln.speaker].append(ln.text)
        elif conf:
            conf_spk = None
        prev_end = ln.end
    close_scene()
    return st


def mattr(tokens: list[str], w: int = 40) -> float | None:
    if len(tokens) < w:
        return None
    return float(np.mean([len(set(tokens[i:i + w])) / w for i in range(0, len(tokens) - w + 1, 5)]))


def style(texts: list[str], prior_vocab: set[str] | None = None) -> dict:
    body = " ".join(texts)
    toks = [t.lower() for t in WORD.findall(body)]
    if len(toks) < MIN_WORDS:
        return {}
    raw = body.encode()[-20000:]
    content = [t for t in toks if t not in STOP and len(t) > 2]
    out = {"sty_gzip": len(gzip.compress(raw)) / len(raw), "sty_mattr": mattr(toks), "sty_wpl": len(toks) / max(1, len(texts))}
    if prior_vocab is not None and len(prior_vocab) >= 100 and content:
        out["sty_novelty"] = sum(t not in prior_vocab for t in set(content)) / len(set(content))
    return out


def vocab(texts: list[str]) -> set[str]:
    return {t.lower() for t in WORD.findall(" ".join(texts)) if len(t) > 2 and t.lower() not in STOP}


# ----------------------------------------------------------------------------- family 7: sounds like a boot

@dataclass
class BootModel:
    """Fitted on finished seasons only: a boot-minus-survivor direction in embedding space (all own lines and
    confessionals kept apart) and word log-odds."""
    vsum: dict = field(default_factory=lambda: {("all", 1): None, ("all", 0): None, ("conf", 1): None, ("conf", 0): None})
    vn: collections.Counter = field(default_factory=collections.Counter)
    words: dict = field(default_factory=lambda: {1: collections.Counter(), 0: collections.Counter()})
    pending: list = field(default_factory=list)     # this season's examples, added when the season is over
    _dir: dict = field(default_factory=dict)
    _lo: dict = field(default_factory=dict)

    def note(self, label: int, cents: dict, texts: list[str]) -> None:
        self.pending.append((label, cents, texts))

    def season_over(self) -> None:
        for label, cents, texts in self.pending:
            for dom, v in cents.items():
                k = (dom, label)
                self.vsum[k] = v.copy() if self.vsum[k] is None else self.vsum[k] + v
                self.vn[k] += 1
            self.words[label].update(t.lower() for t in WORD.findall(" ".join(texts)) if len(t) > 2)
        self.pending = []
        self._dir = {}
        for dom in ("all", "conf"):
            if self.vn[(dom, 1)] >= 20 and self.vn[(dom, 0)] >= 20:
                d = self.vsum[(dom, 1)] / self.vn[(dom, 1)] - self.vsum[(dom, 0)] / self.vn[(dom, 0)]
                self._dir[dom] = d / (np.linalg.norm(d) + 1e-12)
        n1, n0 = sum(self.words[1].values()), sum(self.words[0].values())
        vocab_ = {w for w in set(self.words[1]) | set(self.words[0]) if self.words[1][w] + self.words[0][w] >= 10}
        V = len(vocab_) or 1
        self._lo = {w: math.log((self.words[1][w] + 1) / (n1 + V)) - math.log((self.words[0][w] + 1) / (n0 + V))
                    for w in vocab_} if n1 and n0 else {}

    def score(self, cents: dict, texts: list[str]) -> dict:
        out = {}
        for dom, d in self._dir.items():
            if dom in cents:
                v = cents[dom]
                out[f"boot_sim_{dom}"] = float(v @ d / (np.linalg.norm(v) + 1e-12))
        if self._lo:
            toks = [t.lower() for t in WORD.findall(" ".join(texts)) if t.lower() in self._lo]
            if len(toks) >= 20:
                out["boot_lex"] = float(np.mean([self._lo[t] for t in toks]))
        return out


def centroids(lines: list[Line], idx: list[int], vecs: np.ndarray | None, cid: str) -> dict:
    if vecs is None:
        return {}
    out = {}
    own = [i for i in idx if lines[i].speaker == cid]
    conf = [i for i in own if lines[i].domain == "confessional"]
    for dom, ii in (("all", own), ("conf", conf)):
        if len(ii) >= MIN_LINES:
            out[dom] = vecs[ii].mean(0).astype(np.float64)
    return out


# ----------------------------------------------------------------------------- features

FEATURES = [
    # family 4
    "air_secs", "air_lines", "air_share", "conf_n", "conf_share", "camp_share", "conf_zero", "conf_first", "conf_last",
    "conf_late_share", "closer_prev", "scene_n", "scene_share", "conf_first_rate", "conf_last_rate", "closer_rate",
    # family 5
    "top_strategy", "top_alliance", "top_advantage", "top_challenge", "top_camp", "top_backstory", "top_conflict",
    "top_bonding", "top_entropy", "top_strategy_delta",
    # family 6
    "sty_gzip", "sty_mattr", "sty_wpl", "sty_question", "sty_novelty",
    # family 7
    "boot_sim_all", "boot_sim_conf", "boot_lex",
    # coverage
    "attr_share", "attr_source",
]


def derive(st: Stats, cast: list[str], source: str, *, tonight: bool, record: dict | None = None,
           closer: str | None = None, prior: Stats | None = None, n_councils: int = 0, n_eps: int = 0) -> dict[str, dict]:
    out = {}
    for c in cast:
        x = st.c.get(c, collections.Counter())
        tops = {k: div(x["top_" + k], x["air_lines"]) if x["air_lines"] >= MIN_LINES else None for k in TOPICS}
        vals = [v for v in tops.values() if v]
        ent = None
        if vals and sum(vals) > 0:
            p = np.array(vals) / sum(vals)
            ent = float(-(p * np.log(p)).sum() / math.log(len(TOPICS)))
        prior_vocab = vocab(prior.text.get(c, [])) if prior is not None else None
        row = {
            "air_secs": x["air_secs"], "air_lines": x["air_lines"], "air_share": div(x["air_secs"], st.tot["air_secs"]),
            "conf_n": x["conf_n"], "conf_share": div(x["conf_secs"], st.tot["conf_secs"]),
            "camp_share": div(x["camp_secs"], st.tot["camp_secs"]),
            "conf_zero": int(x["conf_n"] == 0) if st.tot["conf_n"] else None,
            "scene_n": x["scene_n"], "scene_share": div(x["scene_n"], st.n_scenes),
            **{"top_" + k: v for k, v in tops.items()}, "top_entropy": ent,
            "sty_question": div(x["questions"], x["air_lines"]) if x["air_lines"] >= MIN_LINES else None,
            **style(st.text.get(c, []), prior_vocab if tonight else None),
            "attr_share": div(st.tot["attr"], st.tot["lines"]), "attr_source": source,
        }
        if tonight:
            row["conf_first"] = int(st.first_conf == c) if st.first_conf else None
            row["conf_last"] = int(st.last_conf == c) if st.last_conf else None
            row["conf_late_share"] = div(x["late_conf_secs"], st.tot["late_conf_secs"])
            row["closer_prev"] = int(closer == c) if closer else None
            if prior is not None:
                px = prior.c.get(c, collections.Counter())
                ps = div(px["top_strategy"], px["air_lines"]) if px["air_lines"] >= MIN_LINES else None
                row["top_strategy_delta"] = (tops["strategy"] - ps) if tops["strategy"] is not None and ps is not None else None
        elif record is not None:
            row["conf_first_rate"] = div(record["first"][c], n_councils)
            row["conf_last_rate"] = div(record["last"][c], n_councils)
            row["closer_rate"] = div(record["closer"][c], n_eps)
        out[c] = row
    return out


def closer_of(ep, st_lines: list[Line]) -> str | None:
    """The last confessional after the episode's final vote by someone still playing (the post-vote reaction)."""
    ks = [k for k, t in ep.tallies.items() if t is not None]
    if not ks:
        return None
    t = max(ep.tallies[k] for k in ks)
    gone = set(ep.boots.values())
    for ln in reversed(st_lines):
        if ln.start < t:
            break
        if ln.domain == "confessional" and ln.speaker in ep.cast and ln.speaker not in gone:
            return ln.speaker
    return None


def main(args: list[str], embed=None) -> int:
    t0 = time.time()
    out_path = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    use_embed = "--no-embed" not in args
    if use_embed and embed is None:
        from nlp_speaker_families import Embedder
        embed = Embedder()
    bm = BootModel()
    rows_out: list[dict] = []
    n_ep = 0
    prior = record = None
    last_closer = None
    cur_vs = None
    for ep in episodes(args):
        if ep.first_of_season:
            if cur_vs is not None:
                bm.season_over()
            cur_vs = ep.vs
            prior, record, last_closer = Stats(), {"first": collections.Counter(), "last": collections.Counter(),
                                                   "closer": collections.Counter()}, None
            n_councils = n_eps = 0
        cast, cs = ep.cast, set(ep.cast)
        vecs = embed(f"{ep.vs}_E{ep.ep:02d}", [ln.text for ln in ep.lines]) if use_embed and ep.lines else None
        prow = derive(prior, cast, ep.source, tonight=False, record=record, n_councils=n_councils, n_eps=n_eps)
        for c in cast:
            rows_out.append({"window": "prior", "version_season": ep.vs, "episode": ep.ep, "k": None, "castaway_id": c, **prow[c]})
        for k, a, b in ep.windows:
            idx = [i for i, ln in enumerate(ep.lines) if a <= ln.start < b]
            wl = [ep.lines[i] for i in idx]
            st = window_stats(wl, cs, a, b)
            rows = derive(st, cast, ep.source, tonight=True, closer=last_closer, prior=prior)
            boot = ep.boots.get(k)
            for c in cast:
                cents = centroids(ep.lines, idx, vecs, c)
                rows[c].update(bm.score(cents, st.text.get(c, [])))
                if boot and cents:
                    bm.note(int(c == boot), cents, st.text.get(c, []))
                rows_out.append({"window": "tonight", "version_season": ep.vs, "episode": ep.ep, "k": k, "castaway_id": c, **rows[c]})
            if st.first_conf:
                record["first"][st.first_conf] += 1
            if st.last_conf:
                record["last"][st.last_conf] += 1
            n_councils += 1
        whole = window_stats(ep.lines, cs)
        prior.add(whole)
        last_closer = closer_of(ep, ep.lines)
        if last_closer:
            record["closer"][last_closer] += 1
        n_eps += 1
        n_ep += 1
    write_rows(out_path, FEATURES, rows_out)
    print(f"\n{n_ep} episodes, {len(rows_out)} rows in {time.time() - t0:.0f} s -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
