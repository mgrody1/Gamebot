"""Families 8-10 of the NLP plan (the "Gamebot NLP features plan" doc, batch 5a): features that need to know who
said each line. Same rows, windows and leakage rule as nlp_features.py (tonight = recap end to the council's cut;
prior = episodes before this one), same speaker attribution (audio labels, else caption names).

  family 8   semantic similarity   sentence embeddings of each player's camp lines (confessionals kept apart; game
                                   talk, i.e. vote words or another player's name, left out):
                                   similarity to their tribemates, to the whole cast, to their closest tribemate,
                                   and how their tribe similarity moved against the prior window
  family 9   agenda-setting        vote talk = a line with vote/target words that names another player. From past
                                   councils only: who first named the eventual boot, and how often a player's vote
                                   talk pointed at the eventual boot. Tonight: how much of the vote talk naming a
                                   player comes from people who set past agendas
  family 10  conversation network  camp scenes (lines less than 6 s apart, broken by a confessional); an exchange is
                                   one known speaker followed by another; partners, share of exchanges, share of
                                   tribemates talked with, PageRank, scenes joined

Writes numbers only: data_cache/nlp/nlp_speaker_families.csv. Embeddings are cached under data_cache/nlp/emb.

    uv run --with vaderSentiment --with sentence-transformers python scripts/nlp_speaker_families.py [--seasons US45,US47]
"""

from __future__ import annotations

import collections
import csv
import hashlib
import math
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402
from nlp_features import HOST, Line, sm  # noqa: E402

OUT = nf.ROOT / "data_cache/nlp/nlp_speaker_families.csv"
EMB_DIR = nf.ROOT / "data_cache/nlp/emb"
MODEL = "sentence-transformers/all-MiniLM-L6-v2"
MIN_LINES = 3            # lines a player needs in a window before their centroid counts
SCENE_GAP_S = 6.0        # a pause longer than this ends a camp scene
MIN_VOTE_TALK = 3        # vote-talk lines before a player's hit share counts


# ----------------------------------------------------------------------------- embeddings

class Embedder:
    """Unit-length sentence embeddings, cached per episode (keyed by the texts, so a changed subtitle re-embeds)."""

    def __init__(self, model: str = MODEL, cache: Path | None = EMB_DIR):
        self.model_name, self.cache, self._m = model, cache, None

    def __call__(self, key: str, texts: list[str]) -> np.ndarray:
        h = hashlib.sha1("\n".join(texts).encode()).hexdigest()[:12]
        path = self.cache / f"{key}_{h}.npy" if self.cache else None
        if path is not None and path.exists():
            return np.load(path).astype(np.float32)
        if self._m is None:
            import torch
            from sentence_transformers import SentenceTransformer
            dev = "mps" if torch.backends.mps.is_available() else "cpu"
            self._m = SentenceTransformer(self.model_name, device=dev)
        v = self._m.encode(texts, batch_size=256, normalize_embeddings=True, show_progress_bar=False).astype(np.float32)
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            np.save(path, v.astype(np.float16))
        return v


# ----------------------------------------------------------------------------- window statistics

@dataclass
class Win:
    """Additive counts for one window; the prior window is the sum over earlier episodes."""
    camp: dict = field(default_factory=dict)       # cid -> [vector sum, n] over camp lines
    conf: dict = field(default_factory=dict)       # cid -> [vector sum, n] over confessional lines
    edges: collections.Counter = field(default_factory=collections.Counter)   # frozenset{a, b} -> exchanges
    turns: collections.Counter = field(default_factory=collections.Counter)   # camp lines per speaker
    scenes: collections.Counter = field(default_factory=collections.Counter)  # multi-player scenes joined
    n_scenes: int = 0
    vt_by: collections.Counter = field(default_factory=collections.Counter)   # vote-talk lines per speaker
    vt_named: dict = field(default_factory=lambda: collections.defaultdict(collections.Counter))  # named -> speaker -> n
    first_named: str | None = None                 # first player named in vote talk in the window
    lines: int = 0
    attr: int = 0

    def add(self, o: "Win") -> None:
        for mine, theirs in ((self.camp, o.camp), (self.conf, o.conf)):
            for c, (v, n) in theirs.items():
                if c in mine:
                    mine[c][0] = mine[c][0] + v
                    mine[c][1] += n
                else:
                    mine[c] = [v.copy(), n]
        self.edges.update(o.edges)
        self.turns.update(o.turns)
        self.scenes.update(o.scenes)
        self.n_scenes += o.n_scenes
        self.vt_by.update(o.vt_by)
        for c, cnt in o.vt_named.items():
            self.vt_named[c].update(cnt)
        self.lines += o.lines
        self.attr += o.attr


def is_vote_talk(ln: Line, cast: set[str]) -> list[str]:
    """Players a vote-talk line names (not the speaker); empty when the line is not vote talk."""
    if ln.speaker not in cast or not sm.TARGET.search(ln.text):
        return []
    return sorted(c for c in ln.named if c in cast and c != ln.speaker)


def window_stats(lines: list[Line], vecs: np.ndarray | None, cast: set[str]) -> Win:
    w = Win()
    scene: list[str] = []          # castaways speaking in the current scene
    prev_spk, prev_end = None, -1e9

    def close_scene():
        nonlocal scene
        who = set(scene)
        if len(who) >= 2:
            w.n_scenes += 1
            for c in who:
                w.scenes[c] += 1
        scene = []

    for i, ln in enumerate(lines):
        w.lines += 1
        w.attr += ln.speaker is not None
        conf = ln.domain == "confessional"
        # centroids leave out game talk (vote words or another player's name): it is the same topic for everyone
        # before a vote and made boots look like their tribe on the night they went
        if ln.speaker in cast and vecs is not None and not sm.TARGET.search(ln.text) and not (ln.named - {ln.speaker}):
            bucket = w.conf if conf else w.camp
            if ln.speaker in bucket:
                bucket[ln.speaker][0] = bucket[ln.speaker][0] + vecs[i]
                bucket[ln.speaker][1] += 1
            else:
                bucket[ln.speaker] = [vecs[i].astype(np.float64), 1]
        named = is_vote_talk(ln, cast)
        if named:
            w.vt_by[ln.speaker] += 1
            for c in named:
                w.vt_named[c][ln.speaker] += 1
            if w.first_named is None:
                w.first_named = named[0] if len(named) == 1 else None
        # scenes and exchanges (camp only)
        if conf or ln.start - prev_end > SCENE_GAP_S:
            close_scene()
            prev_spk = None
        if conf:
            prev_end = ln.end
            continue
        if ln.speaker is not None:
            if ln.speaker in cast:
                w.turns[ln.speaker] += 1
                scene.append(ln.speaker)
                if prev_spk in cast and prev_spk != ln.speaker:
                    w.edges[frozenset((prev_spk, ln.speaker))] += 1
            prev_spk = ln.speaker
        prev_end = ln.end
    close_scene()
    return w


# ----------------------------------------------------------------------------- agenda record (past councils)

@dataclass
class Agenda:
    first: collections.Counter = field(default_factory=collections.Counter)   # councils where they named the boot first
    vt_total: collections.Counter = field(default_factory=collections.Counter)
    vt_hit: collections.Counter = field(default_factory=collections.Counter)
    n: int = 0

    def power(self, s: str) -> float:
        return (self.first[s] + 0.25) / (self.n + 1)

    def hit(self, s: str) -> float:
        return (self.vt_hit[s] + 1) / (self.vt_total[s] + 4)

    def record(self, lines: list[Line], cast: set[str], boot: str | None) -> None:
        """A council is over: credit its window's vote talk against who actually went."""
        if not boot:
            return
        self.n += 1
        firsted = False
        for ln in lines:
            named = is_vote_talk(ln, cast | {boot})
            if not named:
                continue
            self.vt_total[ln.speaker] += 1
            if boot in named:
                self.vt_hit[ln.speaker] += 1
                if not firsted:
                    self.first[ln.speaker] += 1
                    firsted = True


# ----------------------------------------------------------------------------- features

FEATURES = [
    # family 8
    "sem_camp_n", "sem_tribe_sim", "sem_tribe_dev", "sem_tribe_rank", "sem_cast_sim", "sem_best_partner",
    "sem_conf_sim", "sem_tribe_sim_delta",
    # family 9
    "ag_first_n", "ag_first_rate", "ag_hit_share", "ag_vt_n", "ag_named_vt", "ag_power_named", "ag_hit_named",
    "ag_first_named",
    # family 10
    "cv_turns", "cv_exch_share", "cv_partners", "cv_tribe_partner_share", "cv_scene_share", "cv_pagerank", "cv_isolated",
    # coverage
    "attr_share", "attr_source",
]


def _unit(v: np.ndarray) -> np.ndarray:
    return v / (np.linalg.norm(v) + 1e-12)


def semantic(bucket: dict, cast: list[str], tribe: dict[str, str]) -> dict[str, dict]:
    means = {c: _unit(v / n) for c, (v, n) in bucket.items() if c in cast and n >= MIN_LINES}
    out = {c: {} for c in cast}
    sims = {}
    for c in cast:
        if c not in means:
            continue
        mates = [m for m in means if m != c and tribe.get(m) is not None and tribe.get(m) == tribe.get(c)]
        others = [m for m in means if m != c]
        if mates:
            sims[c] = float(means[c] @ _unit(np.mean([means[m] for m in mates], axis=0)))
            out[c]["best"] = float(max(means[c] @ means[m] for m in mates))
        if others:
            out[c]["cast"] = float(means[c] @ _unit(np.mean([means[m] for m in others], axis=0)))
    by_tribe = collections.defaultdict(list)
    for c, s in sims.items():
        by_tribe[tribe.get(c)].append((s, c))
    for grp in by_tribe.values():
        grp.sort()
        mu = float(np.mean([s for s, _ in grp]))
        for r, (s, c) in enumerate(grp):
            out[c]["tribe"] = s
            out[c]["dev"] = s - mu
            out[c]["rank"] = r / (len(grp) - 1) if len(grp) > 1 else None
    return out


def derive(w: Win, ag: Agenda, cast: list[str], tribe: dict[str, str], source: str,
           prior_sem: dict | None = None) -> dict[str, dict]:
    div = lambda a, b: (a / b) if b else None  # noqa: E731
    sem = semantic(w.camp, cast, tribe)
    sem_c = semantic(w.conf, cast, {c: "all" for c in cast})
    tot_ex = sum(w.edges.values())
    und = collections.Counter()
    for e, n in w.edges.items():
        a, b = tuple(e)
        und[(a, b)] += n
        und[(b, a)] += n
    pr = nf.pagerank(sorted(cast), und) if und else {}
    out = {}
    for c in cast:
        s = sem[c]
        ex = sum(n for e, n in w.edges.items() if c in e)
        partners = {x for e in w.edges for x in e if c in e and x != c}
        mates = [m for m in cast if m != c and tribe.get(m) is not None and tribe.get(m) == tribe.get(c)]
        named_by = w.vt_named.get(c, collections.Counter())
        row = {
            "sem_camp_n": w.camp[c][1] if c in w.camp else 0,
            "sem_tribe_sim": s.get("tribe"), "sem_tribe_dev": s.get("dev"), "sem_tribe_rank": s.get("rank"),
            "sem_cast_sim": s.get("cast"), "sem_best_partner": s.get("best"), "sem_conf_sim": sem_c[c].get("cast"),
            "sem_tribe_sim_delta": (s["tribe"] - prior_sem[c]["tribe"]) if prior_sem is not None and "tribe" in s
            and "tribe" in prior_sem.get(c, {}) else None,
            "ag_first_n": ag.first[c], "ag_first_rate": div(ag.first[c], ag.n),
            "ag_hit_share": div(ag.vt_hit[c], ag.vt_total[c]) if ag.vt_total[c] >= MIN_VOTE_TALK else None,
            "ag_vt_n": w.vt_by[c], "ag_named_vt": sum(named_by.values()),
            "ag_power_named": sum(ag.power(sp) * n for sp, n in named_by.items()),
            "ag_hit_named": sum(ag.hit(sp) * n for sp, n in named_by.items()),
            "ag_first_named": (int(w.first_named == c) if w.first_named is not None else None),
            "cv_turns": w.turns[c], "cv_exch_share": div(ex, tot_ex), "cv_partners": len(partners),
            "cv_tribe_partner_share": div(len(partners & set(mates)), len(mates)),
            "cv_scene_share": div(w.scenes[c], w.n_scenes), "cv_pagerank": pr.get(c) if pr else None,
            "cv_isolated": int(w.turns[c] >= MIN_LINES and ex == 0),
            "attr_share": div(w.attr, w.lines), "attr_source": source,
        }
        out[c] = row
    return out


# ----------------------------------------------------------------------------- main

def main(args: list[str], embed=None) -> int:
    t0 = time.time()
    subs = nf.connect_ro(Path(args[args.index("--subs") + 1]) if "--subs" in args else nf.SUBS, immutable=True)
    gb = nf.connect_ro(Path(args[args.index("--gamebot") + 1]) if "--gamebot" in args else nf.GAMEBOT)
    out_path = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    only = set(args[args.index("--seasons") + 1].split(",")) if "--seasons" in args else None
    embed = embed or Embedder()
    audio = nf.load_audio_labels(args)
    alias = sm.aliases()
    seasons = [r[0] for r in gb.execute("SELECT version_season FROM season_summary WHERE version = 'US' ORDER BY season")]
    rows_out: list[dict] = []
    n_ep = 0
    for vs in seasons:
        if only and vs not in only:
            continue
        pats = sm._patterns(gb, alias, vs)
        eps = [r[0] for r in subs.execute("SELECT DISTINCT episode FROM cues WHERE version_season=? ORDER BY episode", (vs,))]
        prior, ag, srcs = Win(), Agenda(), set()
        for ep in eps:
            bm = gb.execute("SELECT castaway_id, tribe FROM boot_mapping WHERE version_season=? AND episode=? "
                            "ORDER BY boot_mapping_order", (vs, ep)).fetchall()
            if not bm:
                continue
            cast = sorted({r["castaway_id"] for r in bm})
            tribe = {r["castaway_id"]: r["tribe"] for r in bm}
            rec = subs.execute("SELECT recap_end_s FROM recaps WHERE version_season=? AND episode=?", (vs, ep)).fetchone()
            recap_end = float(rec[0]) if rec and rec[0] is not None else 0.0
            cues = subs.execute("""SELECT start_s, end_s, text, sdh_name, speaker_id, italic, turn, sound FROM cues
                                   WHERE version_season=? AND episode=? AND start_s >= ? ORDER BY idx""",
                                (vs, ep, recap_end)).fetchall()
            cues = [r for r in cues if r["text"] and not r["sound"]]
            lines = [Line(r["start_s"], r["end_s"], r["text"], int(r["italic"] or 0)) for r in cues]
            source = nf.attribute(lines, cues, audio.get((vs, ep)))
            for ln in lines:
                ln.named = frozenset(c for c, p in pats.items() if c in tribe and sm._hit(p, ln.text))
                if ln.domain is None and ln.italic:
                    ln.domain = "confessional"
            vecs = embed(f"{vs}_E{ep:02d}", [ln.text for ln in lines]) if lines else None

            prior_rows = derive(prior, ag, cast, tribe, "+".join(sorted(srcs)) or "none")
            prior_sem = semantic(prior.camp, cast, tribe)
            for c in cast:
                rows_out.append({"window": "prior", "version_season": vs, "episode": ep, "k": None, "castaway_id": c,
                                 **prior_rows[c]})
            councils = subs.execute("SELECT k, castaway_id, cut_s, tally_s FROM tribals WHERE version_season=? AND episode=?",
                                    (vs, ep)).fetchall()
            boots = {r["k"]: r["castaway_id"] for r in councils}
            for k, a, b in nf.windows(recap_end, councils):
                idx = [i for i, ln in enumerate(lines) if a <= ln.start < b]
                wl = [lines[i] for i in idx]
                w = window_stats(wl, vecs[idx] if vecs is not None and idx else None, set(cast))
                rows = derive(w, ag, cast, tribe, source, prior_sem)
                for c in cast:
                    rows_out.append({"window": "tonight", "version_season": vs, "episode": ep, "k": k, "castaway_id": c,
                                     **rows[c]})
                ag.record(wl, set(cast), boots.get(k))      # the vote is in: it joins the record for later councils
            prior.add(window_stats(lines, vecs, set(cast)))
            srcs.add(source)
            n_ep += 1
        print(vs, end=" ", flush=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["window", "version_season", "episode", "k", "castaway_id"] + FEATURES
    with open(out_path, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=cols)
        wr.writeheader()
        for r in rows_out:
            wr.writerow({c: (round(r[c], 5) if isinstance(r[c], float) and math.isfinite(r[c]) else r[c]) for c in cols})
    print(f"\n{n_ep} episodes, {len(rows_out)} rows in {time.time() - t0:.0f} s -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
