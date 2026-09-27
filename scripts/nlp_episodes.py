"""The episode loop the NLP family scripts share: the same rows, windows, leakage rule and speaker attribution as
nlp_features.py (tonight = recap end to the council's cut; prior = episodes before this one; audio labels, else
caption names), handed to a family script one episode at a time.

    for ep in episodes(args):           # in season order, episodes in order
        ep.vs, ep.ep, ep.cast, ep.tribe, ep.lines, ep.source, ep.windows, ep.boots, ep.first_of_season

A family script keeps its own prior-window sums and adds each episode to them after it has written that episode's
rows, so nothing from episode e (or a later one) reaches a row of episode e.
"""

from __future__ import annotations

import csv
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402
from nlp_features import Line, sm  # noqa: E402


@dataclass
class Episode:
    vs: str
    ep: int
    cast: list[str]
    tribe: dict[str, str]
    lines: list[Line]
    source: str                       # audio | captions | none
    windows: list[tuple[int, float, float]]   # (k, start, cut) per council with a cut
    boots: dict[int, str]             # council k -> who went
    tallies: dict[int, float | None]  # council k -> when the votes were read
    recap_end: float
    first_of_season: bool

    def window(self, a: float, b: float) -> list[Line]:
        return [ln for ln in self.lines if a <= ln.start < b]


def episodes(args: list[str], subs_path: Path | None = None, gb_path: Path | None = None) -> Iterator[Episode]:
    subs = nf.connect_ro(Path(args[args.index("--subs") + 1]) if "--subs" in args else (subs_path or nf.SUBS), immutable=True)
    gb = nf.connect_ro(Path(args[args.index("--gamebot") + 1]) if "--gamebot" in args else (gb_path or nf.GAMEBOT))
    only = set(args[args.index("--seasons") + 1].split(",")) if "--seasons" in args else None
    audio = nf.load_audio_labels(args)
    alias = sm.aliases()
    seasons = [r[0] for r in gb.execute("SELECT version_season FROM season_summary WHERE version = 'US' ORDER BY season")]
    for vs in seasons:
        if only and vs not in only:
            continue
        pats = sm._patterns(gb, alias, vs)
        eps = [r[0] for r in subs.execute("SELECT DISTINCT episode FROM cues WHERE version_season=? ORDER BY episode", (vs,))]
        first = True
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
            councils = subs.execute("SELECT k, castaway_id, cut_s, tally_s FROM tribals WHERE version_season=? AND episode=?",
                                    (vs, ep)).fetchall()
            yield Episode(vs, ep, cast, tribe, lines, source, nf.windows(recap_end, councils),
                          {r["k"]: r["castaway_id"] for r in councils}, {r["k"]: r["tally_s"] for r in councils},
                          recap_end, first)
            first = False
        print(vs, end=" ", flush=True)


def write_rows(out_path: Path, features: list[str], rows: list[dict]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["window", "version_season", "episode", "k", "castaway_id"] + features
    with open(out_path, "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        wr.writeheader()
        for r in rows:
            wr.writerow({c: (round(r[c], 5) if isinstance(r.get(c), float) and math.isfinite(r[c])
                             else (None if isinstance(r.get(c), float) else r.get(c))) for c in cols})


def div(a, b):
    return (a / b) if b else None
