"""The music bed under every line, for family 21 (the "Gamebot NLP features plan" doc): what the editors played under
a player's confessional. The separation step keeps only the vocals, so the bed is the broadcast mix minus the vocals
stem (both 16 kHz mono, sample-aligned). Per line with a trusted speaker label (the rows voice_features.py uses):

  mus_db      level of the bed (dBFS) and mvr = bed minus voice (dB): how loud the music is against the speaker
  low_share   share of the bed's energy under 250 Hz (drones, low strings)
  centroid    spectral centroid of the bed (Hz): dark vs bright
  minor       best minor-key minus best major-key correlation of the bed's chroma with the Krumhansl-Kessler key
              profiles; tonal = the best correlation of either (how clearly it is in a key at all). Only where the
              bed is audible (mus_db > -50)

Writes numbers only: data_cache/nlp/music_utts.csv (appends new episodes; --force redoes them).

    uv run --with librosa --with soundfile python scripts/music_features.py [--seasons US45,US46] [--force]
"""

from __future__ import annotations

import csv
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402
from voice_features import SR  # noqa: E402

OUT = nf.ROOT / "data_cache/nlp/music_utts.csv"
A = nf.STARGAZER / "survivor_audio"
CTX_S = 1.0            # seconds of context either side of the line
AUDIBLE_DB = -50.0
COLS = ["version_season", "episode", "utt_id", "speaker_id", "domain", "sub_start", "sub_end", "mus_db", "voc_db",
        "mvr", "low_share", "centroid", "minor", "tonal"]
MAJOR = np.array([6.35, 2.23, 3.48, 2.33, 4.38, 4.09, 2.52, 5.19, 2.39, 3.66, 2.29, 2.88])
MINOR = np.array([6.33, 2.68, 3.52, 5.38, 2.60, 3.53, 2.54, 4.75, 3.98, 2.69, 3.34, 3.17])


def key_scores(chroma: np.ndarray) -> tuple[float, float]:
    """(best minor corr - best major corr, best corr) of a 12-bin chroma vector against the 24 key profiles."""
    c = chroma - chroma.mean()
    if not np.any(c):
        return 0.0, 0.0
    best = {}
    for name, prof in (("maj", MAJOR), ("min", MINOR)):
        rs = []
        for k in range(12):
            p = np.roll(prof, k) - prof.mean()
            rs.append(float(c @ p / (np.linalg.norm(c) * np.linalg.norm(p) + 1e-12)))
        best[name] = max(rs)
    return best["min"] - best["maj"], max(best.values())


def db(x: np.ndarray) -> float:
    return float(20 * np.log10(np.sqrt(np.mean(x ** 2)) + 1e-8))


def line_music(bed: np.ndarray, voc: np.ndarray) -> dict:
    import librosa

    out = {"mus_db": round(db(bed), 2), "voc_db": round(db(voc), 2)}
    out["mvr"] = round(out["mus_db"] - out["voc_db"], 2)
    out.update(low_share=None, centroid=None, minor=None, tonal=None)
    if len(bed) < SR or out["mus_db"] < AUDIBLE_DB:
        return out
    S = np.abs(librosa.stft(bed, n_fft=4096, hop_length=1024)) ** 2
    freqs = librosa.fft_frequencies(sr=SR, n_fft=4096)
    tot = S.sum()
    if tot <= 0:
        return out
    out["low_share"] = round(float(S[freqs < 250].sum() / tot), 4)
    out["centroid"] = round(float((freqs[:, None] * S).sum() / tot), 1)
    chroma = librosa.feature.chroma_stft(S=S, sr=SR, n_fft=4096, hop_length=1024).mean(1)
    m, t = key_scores(chroma)
    out["minor"], out["tonal"] = round(m, 4), round(t, 4)
    return out


def main(args: list[str]) -> int:
    import soundfile as sf

    t0 = time.time()
    only = set(args[args.index("--seasons") + 1].split(",")) if "--seasons" in args else None
    force = "--force" in args
    out_path = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    old, done = [], set()
    if out_path.exists() and not force:
        with open(out_path) as f:
            old = list(csv.DictReader(f))
        done = {(r["version_season"], int(r["episode"])) for r in old}
    con = nf.connect_ro(nf.SPEAKERS)
    rows = con.execute(
        """SELECT u.utt_id, u.version_season, u.episode, u.start_s, u.end_s, u.domain_hint, l.speaker_id, l.source,
                  COALESCE(l.confidence, 0) AS conf, MIN(c.start_s) AS sub_start, MAX(c.end_s) AS sub_end
           FROM labels l JOIN utterances u USING (utt_id) JOIN utterance_cues uc USING (utt_id) JOIN cues c ON c.cue_id = uc.cue_id
           WHERE u.segment = 'body' AND l.speaker_id NOT IN ('UNKNOWN', 'NOSPEECH', 'OTHER')
           GROUP BY u.utt_id ORDER BY u.version_season, u.episode, u.start_s""").fetchall()
    by_ep: dict = {}
    for r in rows:
        if r["source"] in ("human", "sdh", "chyron") or (r["source"] == "auto" and r["conf"] >= nf.AUTO_MIN_CONF):
            by_ep.setdefault((r["version_season"], r["episode"]), []).append(r)
    new_rows = []
    for (vs, ep), utts in sorted(by_ep.items()):
        if (only and vs not in only) or (vs, ep) in done:
            continue
        pr, pv = A / "raw" / vs / f"E{ep:02d}.flac", A / "vocals" / vs / f"E{ep:02d}.flac"
        if not (pr.exists() and pv.exists()):
            continue
        raw, s1 = sf.read(str(pr), dtype="float32", always_2d=False)
        voc, s2 = sf.read(str(pv), dtype="float32", always_2d=False)
        assert s1 == s2 == SR
        n = min(len(raw), len(voc))
        bed = raw[:n] - voc[:n]
        for r in utts:
            a, b = int(max(0, r["start_s"] - CTX_S) * SR), int(min(n / SR, r["end_s"] + CTX_S) * SR)
            va, vb = int(r["start_s"] * SR), int(r["end_s"] * SR)
            m = line_music(bed[a:b], voc[va:vb])
            new_rows.append({"version_season": vs, "episode": ep, "utt_id": r["utt_id"],
                             "speaker_id": "HOST" if r["speaker_id"].startswith("HOST") else r["speaker_id"],
                             "domain": r["domain_hint"], "sub_start": r["sub_start"], "sub_end": r["sub_end"], **m})
        print(f"{vs} E{ep:02d}: {len(utts)} lines [{time.time() - t0:.0f} s]", flush=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        for r in old + new_rows:
            w.writerow({c: r.get(c) for c in COLS})
    print(f"{len(new_rows)} new lines, {len(old) + len(new_rows)} in all -> {out_path} [{time.time() - t0:.0f} s]")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
