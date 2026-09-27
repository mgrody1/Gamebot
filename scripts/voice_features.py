"""Per-line voice numbers for family 17 (the "Gamebot NLP features plan" doc): median pitch, pitch range, loudness and
speaking rate of every line with a trusted speaker label in survivor-speakers (human, caption name, name card, or
auto at confidence >= 0.6), from the separated vocals track.

Pitch is librosa's YIN (70-400 Hz, 64 ms frames, 10 ms hop) on frames within 6 dB of the line's loudest decile, which
keeps breaths and music out; range = semitones between the 10th and 90th percentile. Rate = words per second of
speech. Times are written on the subtitle clock (the span of the line's cues), the clock the NLP windows use.

Writes numbers only: data_cache/nlp/voice_utts.csv (appends new episodes; --force redoes them).

    uv run --with librosa --with soundfile python scripts/voice_features.py [--seasons US45,US46] [--force]
"""

from __future__ import annotations

import csv
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402

OUT = nf.ROOT / "data_cache/nlp/voice_utts.csv"
AUDIO = nf.STARGAZER / "survivor_audio/vocals"
SR = 16000
COLS = ["version_season", "episode", "utt_id", "speaker_id", "domain", "sub_start", "sub_end", "dur",
        "f0_med", "f0_range_st", "rms_db", "rate_wps"]


def line_voice(y: np.ndarray, n_words: int) -> dict:
    import librosa

    dur = len(y) / SR
    out = {"dur": round(dur, 3), "f0_med": None, "f0_range_st": None, "rms_db": None,
           "rate_wps": round(n_words / dur, 3) if dur > 0.5 and n_words else None}
    if dur < 0.8:
        return out
    rms = librosa.feature.rms(y=y, frame_length=1024, hop_length=160, center=True)[0]
    db = 20 * np.log10(rms + 1e-8)
    f0 = librosa.yin(y, fmin=70, fmax=400, sr=SR, frame_length=1024, hop_length=160, center=True)
    n = min(len(f0), len(db))
    f0, db = f0[:n], db[:n]
    loud = db >= np.percentile(db, 90) - 6.0
    ok = loud & (f0 > 72) & (f0 < 395)
    out["rms_db"] = round(float(db[loud].mean()), 2) if loud.any() else None
    if ok.sum() >= 20:
        p10, p50, p90 = np.percentile(f0[ok], [10, 50, 90])
        out["f0_med"], out["f0_range_st"] = round(float(p50), 2), round(float(12 * np.log2(p90 / p10)), 3)
    return out


def main(args: list[str]) -> int:
    import soundfile as sf

    t0 = time.time()
    only = set(args[args.index("--seasons") + 1].split(",")) if "--seasons" in args else None
    force = "--force" in args
    out_path = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    done: set = set()
    old: list[dict] = []
    if out_path.exists() and not force:
        with open(out_path) as f:
            old = list(csv.DictReader(f))
        done = {(r["version_season"], int(r["episode"])) for r in old}
    con = nf.connect_ro(nf.SPEAKERS)
    rows = con.execute(
        """SELECT u.utt_id, u.version_season, u.episode, u.start_s, u.end_s, u.text, u.domain_hint, l.speaker_id,
                  l.source, COALESCE(l.confidence, 0) AS conf, MIN(c.start_s) AS sub_start, MAX(c.end_s) AS sub_end
           FROM labels l JOIN utterances u USING (utt_id) JOIN utterance_cues uc USING (utt_id) JOIN cues c ON c.cue_id = uc.cue_id
           WHERE u.segment = 'body' AND l.speaker_id NOT IN ('UNKNOWN', 'NOSPEECH', 'OTHER')
           GROUP BY u.utt_id ORDER BY u.version_season, u.episode, u.start_s""").fetchall()
    by_ep: dict = {}
    for r in rows:
        if r["source"] in ("human", "sdh", "chyron") or (r["source"] == "auto" and r["conf"] >= nf.AUTO_MIN_CONF):
            by_ep.setdefault((r["version_season"], r["episode"]), []).append(r)
    new_rows: list[dict] = []
    for (vs, ep), utts in sorted(by_ep.items()):
        if (only and vs not in only) or ((vs, ep) in done):
            continue
        path = AUDIO / vs / f"E{ep:02d}.flac"
        if not path.exists():
            continue
        y, sr = sf.read(str(path), dtype="float32", always_2d=False)
        if y.ndim > 1:
            y = y.mean(1)
        assert sr == SR, f"{path}: {sr} Hz"
        for r in utts:
            seg = y[int(r["start_s"] * SR):int(r["end_s"] * SR)]
            v = line_voice(seg, len(nf.WORD.findall(r["text"] or "")))
            new_rows.append({"version_season": vs, "episode": ep, "utt_id": r["utt_id"],
                             "speaker_id": "HOST" if r["speaker_id"].startswith("HOST") else r["speaker_id"],
                             "domain": r["domain_hint"], "sub_start": r["sub_start"], "sub_end": r["sub_end"], **v})
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
