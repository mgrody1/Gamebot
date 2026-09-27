"""Music cue types under every line, for family 28 (the "Gamebot NLP features plan" doc): the kind of music the
editors chose, including the "dodo music" fans point out (goofy, cartoonish cues under someone who is getting it
wrong). The bed is the broadcast mix minus the vocals stem, as in music_features.py; each line's bed (1 s of context
either side, at most 10 s) is scored against a set of text descriptions with LAION's CLAP (music and speech) model, zero-shot, on
this Mac. Per line: the probability of each cue (softmax over the descriptions).

Writes numbers only: data_cache/nlp/music_cues.csv (appends new episodes; --force redoes them), and each line's
CLAP audio embedding in data_cache/nlp/clap_emb/<vs>_E<ep>.npz (utt_ids, emb float16), which music_cue_model.py
learns the labelled cues from. With the embeddings on disk, a rerun (new descriptions) skips the audio.

    uv run --with librosa --with soundfile --with torch --with "transformers==4.46.3" python scripts/music_cues.py [--seasons US46]
"""

from __future__ import annotations

import csv
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402
from music_features import A, CTX_S  # noqa: E402
from voice_features import SR  # noqa: E402

OUT = nf.ROOT / "data_cache/nlp/music_cues.csv"
EMB = nf.ROOT / "data_cache/nlp/clap_emb"
MODEL = "laion/larger_clap_music_and_speech"   # larger_clap_music loads, but its embeddings come out constant
CUES = {
    "goofy": "goofy comedic cartoonish music with plucked strings and bassoon, a silly blunder",
    "ominous": "ominous dark suspenseful music with low drones",
    "tense": "tense dramatic music with pounding percussion",
    "sad": "sad emotional piano and strings",
    "triumphant": "triumphant heroic orchestral music",
    "upbeat": "upbeat happy acoustic music",
    "eerie": "mysterious eerie ambient music",
    "none": "no music, only wind, waves and outdoor ambience",
}
MAX_S = 10.0
BATCH = 32


def _emb(out):
    """get_*_features returns a tensor in older transformers and an output object (pooler_output) in newer ones."""
    if hasattr(out, "norm"):
        return out
    for k in ("text_embeds", "audio_embeds", "pooler_output"):
        v = getattr(out, k, None)
        if v is not None:
            return v
    raise TypeError(f"no embedding in {type(out).__name__}")


class Clap:
    def __init__(self, model: str | None = None):
        import torch
        from transformers import ClapModel, ClapProcessor

        self.torch = torch
        import os
        model = model or os.environ.get("CLAP_MODEL") or MODEL
        self.dev = os.environ.get("CLAP_DEVICE") or ("mps" if torch.backends.mps.is_available() else "cpu")
        self.m = ClapModel.from_pretrained(model).to(self.dev).eval()
        self.p = ClapProcessor.from_pretrained(model)
        with torch.no_grad():
            t = self.p(text=list(CUES.values()), return_tensors="pt", padding=True).to(self.dev)
            e = _emb(self.m.get_text_features(**t))
            self.text = (e / e.norm(dim=-1, keepdim=True)).float()
            self.scale = float(self.m.logit_scale_a.exp().item())

    def embed(self, clips: list[np.ndarray]) -> np.ndarray:
        """Unit-norm audio embeddings, one row per clip."""
        import librosa

        x = [librosa.resample(c, orig_sr=SR, target_sr=48000) for c in clips]
        with self.torch.no_grad():
            a = self.p(audios=x, sampling_rate=48000, return_tensors="pt").to(self.dev)
            e = _emb(self.m.get_audio_features(**a))
            return (e / e.norm(dim=-1, keepdim=True)).float().cpu().numpy()

    def probs(self, emb: np.ndarray) -> np.ndarray:
        logits = self.scale * self.torch.from_numpy(emb.astype(np.float32)).to(self.dev) @ self.text.T
        return self.torch.softmax(logits, dim=-1).cpu().numpy()

    def __call__(self, clips: list[np.ndarray]) -> np.ndarray:
        return self.probs(self.embed(clips))


def main(args: list[str]) -> int:
    import soundfile as sf

    t0 = time.time()
    only = set(args[args.index("--seasons") + 1].split(",")) if "--seasons" in args else None
    force = "--force" in args
    out_path = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    cols = ["version_season", "episode", "utt_id", "speaker_id", "domain", "sub_start", "sub_end"] + [f"cue_{k}" for k in CUES]
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
    clap = None
    n_new = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    EMB.mkdir(parents=True, exist_ok=True)
    # the kept rows first, then each episode appended as it finishes, so a stopped run keeps what it did
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in old:
            w.writerow({c: r.get(c) for c in cols})
    for (vs, ep), utts in sorted(by_ep.items()):
        if (only and vs not in only) or (vs, ep) in done:
            continue
        pe = EMB / f"{vs}_E{ep:02d}.npz"
        ids = [r["utt_id"] for r in utts]
        emb = None
        if pe.exists():
            z = np.load(pe)
            have = dict(zip(z["utt_ids"].tolist(), z["emb"]))
            if all(u in have for u in ids):
                emb = np.stack([have[u] for u in ids]).astype(np.float32)
        if emb is None:
            pr, pv = A / "raw" / vs / f"E{ep:02d}.flac", A / "vocals" / vs / f"E{ep:02d}.flac"
            if not (pr.exists() and pv.exists()):
                continue
            clap = clap or Clap()
            raw, _ = sf.read(str(pr), dtype="float32", always_2d=False)
            voc, _ = sf.read(str(pv), dtype="float32", always_2d=False)
            n = min(len(raw), len(voc))
            bed = raw[:n] - voc[:n]
            clips = []
            for r in utts:
                a = max(0.0, r["start_s"] - CTX_S)
                b = min(n / SR, r["end_s"] + CTX_S, a + MAX_S)
                c = bed[int(a * SR):int(b * SR)]
                clips.append(c if len(c) >= SR else np.pad(c, (0, SR - len(c))))
            emb = np.concatenate([clap.embed(clips[i:i + BATCH]) for i in range(0, len(clips), BATCH)])
            np.savez_compressed(pe, utt_ids=np.array(ids), emb=emb.astype(np.float16))
        clap = clap or Clap()
        probs = clap.probs(emb)
        with open(out_path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            for r, p in zip(utts, probs):
                w.writerow({"version_season": vs, "episode": ep, "utt_id": r["utt_id"],
                            "speaker_id": "HOST" if r["speaker_id"].startswith("HOST") else r["speaker_id"],
                            "domain": r["domain_hint"], "sub_start": r["sub_start"], "sub_end": r["sub_end"],
                            **{f"cue_{k}": round(float(v), 4) for k, v in zip(CUES, p)}})
        n_new += len(utts)
        print(f"{vs} E{ep:02d}: {len(utts)} lines [{time.time() - t0:.0f} s]", flush=True)
    print(f"{n_new} new lines, {len(old) + n_new} in all -> {out_path} [{time.time() - t0:.0f} s]")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
