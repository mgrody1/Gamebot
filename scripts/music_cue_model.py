"""Learned music cues, bootstrapped from the scenes labelled in the review app's "music cues" mode.

The zero-shot CLAP descriptions (music_cues.py) mistake strategy music for goofy music and cannot say "strategy" at
all. This fits a small classifier on the labelled scenes instead: a scene's vector is the mean of its lines' CLAP audio
embeddings (data_cache/nlp/clap_emb/, written by music_cues.py), unit-normed; the model is a balanced, regularised
multinomial logistic regression over whatever cues have been labelled (dodo, strategy, ominous, ..., none).

    uv run --with numpy --with scikit-learn python scripts/music_cue_model.py [--min-labels 20]

Prints cross-validated accuracy (leave one episode out) next to the zero-shot model's on the same scenes, then scores
every line that has an embedding and writes data_cache/nlp/music_cues_learned.csv in music_cues.csv's format
(cue_<name> columns). The review app prefers that file once it exists, so the next labels go where this model is
surest of dodo music and where it is least sure. It is written only when it beats zero-shot in that check (--force
writes it anyway); otherwise an older one is set aside so the queue stays on zero-shot. Numbers only.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402

EMB = nf.ROOT / "data_cache/nlp/clap_emb"
ZERO = nf.ROOT / "data_cache/nlp/music_cues.csv"
OUT = nf.ROOT / "data_cache/nlp/music_cues_learned.csv"
ZERO_NAMES = {"goofy": "dodo"}          # the zero-shot model's name for the same cue
C = 1.0


def _unit(x: np.ndarray) -> np.ndarray:
    return x / np.maximum(np.linalg.norm(x, axis=-1, keepdims=True), 1e-9)


def load_emb(vs: str, ep: int, cache: dict) -> dict[str, np.ndarray]:
    if (vs, ep) not in cache:
        p = EMB / f"{vs}_E{ep:02d}.npz"
        cache[(vs, ep)] = {}
        if p.exists():
            z = np.load(p)
            cache[(vs, ep)] = dict(zip(z["utt_ids"].tolist(), z["emb"].astype(np.float32)))
    return cache[(vs, ep)]


def scene_vectors(labels: list[dict], utts: dict, cache: dict) -> tuple[np.ndarray, list[dict]]:
    """One unit vector per labelled scene (mean of its lines' embeddings); scenes without any embedded line drop out."""
    X, kept = [], []
    for lab in labels:
        emb = load_emb(lab["vs"], lab["ep"], cache)
        vs = [emb[u] for u, a, b in utts.get((lab["vs"], lab["ep"]), []) if a >= lab["t0"] - 0.05 and b <= lab["t1"] + 0.05 and u in emb]
        if vs:
            X.append(_unit(np.mean(vs, axis=0)))
            kept.append(lab)
    return (np.stack(X) if X else np.zeros((0, 512), np.float32)), kept


def fit(X: np.ndarray, y: list[str]):
    from sklearn.linear_model import LogisticRegression

    return LogisticRegression(C=C, class_weight="balanced", max_iter=2000).fit(X, y)


def cv_accuracy(X: np.ndarray, y: list[str], groups: list) -> tuple[float, int]:
    """Leave one episode out, or one scene out while every label comes from one episode (every fold must have two
    cues to fit); returns (accuracy, scenes scored)."""
    y = np.array(y)
    g = np.array([f"{a}|{b}" for a, b in groups])
    if len(set(g)) < 2:
        g = np.arange(len(y)).astype(str)
    hit = n = 0
    for k in sorted(set(g)):
        tr, te = g != k, g == k
        if len(set(y[tr])) < 2:
            continue
        m = fit(X[tr], list(y[tr]))
        hit += int((m.predict(X[te]) == y[te]).sum())
        n += int(te.sum())
    return (hit / n if n else float("nan")), n


def zero_shot_accuracy(kept: list[dict], utts: dict) -> tuple[float, int]:
    """The zero-shot CLAP model's scene guess (mean of its line probabilities) against the same labels."""
    if not ZERO.exists():
        return float("nan"), 0
    probs: dict = {}
    with open(ZERO) as f:
        for r in csv.DictReader(f):
            probs[r["utt_id"]] = {ZERO_NAMES.get(k[4:], k[4:]): float(v) for k, v in r.items() if k.startswith("cue_") and v}
    hit = n = 0
    for lab in kept:
        ps = [probs[u] for u, a, b in utts.get((lab["vs"], lab["ep"]), []) if a >= lab["t0"] - 0.05 and b <= lab["t1"] + 0.05 and u in probs]
        if not ps:
            continue
        mean = {k: np.mean([p.get(k, 0.0) for p in ps]) for k in ps[0]}
        hit += max(mean, key=mean.get) == lab["cue"]
        n += 1
    return (hit / n if n else float("nan")), n


def main(args: list[str]) -> int:
    min_labels = int(args[args.index("--min-labels") + 1]) if "--min-labels" in args else 20
    con = nf.connect_ro(nf.SPEAKERS)
    try:
        rows = con.execute("SELECT version_season, episode, t0, t1, cue, subjects FROM music_labels").fetchall()
    except Exception:  # noqa: BLE001 - no table until the first music label
        rows = []
    labels = [{"vs": r[0], "ep": r[1], "t0": r[2], "t1": r[3], "cue": r[4], "subjects": json.loads(r[5] or "[]")}
              for r in rows if r[4] != "other"]
    eps = {(x["vs"], x["ep"]) for x in labels}
    utts: dict = {}
    for vs, ep in eps:
        utts[(vs, ep)] = [tuple(r) for r in con.execute(
            "SELECT utt_id, start_s, end_s FROM utterances WHERE version_season=? AND episode=? AND segment='body'", (vs, ep))]
    cache: dict = {}
    X, kept = scene_vectors(labels, utts, cache)
    y = [x["cue"] for x in kept]
    counts = {c: y.count(c) for c in sorted(set(y))}
    print(f"{len(labels)} labelled scenes, {len(kept)} with embeddings: {counts}")
    if len(kept) < min_labels or len(counts) < 2:
        print(f"need at least {min_labels} embedded scenes and two cues; label more (or --min-labels N)")
        return 1
    acc, n = cv_accuracy(X, y, [(x["vs"], x["ep"]) for x in kept])
    zacc, zn = zero_shot_accuracy(kept, utts)
    top = max(counts.values()) / len(y)
    print(f"cross-validated accuracy {acc:.1%} on {n} scenes; zero-shot {zacc:.1%} on {zn}; always-the-commonest {top:.1%}")
    if not (acc >= zacc) and "--force" not in args:
        # the review app reads OUT whenever it exists: set a losing model aside so the queue keeps the zero-shot one
        if OUT.exists():
            OUT.replace(OUT.with_suffix(".set_aside.csv"))
        print("the learned model does not beat zero-shot yet: kept zero-shot for the review queue (label more, or --force)")
        return 0
    m = fit(X, y)
    classes = list(m.classes_)
    cols = ["version_season", "episode", "utt_id"] + [f"cue_{c}" for c in classes]
    n_out = 0
    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for p in sorted(EMB.glob("*.npz")):
            vs, ep = p.stem.split("_E")
            e = load_emb(vs, int(ep), {})
            if not e:
                continue
            ids = list(e)
            P = m.predict_proba(_unit(np.stack([e[u] for u in ids])))
            for u, pr in zip(ids, P):
                w.writerow([vs, int(ep), u] + [round(float(v), 4) for v in pr])
            n_out += len(ids)
    print(f"{n_out} lines scored with cues {classes} -> {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
