"""music_cue_model.py: scene vectors from line embeddings, and a leave-one-episode-out check that can learn."""

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
pytest.importorskip("sklearn")
import music_cue_model as mm  # noqa: E402


def test_scene_vectors_average_the_lines_inside(tmp_path, monkeypatch):
    monkeypatch.setattr(mm, "EMB", tmp_path)
    e = np.eye(4, dtype=np.float32)
    np.savez_compressed(tmp_path / "US46_E04.npz", utt_ids=np.array(["a", "b", "c"]), emb=e[:3].astype(np.float16))
    utts = {("US46", 4): [("a", 10.0, 12.0), ("b", 13.0, 15.0), ("c", 40.0, 42.0)]}
    labels = [{"vs": "US46", "ep": 4, "t0": 10.0, "t1": 15.0, "cue": "dodo"},
              {"vs": "US46", "ep": 4, "t0": 60.0, "t1": 70.0, "cue": "none"}]     # no lines: dropped
    X, kept = mm.scene_vectors(labels, utts, {})
    assert len(kept) == 1 and kept[0]["cue"] == "dodo"
    assert np.allclose(X[0], np.array([1, 1, 0, 0]) / np.sqrt(2))


def test_cv_learns_separable_cues():
    rng = np.random.default_rng(0)
    centers = {"dodo": rng.normal(size=16), "strategy": rng.normal(size=16)}
    X, y, g = [], [], []
    for ep in range(4):
        for cue, c in centers.items():
            for _ in range(5):
                X.append(mm._unit(c + 0.3 * rng.normal(size=16)))
                y.append(cue)
                g.append(("US46", ep))
    acc, n = mm.cv_accuracy(np.stack(X), y, g)
    assert n == 40 and acc > 0.9
