"""Family 25 (scripts/vote_intent_llm.py): stated positions become a tally; names resolve to castaway ids; the last
stated vote wins; unknown names and self-votes are dropped. No model is called here."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import vote_intent_llm as vi  # noqa: E402
from nlp_features import Line  # noqa: E402

CAST = ["X1", "X2", "X3", "X4"]
BY_NAME = {"alice": "X1", "bob": "X2", "cara": "X3", "dan": "X4"}


def test_tally_from_stated_positions():
    intents = [
        {"speaker": "Alice", "target": "Bob", "kind": "will_vote", "final": False},
        {"speaker": "Alice", "target": "Cara", "kind": "will_vote", "final": True},    # changed her mind
        {"speaker": "Dan", "target": "Cara", "kind": "will_vote", "final": True},
        {"speaker": "Bob", "target": "Bob", "kind": "will_vote", "final": True},       # a self-vote: dropped
        {"speaker": "Cara", "target": "Cara", "kind": "believes_target", "final": True},
        {"speaker": "Bob", "target": "Cara", "kind": "believes_target", "final": True},
        {"speaker": "Bob", "target": "Dan", "kind": "considers", "final": True},
        {"speaker": "Zed", "target": "Dan", "kind": "will_vote", "final": True},       # not in the cast
    ]
    f = vi.features(intents, CAST, BY_NAME, {})
    assert f["X3"]["vi_will_n"] == 2 and f["X3"]["vi_top"] == 1 and f["X3"]["vi_will_share"] == 1.0
    assert f["X2"]["vi_will_n"] == 0 and f["X3"]["vi_n_stances"] == 2
    assert f["X3"]["vi_self_fear"] == 1 and f["X3"]["vi_believed_n"] == 1          # Bob; her own fear counts apart
    assert f["X4"]["vi_consider_n"] == 1 and f["X1"]["vi_top"] == 0


def test_transcript_names_speakers_and_places():
    lines = [Line(0, 1, "vote Bob", 1, speaker="X1", domain="confessional"), Line(2, 3, "hi", 0, speaker=None),
             Line(4, 5, "come on in", 0, speaker="HOST")]
    t = vi.transcript(lines, {"X1": "Alice"})
    assert t.splitlines() == ["Alice [confessional]: vote Bob", "? [camp]: hi", "JEFF [camp]: come on in"]
