"""Family 25 of the NLP plan (the "Gamebot NLP features plan" doc, batch 6b): the castaways' own forecast. A local
model (oMLX on this Mac, OpenAI-compatible API; the dialogue never leaves the machine) reads each council's `tonight`
window (the episode from the recap to the cut) with speaker names, and lists every castaway's stated plan: who they
say they will vote for, who they think is going, who they are considering. Those stances become a predicted tally.

Answers are cached per window under data_cache/nlp/llm/ (castaway names and stance kinds only, no dialogue).
Features per castaway and council (tonight only):

  vi_will_n        castaways whose final stated vote is for them
  vi_will_share    their share of all final stated votes
  vi_top           1 if they lead the stated tally (ties share it)
  vi_believed_n    castaways who say they are the one going
  vi_consider_n    castaways who name them as an option
  vi_self_fear     1 if they say they themselves are the target
  vi_n_stances     stated votes in the window (coverage)

    uv run python scripts/vote_intent_llm.py --seasons US45,US46 [--model Qwen3.6-35B-A3B-MLX-8bit] [--url http://127.0.0.1:8001]
"""

from __future__ import annotations

import collections
import hashlib
import json
import re
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nlp_features as nf  # noqa: E402
from nlp_episodes import div, episodes, write_rows  # noqa: E402
from nlp_features import HOST, sm  # noqa: E402

OUT = nf.ROOT / "data_cache/nlp/nlp_vote_intent.csv"
CACHE = nf.ROOT / "data_cache/nlp/llm"
MODEL = "Qwen3.6-35B-A3B-MLX-8bit"
URL = "http://127.0.0.1:8001"
MAX_CHARS = 90000
KINDS = ("will_vote", "believes_target", "considers")

SYSTEM = """You read the transcript of one Survivor episode, from the start up to the moment the castaways vote at \
Tribal Council. Each line is `NAME [confessional|camp]: words`; `?` means the speaker is not known and `JEFF` is the \
host. Your job: list each castaway's own stated position on TONIGHT's vote. Reply with JSON only."""

INSTRUCTIONS = """Cast still in the game (name — tribe): {cast}

Return {{"intents": [{{"speaker": NAME, "target": NAME, "kind": KIND, "final": true|false}}, ...]}} where:
- speaker and target are names from the cast list exactly as written (a castaway can be their own target).
- kind "will_vote": the speaker says they (or a group they are part of) will vote for, want out, or are targeting \
the target tonight.
- kind "believes_target": the speaker says they think the target is the one going home or the one being targeted \
tonight (use the speaker's own name when they fear it is them).
- kind "considers": the speaker names the target as an option they are weighing.
- final: true for the speaker's last stated position of that kind in the episode (they may change their mind).
Only use what the lines say. Skip unknown speakers (`?`) and Jeff. Skip plans about other nights. If nobody states a \
position, return {{"intents": []}}.

Transcript:
{transcript}"""


def fmt(t: float) -> str:
    return f"{int(t // 60):02d}:{int(t % 60):02d}"


def transcript(lines, names: dict) -> str:
    out = []
    for ln in lines:
        who = "JEFF" if ln.speaker == HOST else names.get(ln.speaker, "?")
        where = "confessional" if ln.domain == "confessional" else "camp"
        out.append(f"{who} [{where}]: {ln.text}")
    s = "\n".join(out)
    return s if len(s) <= MAX_CHARS else s[-MAX_CHARS:]


def ask(prompt: str, model: str, url: str) -> dict:
    body = {"model": model, "temperature": 0, "max_tokens": 1500, "chat_template_kwargs": {"enable_thinking": False},
            "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": prompt}]}
    req = urllib.request.Request(url + "/v1/chat/completions", json.dumps(body).encode(), {"Content-Type": "application/json"})
    r = json.load(urllib.request.urlopen(req, timeout=900))
    txt = r["choices"][0]["message"]["content"]
    m = re.search(r"\{.*\}", txt, re.S)
    try:
        return json.loads(m.group(0)) if m else {"intents": []}
    except ValueError:
        return {"intents": [], "error": "bad json"}


def resolve(name: str, by_name: dict, pats: dict, cast: set) -> str | None:
    n = (name or "").strip().lower()
    if n in by_name:
        return by_name[n]
    hits = [c for c, p in pats.items() if c in cast and sm._hit(p, name or "")]
    return hits[0] if len(hits) == 1 else None


def features(intents: list[dict], cast: list[str], by_name: dict, pats: dict) -> dict[str, dict]:
    cs = set(cast)
    final_vote: dict[str, str] = {}
    believed, consider, self_fear = collections.defaultdict(set), collections.defaultdict(set), set()
    for it in intents:
        s = resolve(it.get("speaker"), by_name, pats, cs)
        t = resolve(it.get("target"), by_name, pats, cs)
        k = it.get("kind")
        if s is None or t is None or k not in KINDS:
            continue
        if k == "will_vote" and s != t:
            if it.get("final", True) or s not in final_vote:
                final_vote[s] = t
        elif k == "believes_target":
            believed[t].add(s)
            if s == t:
                self_fear.add(s)
        elif k == "considers" and s != t:
            consider[t].add(s)
    tally = collections.Counter(final_vote.values())
    top = max(tally.values()) if tally else 0
    n = sum(tally.values())
    return {c: {"vi_will_n": tally[c], "vi_will_share": div(tally[c], n), "vi_top": int(top > 0 and tally[c] == top),
                "vi_believed_n": len(believed[c] - {c}), "vi_consider_n": len(consider[c]), "vi_self_fear": int(c in self_fear),
                "vi_n_stances": n} for c in cast}


FEATURES = ["vi_will_n", "vi_will_share", "vi_top", "vi_believed_n", "vi_consider_n", "vi_self_fear", "vi_n_stances",
            "attr_source"]


def main(args: list[str]) -> int:
    t0 = time.time()
    model = args[args.index("--model") + 1] if "--model" in args else MODEL
    url = args[args.index("--url") + 1] if "--url" in args else URL
    out_path = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    gb = nf.connect_ro(Path(args[args.index("--gamebot") + 1]) if "--gamebot" in args else nf.GAMEBOT)
    alias = sm.aliases()
    CACHE.mkdir(parents=True, exist_ok=True)
    rows_out, n_asked = [], 0
    names_by_vs, pats_by_vs = {}, {}
    for ep in episodes(args):
        if ep.vs not in names_by_vs:
            names_by_vs[ep.vs] = {r[0]: r[1] for r in gb.execute(
                "SELECT castaway_id, castaway FROM castaways WHERE version_season=? GROUP BY castaway_id", (ep.vs,))}
            pats_by_vs[ep.vs] = sm._patterns(gb, alias, ep.vs)
        names, pats = names_by_vs[ep.vs], pats_by_vs[ep.vs]
        by_name = {names[c].lower(): c for c in ep.cast if c in names}
        cast_s = ", ".join(f"{names.get(c, c)} — {ep.tribe.get(c) or '?'}" for c in ep.cast)
        for k, a, b in ep.windows:
            wl = ep.window(a, b)
            prompt = INSTRUCTIONS.format(cast=cast_s, transcript=transcript(wl, names))
            key = hashlib.sha1((model + prompt).encode()).hexdigest()[:12]
            cache = CACHE / f"{ep.vs}_E{ep.ep:02d}_k{k}_{key}.json"
            if cache.exists():
                ans = json.loads(cache.read_text())
            else:
                ans = ask(prompt, model, url)
                cache.write_text(json.dumps(ans))
                n_asked += 1
            feats = features(ans.get("intents") or [], ep.cast, by_name, pats)
            for c in ep.cast:
                rows_out.append({"window": "tonight", "version_season": ep.vs, "episode": ep.ep, "k": k, "castaway_id": c,
                                 **feats[c], "attr_source": ep.source})
            print(f"  {ep.vs} E{ep.ep:02d} k{k}: {sum(f['vi_will_n'] for f in feats.values())} stated votes [{time.time() - t0:.0f} s]", flush=True)
    write_rows(out_path, FEATURES, rows_out)
    print(f"\n{len(rows_out)} rows, {n_asked} windows asked in {time.time() - t0:.0f} s -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
