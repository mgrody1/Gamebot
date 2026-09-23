#!/usr/bin/env python3
"""
New questions for the daily five, written by a language model and kept only if the records back them.

    uv run --with duckdb python scripts/quiz_llm.py                 # from Gamebot/; asks for 20 and keeps what passes
    uv run --with duckdb python scripts/quiz_llm.py --n 40
    uv run --with duckdb python scripts/quiz_llm.py --recheck       # re-verify the kept questions only, ask for none
    uv run --with duckdb python scripts/quiz_llm.py --data DIR --out DIR   # the refresh passes its staged copies

The model writes the question and the SQL. The SQL decides the answer. For each candidate:

  1. SQL safety   one read-only SELECT over the warehouse tables; no file, network or settings functions.
  2. one answer   the query returns one row, or two columns (answer, value) whose top value is not tied.
  3. second route a check query that reaches the answer another way (another table, or a column such as
                  season_summary.final_vote) must return the same answer. Two queries agreeing is what catches a
                  query that is valid SQL but counts the wrong thing (every juror instead of their votes, say).
  4. distractors  a third query returns wrong answers of the same kind; three are kept that differ from the answer.
  5. explanation  the model's one-line "why" may contain no name or number that the question and results do not.
  6. review       a second model call, told to be strict, checks that the SQL answers the question a fan would read,
                  that only one choice can be right, and that the question does not hinge on how survivoR codes
                  something (what counts as a quit, an idol, a tie). It rejects anything it is unsure of.
  7. new          the question is not already in the bank or the kept set, and is not in quiz_rejects.txt.

Kept questions live in <out>/llm.json. Every run re-verifies all of them against the current data first and drops
any that no longer pass, so a correction in survivoR can retire a question. Each run also appends what it added
to run_logs/quiz_llm.md, for a read-through; a question id listed in Gamebot/quiz_rejects.txt never comes back.
The keys come from ~/.config/preferencespace/steer.env (the word box's provider table). The warehouse is the
site's Parquet export, the same tables the SQL editor on the warehouse page reads, so every kept query runs there.
A network or provider failure leaves llm.json as it was and exits 0.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
import sys
import urllib.request
from datetime import date
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT.parent / "preferencespace"
DATA = SITE / "static/survivor/gamebot/data"
OUT = SITE / "static/survivor/quiz/data"
REJECTS = ROOT / "quiz_rejects.txt"
LOG = ROOT / "run_logs/quiz_llm.md"

FORBIDDEN = re.compile(r"\b(read_\w+|glob|copy|attach|detach|install|load|pragma|set|export|import|create|insert|update|delete|drop|alter|"
                       r"call|http\w*|s3|getenv|current_setting|sniff_csv|parquet_\w+|query_table|query)\b|;.+", re.I | re.S)
THEMES = ["jury votes and finales", "idols and advantages", "individual immunity", "tribal council votes and blindsides",
          "returning players and careers", "confessionals and the edit", "first boots and early exits", "season locations and formats",
          "challenges", "records across all US seasons", "journeys and twists", "ages and hometowns"]

WRITE = """You write questions for a daily Survivor quiz. Each question has one right answer that a SQL query over the
records returns, and three wrong answers that a second query returns. Fans of the US show play it.

Write {n} new questions, on these themes: {themes}.
- US seasons only (version = 'US'), unless a question names another country's version.
- Name the season in the question when it is about one season, by its name as season_summary.season_name has it.
- The answer is a castaway's name as castaways.castaway has it, a season name, a place, or a whole number.
- Ask for exact facts: no "best", "most strategic", "robbed". Superlatives are fine when the query computes them.
- Avoid what depends on how survivoR codes things (medical quits, which idol counts, fire-making as a vote).
- Do not repeat these existing questions: {existing}

Make them hard. The players are devoted fans who have seen most seasons; a good question is one they get right
about half the time. Write about specific moments and near misses: who received the second-most votes at a
council, which juror voted for the losing finalist, the episode an idol was played, who won a particular
challenge, votes against someone over a season, a record across seasons. Never ask:
- who won a season, where a season was filmed, how many castaways or tribes a season had
- anything the season's name or premise gives away (how many winners played Winners at War, how many
  returnees on an all-returnee season, which tribe was the heroes)
- anything a casual viewer would know offhand, or whose answer appears in the question
Be creative with what the warehouse holds at every layer: bronze (episode titles and IMDb ratings, viewers, tribe
names and swaps, the Survivor auction, jobs and hometowns, challenge descriptions and their skills, journeys),
silver (vote_dynamics: votes on the losing side, split votes, tie votes; jury_analysis: jurors voting with or
against their original tribe; challenge_performance: puzzle, water, endurance wins; advantage_strategy; social
positioning), and gold (the per-castaway feature tables). Vary the format as well: "which of these was NOT ...",
"of these four, who ... first", "which season had ...", "which episode ...", and numbers.
Choose wrong answers that a fan could believe: the runner-up in the same ranking, people from the same season
and stage of the game, numbers one or two away.

For each question return:
  "q"               the question, at most 20 words
  "sql"             DuckDB SQL returning the answer: one row and one column, or for a most/least/first question two
                    columns (answer, value) ordered by value, with no LIMIT, so ties can be checked
  "check_sql"       a second DuckDB query that reaches the same answer by a different route (another table, or another
                    column such as season_summary.final_vote, silver.jury_tally, silver.immunity_wins), with the
                    answer in the first column of its first row; the question is dropped when the two disagree
  "distractor_sql"  DuckDB SQL returning at least six plausible wrong answers of the same kind in one column (for
                    example the other castaways of that season, or other seasons' locations), no LIMIT needed
  "why"             one sentence explaining the answer, using {{answer}} and {{value}} where those go
  "theme"           the theme it belongs to
  "difficulty"      1 (casual fan knows it) to 5 (only the most devoted fan knows it); write only 3s, 4s and 5s

Reply with JSON only: {{"questions": [ ... ]}}.

The warehouse:
{context}"""

REVIEW = """You check quiz questions before they go live. Be strict: reject when unsure.

Question: {q}
SQL that computes the answer:
{sql}
A second query that agreed with it:
{check}
Answer it returned: {answer}{value}
Wrong choices shown beside it: {wrong}

Reply with JSON only: {{"ok": true or false, "difficulty": 1 to 5, "reason": "one short sentence"}}.
difficulty: 1 means a casual viewer knows it, 3 means a devoted fan gets it about half the time, 5 means almost no one.
ok is true only if all of these hold:
- It is hard enough: difficulty 3 or more. Reject winners, filming locations, cast sizes, and anything the season's
  name or premise gives away (how many winners played Winners at War).
- The SQL answers exactly the question as a fan would read it (same season, same event, same counting).
- Exactly one of the four choices can be right; none of the wrong choices is also a correct answer.
- The question does not hinge on how the dataset codes something that fans could dispute.
- The wording is clear and names what it needs to (the season, the show's version when not US)."""


# ---- providers ----------------------------------------------------------------------------------

def steer_table() -> dict:
    """STEER_PROVIDERS from the environment or ~/.config/preferencespace/steer.env. The file writes it as a
    single-quoted JSON value that spans several lines, as the shell reads it, so the match runs across lines."""
    raw = os.environ.get("STEER_PROVIDERS")
    env = Path.home() / ".config/preferencespace/steer.env"
    if not raw and env.exists():
        m = re.search(r"^STEER_PROVIDERS='(.*?)'[ \t]*$", env.read_text(), re.M | re.S)
        raw = m.group(1) if m else None
    return json.loads(raw) if raw else {}


def providers() -> dict:
    return steer_table()


def chat(pv: dict, prompt: str, max_tokens: int, temperature: float) -> str:
    body = {"model": pv["model"], "temperature": temperature, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}], **pv.get("kwargs", {})}
    req = urllib.request.Request(pv["base_url"].rstrip("/") + "/chat/completions", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", "Authorization": f"Bearer {pv['api_key']}"})
    with urllib.request.urlopen(req, timeout=180) as r:
        return json.load(r)["choices"][0]["message"]["content"]


def parse_json(text: str):
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    start = min([i for i in (text.find("{"), text.find("[")) if i >= 0], default=0)
    return json.loads(text[start:])


# ---- verification -------------------------------------------------------------------------------

def connect(data: Path):
    con = duckdb.connect()
    for f in sorted(data.glob("*.parquet")):
        layer, table = f.stem.split(".", 1)
        con.execute(f"create schema if not exists {layer}")
        con.execute(f"create table {layer}.{table} as select * from read_parquet('{f.as_posix()}')")
    # the views the warehouse page makes in the browser (static/survivor/gamebot/app.js, PAGE_VIEWS), so a query
    # that uses them runs here and in the SQL editor alike; keep the two in step
    con.execute("""create view silver.immunity_wins as
select castaway_id, version_season, episode, challenge_id, challenge_name, challenge_type, outcome_type, result,
       challenge_format = 'individual' as individual
from silver.challenge_performance
where challenge_type like 'Immunity%' and result in ('won', 'won (immunity only)')""")
    con.execute("create view silver.people as select castaway_id, full_name, castaway as short_name from bronze.castaway_details")
    con.execute("""create view silver.jury_tally as
select finalist_id as castaway_id, version_season,
       count(*) filter (where vote = '1.0') as jury_votes, count(*) as jurors
from bronze.jury_votes group by 1, 2""")
    con.execute("set enable_external_access = false")        # the tables are loaded; the model's SQL can reach nothing else
    return con


def run(con, sql: str):
    sql = sql.strip().rstrip(";").strip()
    if not re.match(r"(?is)^\s*(select|with)\b", sql) or FORBIDDEN.search(sql):
        raise ValueError("not a plain read-only SELECT")
    return con.execute(sql).fetchall()


def words(text: str) -> tuple[set, set]:
    return set(re.findall(r"\d+", text)), set(re.findall(r"\b[A-Z][a-zA-Z'.-]+", text))


def fmt(v) -> str:
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    return str(v).strip()


def verify(con, c: dict) -> tuple[dict | None, str]:
    """(question ready for the page, '') or (None, why it failed)."""
    try:
        rows = run(con, c["sql"])
    except Exception as e:
        return None, f"sql: {str(e).splitlines()[0][:120]}"
    if not rows:
        return None, "sql returned nothing"
    if len(rows[0]) == 1:
        if len(rows) != 1:
            return None, f"sql returned {len(rows)} rows"
        answer, value = rows[0][0], None
    elif len(rows[0]) == 2:
        if re.search(r"\blimit\b", c["sql"], re.I):
            return None, "a ranked query with LIMIT cannot show a tie"
        answer, value = rows[0]
        if len(rows) > 1 and rows[1][1] == value:
            return None, "tied at the top"
    else:
        return None, "sql returned more than two columns"
    if answer is None or len(fmt(answer)) > 60:
        return None, "no usable answer"
    answer = fmt(answer)
    if not answer.isdigit() and len(answer) > 2 and answer.lower() in c["q"].lower():
        return None, "the question gives the answer away"
    if not c.get("check_sql"):
        return None, "no second route"
    try:
        chk = run(con, c["check_sql"])
    except Exception as e:
        return None, f"check_sql: {str(e).splitlines()[0][:120]}"
    got = fmt(chk[0][0]) if chk and chk[0] and chk[0][0] is not None else None
    if got is None or got.lower() != answer.lower():
        return None, f"second route disagrees ({got} against {answer})"
    try:
        cands = [fmt(r[0]) for r in run(con, c["distractor_sql"]) if r and r[0] is not None]
    except Exception as e:
        return None, f"distractor_sql: {str(e).splitlines()[0][:120]}"
    pool = sorted({x for x in cands if x.lower() != answer.lower() and len(x) <= 60})
    if len(pool) < 3:
        return None, "fewer than three wrong answers"
    if re.fullmatch(r"\d+", answer) and not all(re.fullmatch(r"\d+", x) for x in pool):
        return None, "wrong answers are not numbers"
    if c.get("wrong") and all(w in pool for w in c["wrong"]):
        wrong = c["wrong"]                                   # a kept question keeps its choices while they still hold
    else:
        random.Random(hashlib.sha1((c["q"] + answer).encode()).hexdigest()).shuffle(pool)
        wrong = pool[:3]
    why = str(c.get("why", "")).replace("{answer}", answer).replace("{value}", fmt(value) if value is not None else "")
    nums, caps = words(why)
    allowed_n, allowed_c = words(" ".join([c["q"], answer, fmt(value) if value is not None else ""]))
    extra = (nums - allowed_n) | (caps - allowed_c - {"The", "A", "An", "In", "On", "At", "He", "She", "They", "It", "His", "Her", "Their",
                                                      "Survivor", "US", "Season", "That", "This", "With", "After", "Only"})
    if extra or not why or len(why.split()) > 40:
        why = f"The records say {answer}" + (f" ({fmt(value)})." if value is not None else ".")
    qid = "llm:" + hashlib.sha1(c["q"].strip().lower().encode()).hexdigest()[:10]
    return {"id": qid, "template": "model", "theme": c.get("theme", ""), "q": c["q"].strip(), "answer": answer, "wrong": wrong,
            "value": fmt(value) if value is not None else None, "sql": c["sql"].strip().rstrip(";"), "check_sql": c["check_sql"].strip().rstrip(";"),
            "distractor_sql": c["distractor_sql"].strip().rstrip(";"), "why": why}, ""


REVIEW_VERSION = 2          # raise when REVIEW changes; kept questions reviewed under an older version are reviewed again


def review(pv, q: dict) -> tuple[bool, str]:
    prompt = REVIEW.format(q=q["q"], sql=q["sql"], check=q["check_sql"], answer=q["answer"], value=f" (value {q['value']})" if q.get("value") else "",
                           wrong=", ".join(q["wrong"]))
    try:
        v = parse_json(chat(pv, prompt, 1500, 0))       # room for a reasoning pass if the checker has one on
        hard = int(v.get("difficulty") or 0) >= 3
        return bool(v.get("ok")) and hard, (str(v.get("reason", ""))[:200] + ("" if hard else f" (difficulty {v.get('difficulty')})"))
    except Exception as e:
        return False, f"review failed ({type(e).__name__})"


def norm(q: str) -> str:
    return re.sub(r"[^a-z0-9 ]", "", q.lower()).strip()


def same_question(a: dict, b: dict) -> bool:
    """The same answer and mostly the same words: a rewording of a question already asked."""
    if a["answer"].lower() != b["answer"].lower():
        return False
    x, y = set(norm(a["q"]).split()), set(norm(b["q"]).split())
    return len(x & y) / max(1, len(x | y)) >= 0.5


# ---- main ---------------------------------------------------------------------------------------

def main() -> int:
    args = sys.argv[1:]
    data = Path(args[args.index("--data") + 1]).resolve() if "--data" in args else DATA
    out = Path(args[args.index("--out") + 1]).resolve() if "--out" in args else OUT
    n = int(args[args.index("--n") + 1]) if "--n" in args else 20
    con = connect(data)
    kept_path = out / "llm.json"
    kept = json.loads(kept_path.read_text())["questions"] if kept_path.exists() else []
    rejects = {l.split()[0] for l in REJECTS.read_text().splitlines() if l.strip() and not l.lstrip().startswith("#")} if REJECTS.exists() else set()

    # re-verify what was kept, against today's data
    still = []
    for q in kept:
        if q["id"] in rejects:
            print(f"dropped {q['id']} (quiz_rejects.txt)")
            continue
        v, why = verify(con, q)
        if v is None:
            print(f"dropped {q['id']}: {why}")
            continue
        v["added"], v["reviewed_by"], v["review_v"] = q.get("added"), q.get("reviewed_by"), q.get("review_v", 1)
        still.append(v)
    print(f"re-verified {len(kept)} kept questions: {len(still)} still pass")

    added = []
    table = providers()
    if table and any(q["review_v"] < REVIEW_VERSION for q in still):
        ids = list(table)
        checker_id = next((k for k in ids if k != ("deepseek" if "deepseek" in table else ids[0])), ids[0])
        kept_now = []
        for q in still:
            if q["review_v"] >= REVIEW_VERSION:
                kept_now.append(q)
                continue
            ok, reason = review({**table[checker_id]}, q)
            if ok:
                q["review_v"], q["reviewed_by"] = REVIEW_VERSION, checker_id
                kept_now.append(q)
            else:
                print(f"dropped {q['id']} on re-review ({reason}): {q['q']}")
        still = kept_now
    if "--recheck" not in args and n > 0:
        if not table:
            print("no provider table in ~/.config/preferencespace/steer.env; asking for no new questions")
        else:
            ids = list(table)
            writer = {**table["deepseek" if "deepseek" in table else ids[0]]}
            checker_id = next((k for k in ids if k != ("deepseek" if "deepseek" in table else ids[0])), ids[0])
            checker = {**table[checker_id]}
            if "deepseek" in writer.get("base_url", ""):
                writer["kwargs"] = {"thinking": {"type": "disabled"}}
            bank = json.loads((out / "bank.json").read_text())["questions"] if (out / "bank.json").exists() else []
            seen = {norm(q["q"]) for q in bank + still}
            existing = "; ".join(q["q"] for q in random.sample(bank + still, min(40, len(bank + still))))
            ctx_file = data / "nl2sql_context.txt"
            context = ctx_file.read_text() if ctx_file.exists() else ""
            themes = ", ".join(random.sample(THEMES, 4))
            try:
                raw = parse_json(chat(writer, WRITE.format(n=n, themes=themes, existing=existing, context=context), 8000, 0.9))
                cands = raw.get("questions", raw) if isinstance(raw, dict) else raw
            except Exception as e:
                print(f"no new questions: the writer call failed ({type(e).__name__}: {str(e)[:120]})")
                cands = []
            for c in cands:
                if not isinstance(c, dict) or not all(k in c for k in ("q", "sql", "check_sql", "distractor_sql")):
                    print(f"skip (missing a query): {str(c.get('q') if isinstance(c, dict) else c)[:80]}")
                    continue
                if int(c.get("difficulty") or 3) < 3:
                    print(f"skip (the writer rated it easy): {c['q']}")
                    continue
                if norm(c["q"]) in seen:
                    print(f"skip (already asked): {c['q']}")
                    continue
                v, why = verify(con, c)
                if v is None:
                    print(f"skip ({why}): {c['q']}")
                    continue
                if v["id"] in rejects:
                    continue
                twin = next((q for q in bank + still + added if same_question(q, v)), None)
                if twin:
                    print(f"skip (a rewording of: {twin['q']}): {c['q']}")
                    continue
                ok, reason = review(checker, v)
                if not ok:
                    print(f"skip (review: {reason}): {c['q']}")
                    continue
                v["added"] = date.today().isoformat()
                v["reviewed_by"], v["review_v"] = checker_id, REVIEW_VERSION
                added.append(v)
                seen.add(norm(v["q"]))
                print(f"added {v['id']}: {v['q']} -> {v['answer']}")

    out.mkdir(parents=True, exist_ok=True)
    kept_path.write_text(json.dumps({"questions": still + added}, separators=(",", ":"), default=str))
    if added:
        LOG.parent.mkdir(exist_ok=True)
        with LOG.open("a") as fh:
            fh.write(f"\n## {date.today().isoformat()}: {len(added)} added\n\n")
            for q in added:
                fh.write(f"- `{q['id']}` {q['q']} **{q['answer']}** (wrong: {', '.join(q['wrong'])})\n  ```sql\n  {q['sql']}\n  ```\n")
    print(f"wrote {kept_path}: {len(still) + len(added)} model questions ({len(added)} new)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
