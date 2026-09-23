#!/usr/bin/env python3
"""
Gamebot's refresh. When survivoR publishes new data, rebuild the warehouse, check the result, and ship it
to preferencespace.com. launchd starts it every morning at 06:30 (deploy/com.preferencespace.gamebot.plist),
and most mornings it stops at the gate.

  1. gate     Ask survivoR's files for their signatures (HTTP HEAD, no download) and compare them with
              the last run that shipped (run_logs/refresh_state.json). No change: exit 0.
  2. build    scripts/run_lite.py --force-refresh: download, bronze, dataset_versions, dbt build with
              its tests, the gamebot-lite SQLite, pytest.
  3. export   scripts/export_site.py --out preferencespace/.staging/gamebot-data.new; the live data stays untouched.
  4. check    Compare the staged copy with the live data: the same tables, no fewer seasons or
              castaway-seasons, one gold row per key, every file present and non-empty.
  5. prompt   build_context.py --data <the staged copy>: the question box's prompt for the new data.
  5b. fans    The fan pages' data, staged beside the rest: who-goes-home's council table (build.py) and boot
              odds for the latest season (live.py), the season page's episode cards, edit, blocs and comps with
              a model-written lede (scripts/season_hub.py --season latest --lede), and the daily quiz's
              question bank (scripts/quiz_bank.py) with its model-written questions (scripts/quiz_llm.py). Each starts from a copy of the live files, so older
              seasons carry over. A failure here is reported and the fan pages keep yesterday's data; it
              does not hold back the warehouse data.
  6. ship     Under the site lock (jobwatch's daily deploy takes the same one): move the staged copies
              into place, keep the replaced data as .staging/gamebot-data.old, build the site, wrangler deploy.
  7. record   Write the shipped signatures and totals to run_logs/refresh_state.json.

Any failure stops the run and posts a macOS notification. Nothing after the failure runs, and the
state file does not move, so the next morning's run tries again. Before step 6 the live data is never
touched; after a failed deploy the local data is new but checked, and the next run ships it.

    uv run python deploy/refresh.py               # the scheduled run
    uv run python deploy/refresh.py --force       # rebuild and ship even when survivoR has not changed
    uv run python deploy/refresh.py --no-deploy   # stop after step 5 and leave the staged copy for a look
    uv run python deploy/refresh.py --mark-shipped  # record today's signatures as shipped, build nothing

Logs: run_logs/refresh.log (launchd appends stdout and stderr).
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT.parent / "preferencespace"
DATA = SITE / "static/survivor/gamebot/data"
STAGING = SITE / ".staging"             # outside static/, so build.py never copies a staged or retired copy into the site
NEW = STAGING / "gamebot-data.new"
OLD = STAGING / "gamebot-data.old"
WGH = ROOT.parent / "who-goes-home"
FAN = {                                 # the fan pages' live data and its staged copy
    "season": SITE / "static/survivor/season/data",
    "quiz": SITE / "static/survivor/quiz/data",
}
STATE = ROOT / "run_logs/refresh_state.json"
LOCK = SITE / ".deploy.lock"
PATH = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:" + str(Path.home() / ".npm-global" / "bin")

# survivoR corrects old records now and then, so the row count can dip a little between releases.
# A drop of more than 1% is a broken load, not a correction.
MIN_ROW_RATIO = 0.99

sys.path.insert(0, str(ROOT))


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def say(text: str) -> None:
    print(f"[{now()}] {text}", flush=True)


def notify(title: str, text: str) -> None:
    """A macOS notification; launchd runs this agent in the logged-in session, so it shows."""
    script = f"display notification {json.dumps(text)} with title {json.dumps(title)}"
    subprocess.run(["osascript", "-e", script], check=False, capture_output=True)


def step(args: list[str], cwd: Path) -> None:
    say(f"$ {' '.join(args)}   (in {cwd.name})")
    env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}   # Gamebot's venv must not leak into the site's uv
    subprocess.run(args, cwd=cwd, check=True, env={**env, "PATH": PATH, "PYTHONUNBUFFERED": "1"})


@contextmanager
def site_lock():
    """One deploy at a time: this run and jobwatch's daily run both build and deploy the same site."""
    with LOCK.open("w") as fh:
        say("waiting for the site lock")
        fcntl.flock(fh, fcntl.LOCK_EX)
        say("holding the site lock")
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


# ---- 1. gate -----------------------------------------------------------------------------------

def upstream_signatures() -> dict[str, str]:
    """{dataset: signature} for every survivoR dataset the bronze load reads. HEAD requests only."""
    import params
    from gamebot_core.source_metadata import select_dataset_metadata
    out = {}
    for d in params.dataset_order:
        name = d["dataset"]
        out[name] = select_dataset_metadata(name, params.base_raw_url, params.json_raw_url)["signature"]
    return out


def load_state() -> dict:
    return json.loads(STATE.read_text()) if STATE.exists() else {}


def changed(current: dict[str, str], shipped: dict[str, str]) -> list[str]:
    return sorted(k for k in current if current[k] != shipped.get(k))


# ---- 4. check ----------------------------------------------------------------------------------

def check(new_dir: Path, live_dir: Path) -> tuple[list[str], list[str]]:
    """(problems, summary). Any problem stops the run before the live data changes."""
    problems, summary = [], []
    schema = json.loads((new_dir / "schema.json").read_text())
    new = schema["totals"]
    for t in schema["tables"]:
        f = new_dir / t["file"]
        if not f.exists() or f.stat().st_size == 0:
            problems.append(f"{t['file']} is missing or empty")
    for f in ("castaways.json", "nl2sql_context.txt", "SOURCE-LICENSE.txt"):
        if not (new_dir / f).exists():
            problems.append(f"{f} is missing")
    if new["gold_rows"] != new["gold_keys"]:
        problems.append(f"gold repeats keys: {new['gold_rows']} rows for {new['gold_keys']} castaway-seasons")
    live_schema = live_dir / "schema.json"
    if live_schema.exists():
        old = json.loads(live_schema.read_text())["totals"]
        if new["tables"] != old["tables"]:
            problems.append(f"{new['tables']} tables where the live data has {old['tables']}")
        for k in ("seasons", "us_seasons", "castaway_seasons"):
            if new[k] < old[k]:
                problems.append(f"{k} fell from {old[k]} to {new[k]}")
        if new["rows"] < old["rows"] * MIN_ROW_RATIO:
            problems.append(f"rows fell from {old['rows']:,} to {new['rows']:,}")
        summary.append(f"seasons {old['seasons']} -> {new['seasons']}, castaway-seasons {old['castaway_seasons']} -> "
                       f"{new['castaway_seasons']}, rows {old['rows']:,} -> {new['rows']:,} ({new['rows'] - old['rows']:+,})")
    return problems, summary


# ---- 5b. fans -----------------------------------------------------------------------------------

def staged(name: str) -> Path:
    return STAGING / f"{name}-data.new"


def fan_data(uv: str) -> list[str]:
    """Stage the fan pages' data and check it. Returns the names of the copies that passed, to swap in."""
    for name, live in FAN.items():
        new = staged(name)
        if new.exists():
            shutil.rmtree(new)
        if live.exists():
            shutil.copytree(live, new)
        else:
            new.mkdir(parents=True)
    step([uv, "run", "python", "build.py"], WGH)                                  # data/councils.csv, data/next_councils.csv
    step([uv, "run", "python", "live.py", "--out", str(staged("season"))], WGH)  # odds_<latest>.json
    step([uv, "run", "--with", "pandas", "python", "scripts/season_hub.py", "--season", "latest", "--lede", "--out", str(staged("season"))], ROOT)
    step([uv, "run", "python", "scripts/quiz_bank.py", "--out", str(staged("quiz"))], ROOT)
    # model-written questions: re-verify the kept ones on the new data, then ask for more (exits 0 if the model is down)
    step([uv, "run", "--with", "duckdb", "python", "scripts/quiz_llm.py", "--data", str(NEW), "--out", str(staged("quiz"))], ROOT)
    ok = []
    idx = json.loads((staged("season") / "seasons.json").read_text())
    latest = idx.get("latest")
    problems = [f"{f} is missing" for f in (f"hub_{latest}.json", f"odds_{latest}.json") if not (staged("season") / f).exists()]
    live_idx = FAN["season"] / "seasons.json"
    if live_idx.exists() and len(idx["seasons"]) < len(json.loads(live_idx.read_text())["seasons"]):
        problems.append("the season list got shorter")
    if problems:
        say(f"fan check failed (season page keeps its data): {'; '.join(problems)}")
    else:
        ok.append("season")
    bank = json.loads((staged("quiz") / "bank.json").read_text())["questions"]
    live_bank = FAN["quiz"] / "bank.json"
    before = len(json.loads(live_bank.read_text())["questions"]) if live_bank.exists() else 0
    if len(bank) < max(100, int(before * 0.9)):
        say(f"fan check failed (quiz keeps its bank): {len(bank)} questions where the live bank has {before}")
    else:
        ok.append("quiz")
    say(f"fan data staged: season page {latest}, quiz {len(bank)} questions ({len(bank) - before:+})")
    return ok


# ---- 6. ship -----------------------------------------------------------------------------------

def swap_in(fans: list[str]) -> None:
    """The staged copies become the live data, and the data they replace is kept for one rollback."""
    for new, live, old in [(NEW, DATA, OLD)] + [(staged(n), FAN[n], STAGING / f"{n}-data.old") for n in fans]:
        if old.exists():
            shutil.rmtree(old)
        if live.exists():
            live.rename(old)
        live.parent.mkdir(parents=True, exist_ok=True)
        new.rename(live)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="rebuild and ship even when survivoR has not changed")
    ap.add_argument("--no-deploy", action="store_true", help="stop after the checks and leave the staged copy in place")
    ap.add_argument("--mark-shipped", action="store_true", help="record today's signatures as shipped and stop")
    a = ap.parse_args()
    uv = shutil.which("uv", path=PATH) or "/opt/homebrew/bin/uv"
    wrangler = shutil.which("wrangler", path=PATH) or "/opt/homebrew/bin/wrangler"
    say("== gamebot refresh ==")
    try:
        # 1. gate
        current = upstream_signatures()
        state = load_state()
        if a.mark_shipped:
            STATE.parent.mkdir(exist_ok=True)
            STATE.write_text(json.dumps({"shipped_at": now(), "signatures": current, "totals": None, "note": "marked by hand"}, indent=1))
            say(f"recorded {len(current)} signatures as shipped; nothing built")
            return 0
        diff = changed(current, state.get("signatures", {}))
        if not diff and not a.force:
            say(f"no upstream change since {state.get('shipped_at', 'the last run')}; nothing to do")
            return 0
        say(f"changed upstream: {', '.join(diff) or '(none; --force)'}")

        # 2. build, 3. export
        step([uv, "run", "python", "scripts/run_lite.py", "--force-refresh"], ROOT)
        if NEW.exists():
            shutil.rmtree(NEW)
        STAGING.mkdir(exist_ok=True)
        step([uv, "run", "--with", "pyarrow", "--with", "pandas", "python", "scripts/export_site.py", "--out", str(NEW)], ROOT)

        # 4. check, 5. prompt
        step([uv, "run", "--with", "duckdb", "python", "tools/gamebot/nl2sql/build_context.py", "--data", str(NEW)], SITE)
        problems, summary = check(NEW, DATA)
        for line in summary:
            say(line)
        if problems:
            for p in problems:
                say(f"check failed: {p}")
            notify("Gamebot refresh stopped", f"{len(problems)} check(s) failed; the live data is unchanged. See run_logs/refresh.log.")
            return 1
        # 5b. fans: a failure here leaves the fan pages as they are and the rest goes on
        try:
            fans = fan_data(uv)
        except Exception as e:
            fans = []
            say(f"fan data stopped ({type(e).__name__}: {e}); the fan pages keep their data")
            notify("Gamebot fan pages not refreshed", f"{type(e).__name__}. The warehouse data still ships. See run_logs/refresh.log.")
        if a.no_deploy:
            say(f"checks passed; --no-deploy leaves the new data in {STAGING}")
            return 0

        # 6. ship
        with site_lock():
            swap_in(fans)
            say(f"new data is in place ({', '.join(['warehouse'] + fans)}); the previous data is in {STAGING} as *-data.old")
            step([uv, "run", "python", "build.py"], SITE)
            step([wrangler, "deploy"], SITE)

        # 7. record
        totals = json.loads((DATA / "schema.json").read_text())["totals"]
        STATE.parent.mkdir(exist_ok=True)
        STATE.write_text(json.dumps({"shipped_at": now(), "signatures": current, "totals": totals, "changed": diff}, indent=1))
        say(f"== shipped: {'; '.join(summary) or 'first run'} ==")
        notify("Gamebot refreshed", summary[0] if summary else "New survivoR data is live.")
        return 0
    except subprocess.CalledProcessError as e:
        say(f"== stopped: {Path(str(e.cmd[0])).name} exited {e.returncode} ==")
        notify("Gamebot refresh stopped", f"A step failed ({Path(str(e.cmd[0])).name}). See run_logs/refresh.log.")
        return e.returncode or 1
    except Exception as e:                  # the gate's network calls, a missing file: say so and stop
        say(f"== stopped: {type(e).__name__}: {e} ==")
        notify("Gamebot refresh stopped", f"{type(e).__name__}: {str(e)[:120]}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
