#!/usr/bin/env python3
"""
The season page's data: for every US season, episode by episode, what a fan wants the morning after.

    uv run --with pandas python scripts/season_hub.py                       # from Gamebot/
    uv run --with pandas python scripts/season_hub.py --out DIR             # somewhere other than the live directory
    uv run --with pandas python scripts/season_hub.py --season US50         # one season (the refresh passes the latest)
    uv run --with pandas python scripts/season_hub.py --season latest --lede   # and a two-sentence lede per episode

Reads gamebot_lite/data/gamebot.sqlite. Writes, per season, hub_<version_season>.json, and seasons.json (the list).
Every number is computed here from the records; nothing is estimated. For episode e a season file holds:

  card     facts about episode e, each set against the record before it: who left and by what vote, idols
           played and the votes they cancelled, individual immunity, the confessional leader and the quiet ones
  edit     each castaway's confessionals: this episode, to date, and an index where 1.0 is an even share
  blocs    who voted with whom through episode e: agreement over the councils two castaways shared
  comps    for each castaway still in the game, the three castaway-seasons from other US seasons whose game
           through the same episode is closest

and, once per season, the band of eventual winners' confessional index at each episode, from other seasons.

With --lede, a hosted model writes a two-sentence lede for each episode of the season being written, from that
episode's facts and nothing else. A lede that contains a number or a name the facts do not is thrown away and the
page shows the facts alone. The key comes from ~/.config/preferencespace/steer.env (the word box's provider table);
ledes are cached in run_logs/ledes.json by the facts they were written from, so an unchanged episode is not rewritten.
"History" in a card fact means US seasons that aired before this one plus this season's earlier episodes,
so replaying an old season shows what was a record at the time.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import urllib.request
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "gamebot_lite/data/gamebot.sqlite"
if "--db" in sys.argv:
    DB = Path(sys.argv[sys.argv.index("--db") + 1])
OUT = ROOT.parent / "preferencespace/static/survivor/season/data"

ALLY_SHARED, ALLY_AGREE = 2, 0.75        # the Who Goes Home rule: two shared councils, three of four together
QUIET_STREAK = 3                         # a castaway with no confessional for this many episodes in a row gets a line
COMP_FEATURES = {                        # the game through episode e, compared across seasons at the same episode
    "imm_wins": "individual immunity wins",
    "team_win_rate": "team challenge win rate",
    "votes_received": "votes received",
    "correct_rate": "share of votes cast with the majority that went home",
    "councils": "tribal councils attended",
    "idols_found": "idols found",
    "idols_played": "idols played",
    "conf_index": "confessional index",
}
NO_VOTE = {"Final 3 tribal", "Fire challenge", "Fire challenge (f4)", "Player quit", "Quit"}
FOUND = {"Found", "Found (beware)", "Received", "Recieved"}


def ordinal(n: int) -> str:
    n = int(n)
    return f"{n}{'th' if 11 <= n % 100 <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')}"


def plural(n, one, many=None):
    n = int(n)
    return f"{n} {one if n == 1 else (many or one + 's')}"


def names_list(xs: list[str]) -> str:
    return xs[0] if len(xs) == 1 else ", ".join(xs[:-1]) + " and " + xs[-1]


def load(db: Path) -> dict[str, pd.DataFrame]:
    con = sqlite3.connect(db)
    t = {n: pd.read_sql_query(f"select * from {n} where version = 'US'", con)
         for n in ["castaways", "vote_history", "challenge_results", "advantage_movement", "confessionals", "episodes", "season_summary"]}
    t["castaway_details"] = pd.read_sql_query("select castaway_id, gender from castaway_details", con)
    con.close()
    for n in t:
        if "episode" in t[n]:
            t[n] = t[n][t[n]["episode"].notna()].copy()
            t[n]["episode"] = t[n]["episode"].astype(int)
    return t


# ---- per-council records, first round of voting --------------------------------------------------

def councils(vh: pd.DataFrame) -> pd.DataFrame:
    """One row per council with a vote: episode, first-round votes {voter: target}, who left, the tally, what happened."""
    rows = []
    first = vh[vh["castaway_id"].notna()].sort_values("vote_order").groupby(["version_season", "sog_id", "castaway_id"], as_index=False).first()
    events = vh.groupby(["version_season", "sog_id"])["vote_event"].apply(lambda s: sorted(set(s.dropna())))
    for (vs, sog), g in first.groupby(["version_season", "sog_id"]):
        ev = events.get((vs, sog), [])
        if set(ev) & NO_VOTE or g["vote_id"].notna().sum() == 0:
            continue
        votes = {c: v for c, v in zip(g["castaway_id"], g["vote_id"]) if isinstance(v, str)}
        left = sorted(set(vh[(vh.version_season == vs) & (vh.sog_id == sog)]["voted_out_id"].dropna()))
        tally = pd.Series(list(votes.values())).value_counts()
        nullified = int(vh[(vh.version_season == vs) & (vh.sog_id == sog) & (vh.nullified == 1)].shape[0])
        rows.append({"version_season": vs, "sog_id": int(sog), "episode": int(g["episode"].iloc[0]), "votes": votes, "left": left,
                     "tally": [int(x) for x in tally.values], "events": ev, "nullified": nullified,
                     "present": list(g["castaway_id"])})
    return pd.DataFrame(rows)


# ---- cumulative game state per castaway, season and episode --------------------------------------

def game_state(t: dict, C: pd.DataFrame) -> pd.DataFrame:
    """One row per (version_season, castaway_id, episode) with the game through that episode."""
    cast = t["castaways"]
    ep_max = t["episodes"].groupby("version_season")["episode"].max()
    cr = t["challenge_results"].copy()
    won = cr["result"].fillna("").str.startswith("Won")
    cr["imm_wins"] = (cr["challenge_type"].fillna("").str.contains("Immunity") & cr["outcome_type"].fillna("").str.contains("Individual")
                      & won & (cr["result"] != "Won (reward only)")).astype(int)
    cr["team_entered"] = (cr["outcome_type"].fillna("").str.contains("Tribal|Team") & (cr["sit_out"].fillna(0) == 0)).astype(int)
    cr["team_won"] = (cr["team_entered"].astype(bool) & won).astype(int)
    ch = cr.groupby(["version_season", "castaway_id", "episode"])[["imm_wins", "team_entered", "team_won"]].sum()

    am = t["advantage_movement"]
    am = am.assign(idols_found=am["event"].isin(FOUND).astype(int), idols_played=(am["event"] == "Played").astype(int))
    adv = am.groupby(["version_season", "castaway_id", "episode"])[["idols_found", "idols_played"]].sum()

    cf = t["confessionals"].copy()
    cf["confessional_count"] = cf["confessional_count"].fillna(0)
    conf = cf.groupby(["version_season", "castaway_id", "episode"])[["confessional_count"]].sum()
    ep_total = cf.groupby(["version_season", "episode"])["confessional_count"].sum()
    n_conf_cast = cf.groupby("version_season")["castaway_id"].nunique()

    vote_rows = []
    for c in C.itertuples():
        for voter, target in c.votes.items():
            vote_rows.append((c.version_season, voter, c.episode, 0, 1, int(target in c.left)))
            vote_rows.append((c.version_season, target, c.episode, 1, 0, 0))
    V = pd.DataFrame(vote_rows, columns=["version_season", "castaway_id", "episode", "votes_received", "votes_cast", "votes_correct"])
    V = V.groupby(["version_season", "castaway_id", "episode"]).sum()
    att = pd.DataFrame([(c.version_season, p, c.episode, 1) for c in C.itertuples() for p in c.present],
                       columns=["version_season", "castaway_id", "episode", "councils"]).groupby(["version_season", "castaway_id", "episode"]).sum()

    frames = []
    for r in cast.drop_duplicates(["version_season", "castaway_id"]).itertuples():
        vs, cid = r.version_season, r.castaway_id
        if vs not in ep_max.index:
            continue
        E = int(ep_max[vs])
        idx = pd.MultiIndex.from_tuples([(vs, cid, e) for e in range(1, E + 1)], names=["version_season", "castaway_id", "episode"])
        f = pd.DataFrame(index=idx)
        for src in (ch, adv, conf, V, att):
            f = f.join(src, how="left")
        f = f.fillna(0).groupby(level=[0, 1]).cumsum()
        f["exit_episode"] = int(r.episode) if pd.notna(r.episode) else E + 1
        f["result"] = r.result
        frames.append(f)
    S = pd.concat(frames).reset_index()
    S["team_win_rate"] = np.where(S["team_entered"] > 0, S["team_won"] / S["team_entered"].replace(0, np.nan), np.nan)
    S["correct_rate"] = np.where(S["votes_cast"] > 0, S["votes_correct"] / S["votes_cast"].replace(0, np.nan), np.nan)
    tot = ep_total.groupby(level=0).cumsum()
    S["season_conf_to_date"] = [tot.get((vs, e), np.nan) for vs, e in zip(S["version_season"], S["episode"])]
    S["conf_index"] = S["confessional_count"] / (S["season_conf_to_date"] / S["version_season"].map(n_conf_cast))
    return S


# ---- the card: facts about one episode, against the record before it ----------------------------

class History:
    """Running records over US seasons in air order, so a fact is judged against what had aired."""

    def __init__(self):
        self.nullified = []            # votes cancelled by each idol play
        self.ep_conf = []              # confessionals by one castaway in one episode
        self.imm_season = []           # individual immunity wins by a castaway in a completed season
        self.pocket = 0                # castaways voted out holding an advantage

    def rank_high(self, xs, v):        # how many earlier values are at least v
        return sum(1 for x in xs if x >= v)


def card(vs, e, t, C, S, H: History, names):
    facts = []
    cast = t["castaways"][t["castaways"].version_season == vs]
    out = cast[cast["episode"] == e]
    Ce = C[(C.version_season == vs) & (C.episode == e)]
    am = t["advantage_movement"]
    am = am[(am.version_season == vs) & (am.episode == e)]

    gender = dict(zip(t["castaway_details"]["castaway_id"], t["castaway_details"]["gender"]))
    self_word = lambda cid: {"Female": "herself", "Male": "himself"}.get(gender.get(cid), "themselves")
    done = set()
    # who left, and by what vote; two sent home by one vote share a line
    for c in Ce.itertuples():
        gone = [x for x in c.left if x in set(out["castaway_id"])]
        if len(gone) > 1 and c.tally:
            ws = [names.get(x, x) for x in gone]
            facts.append({"kind": "boot", "text": f"{names_list(ws)} were both voted out at one tribal council, by a vote of {'-'.join(map(str, c.tally))}."})
            done.update(gone)
    for r in out.sort_values("day").itertuples():
        if r.castaway_id in done:
            continue
        res = str(r.result or "")
        who = names.get(r.castaway_id, r.castaway)
        if "voted out" in res:
            c = next((c for c in Ce.itertuples() if r.castaway_id in c.left), None)
            how = ""
            if c is not None:
                if "Rock draw" in c.events:
                    how = " on a rock draw after a deadlocked vote"
                elif "Deadlock" in c.events or len(c.tally) > 1 and c.tally[0] == c.tally[1]:
                    how = ", after a tied vote and a revote"
                elif c.tally:
                    how = " unanimously" if len(c.tally) == 1 and c.tally[0] > 1 else f" by a vote of {'-'.join(map(str, c.tally))}"
            facts.append({"kind": "boot", "text": f"{who} was {res}{how}, finishing {ordinal(r.place)}." if str(r.place).isdigit() else f"{who} was {res}{how}."})
            if not am[(am.castaway_id == r.castaway_id) & (am.event == "Voted out with advantage")].empty:
                facts.append({"kind": "record", "text": f"{who} went home holding an advantage, the {ordinal(H.pocket + 1)} castaway on record to do so."})
                H.pocket += 1
        elif "Sole Survivor" in res:
            ss = t["season_summary"].set_index("version_season")
            fv = ss.loc[vs, "final_vote"] if vs in ss.index else None
            facts.append({"kind": "winner", "text": f"{who} won Survivor" + (f", {fv} at the final tribal council." if isinstance(fv, str) and fv else ".")})
        elif any(k in res for k in ("Quit", "Medically", "Withdrew", "Ejected")):
            facts.append({"kind": "exit", "text": f"{who} left the game: {res.lower()}."})
        elif "Eliminated" in res:
            vh = t["vote_history"]
            fire = vh[(vh.version_season == vs) & (vh.voted_out_id == r.castaway_id) & vh["vote_event"].fillna("").str.startswith("Fire challenge")]
            how = " in the fire-making challenge" if len(fire) else ""
            facts.append({"kind": "exit", "text": f"{who} was eliminated{how}, finishing {ordinal(r.place)}." if str(r.place).isdigit() else f"{who} was eliminated{how}."})

    # idols and the votes they cancelled
    for r in am[am.event == "Played"].itertuples():
        who = names.get(r.castaway_id, r.castaway)
        target = names.get(r.played_for_id, r.played_for) if isinstance(r.played_for_id, str) else None
        n = int(r.votes_nullified) if pd.notna(r.votes_nullified) else 0
        for_whom = f"for {self_word(r.castaway_id)}" if target is None or r.played_for_id == r.castaway_id else f"for {target}"
        if n > 0:
            more = H.rank_high(H.nullified, n)
            ctx = f" No idol had cancelled that many before." if more == 0 and H.nullified else \
                  f" Only {plural(more, 'earlier play')} cancelled as many." if more <= 10 else ""
            facts.append({"kind": "idol", "text": f"{who} played an idol {for_whom}, cancelling {plural(n, 'vote')}.{ctx}"})
        else:
            facts.append({"kind": "idol", "text": f"{who} played an idol {for_whom}; it cancelled no votes."})
        H.nullified.append(n)

    # individual immunity
    se = S[(S.version_season == vs) & (S.episode == e)]
    prev = S[(S.version_season == vs) & (S.episode == e - 1)].set_index("castaway_id")["imm_wins"] if e > 1 else pd.Series(dtype=float)
    for r in se.itertuples():
        won_now = r.imm_wins - prev.get(r.castaway_id, 0)
        if won_now > 0:
            who = names.get(r.castaway_id, r.castaway_id)
            n = int(r.imm_wins)
            ctx = ""
            if n >= 3:
                more = H.rank_high(H.imm_season, n)
                ctx = f" No one had won {n} in a season before." if more == 0 else \
                      f" Only {plural(more, 'castaway')} had won {n} or more in a season before." if more <= 5 else ""
            facts.append({"kind": "immunity", "text": f"{who} won individual immunity, {'a first' if n == 1 else 'the ' + ordinal(n)} this season.{ctx}"})

    # confessionals: the leader, and the quiet ones still in the game
    cf = t["confessionals"]
    cf = cf[(cf.version_season == vs) & (cf.episode == e)].copy()
    cf["confessional_count"] = cf["confessional_count"].fillna(0)
    if len(cf) and cf["confessional_count"].max() > 0:
        top = cf[cf["confessional_count"] == cf["confessional_count"].max()]
        n = int(top["confessional_count"].iloc[0])
        more = H.rank_high(H.ep_conf, n)
        share = more / len(H.ep_conf) if H.ep_conf else 1
        ctx = f" That is more than anyone had in one episode before." if more == 0 and H.ep_conf else \
              f" Only {plural(more, 'time')} before had anyone had as many in one episode." if share < 0.01 else ""
        facts.append({"kind": "edit", "text": f"{names_list([names.get(c, c) for c in top['castaway_id']])} had the most confessionals ({n}).{ctx}"})
        H.ep_conf.extend(int(x) for x in cf["confessional_count"])
    quiet = []
    for cid, g in t["confessionals"][t["confessionals"].version_season == vs].groupby("castaway_id"):
        g = g[g.episode <= e].sort_values("episode")
        if not len(g) or g["episode"].iloc[-1] != e:
            continue
        streak = 0
        for x in g["confessional_count"].fillna(0).iloc[::-1]:
            if x == 0:
                streak += 1
            else:
                break
        if streak >= QUIET_STREAK:
            quiet.append((names.get(cid, cid), streak))
    for who, k in quiet:
        facts.append({"kind": "edit", "text": f"{who} has gone {k} episodes without a confessional."})
    return facts


# ---- blocs and comps ---------------------------------------------------------------------------

def blocs(vs, e, C, alive):
    shared, agree = defaultdict(int), defaultdict(int)
    for c in C[(C.version_season == vs) & (C.episode <= e)].itertuples():
        voters = sorted(c.votes)
        for i, a in enumerate(voters):
            for b in voters[i + 1:]:
                shared[(a, b)] += 1
                agree[(a, b)] += int(c.votes[a] == c.votes[b])
    pairs = [[a, b, agree[(a, b)], n] for (a, b), n in shared.items()]
    # groups: castaways still in the game in which every pair meets the ally rule (complete linkage, so one
    # shared ally does not chain two camps together)
    ok = {(a, b) for a, b, ag, n in pairs if n >= ALLY_SHARED and ag / n >= ALLY_AGREE}
    ally = lambda a, b: (a, b) in ok or (b, a) in ok
    groups = [[c] for c in alive]
    for a, b, ag, n in sorted(pairs, key=lambda p: (-p[2] / p[3], -p[3])):
        if not ally(a, b):
            continue
        ga = next((g for g in groups if a in g), None)
        gb = next((g for g in groups if b in g), None)
        if ga is None or gb is None or ga is gb:
            continue
        if all(ally(x, y) for x in ga for y in gb):
            ga.extend(gb)
            groups.remove(gb)
    return {"pairs": pairs, "groups": sorted((sorted(g) for g in groups if len(g) > 1), key=len, reverse=True)}


def comps(vs, e, S, names, season_name, k=3):
    at = S[(S.episode == e) & (S.exit_episode > e)]
    pool = at[at.version_season != vs]
    mine = at[at.version_season == vs]
    if pool.empty or mine.empty:
        return {}
    cols = list(COMP_FEATURES)
    mu, sd = pool[cols].mean(), pool[cols].std().replace(0, 1).fillna(1)
    Zp = ((pool[cols].fillna(mu) - mu) / sd).to_numpy()
    out = {}
    for r in mine.itertuples():
        z = ((pd.Series({c: getattr(r, c) for c in cols}).fillna(mu) - mu) / sd).to_numpy()
        d = np.sqrt(((Zp - z) ** 2).sum(1))
        best = np.argsort(d)[:k]
        rows = []
        for j in best:
            p = pool.iloc[j]
            rows.append({"id": p.castaway_id, "vs": p.version_season, "name": names.get(p.castaway_id, p.castaway_id),
                         "season": season_name.get(p.version_season, p.version_season), "result": p.result, "distance": round(float(d[j]), 2),
                         "then": {c: (None if pd.isna(p[c]) else round(float(p[c]), 2)) for c in cols}})
        out[r.castaway_id] = {"now": {c: (None if pd.isna(getattr(r, c)) else round(float(getattr(r, c)), 2)) for c in cols}, "comps": rows}
    return out


# ---- the lede: a model writes it, the facts check it ----------------------------------------------

LEDE_CACHE = ROOT / "run_logs/ledes.json"
LEDE_PROMPT = ("Write a two-sentence recap of this Survivor episode for a fan, in at most 45 words, using only the facts "
               "below. Add no fact, number, name or judgment that is not in them. Plain words, past tense, no hype.\n\n"
               "Episode: {title}\nFacts:\n{facts}")


def steer_table() -> dict:
    """STEER_PROVIDERS from the environment or ~/.config/preferencespace/steer.env. The file writes it as a
    single-quoted JSON value that spans several lines, as the shell reads it, so the match runs across lines."""
    raw = os.environ.get("STEER_PROVIDERS")
    env = Path.home() / ".config/preferencespace/steer.env"
    if not raw and env.exists():
        m = re.search(r"^STEER_PROVIDERS='(.*?)'[ \t]*$", env.read_text(), re.M | re.S)
        raw = m.group(1) if m else None
    return json.loads(raw) if raw else {}


def provider():
    """The first hosted provider in the word box's table, with thinking off: a lede needs no reasoning."""
    table = steer_table()
    pid = "deepseek" if "deepseek" in table else next(iter(table), None)
    return None if pid is None else {**table[pid], "kwargs": {"thinking": {"type": "disabled"}}}


def words(text: str) -> tuple[set, set]:
    """(numbers, capitalized words) in a text: what a lede may not invent."""
    return set(re.findall(r"\d+", text)), set(re.findall(r"\b[A-Z][a-zA-Z'-]+", text))


def lede(title: str, facts: list[dict], cache: dict, pv) -> str | None:
    text = "\n".join(f"- {f['text']}" for f in facts)
    if not facts or pv is None:
        return None
    key = hashlib.sha1((title + "\n" + text).encode()).hexdigest()
    if key in cache:
        return cache[key]
    body = json.dumps({"model": pv["model"], "temperature": 0, "max_tokens": 200, **pv.get("kwargs", {}),
                       "messages": [{"role": "user", "content": LEDE_PROMPT.format(title=title, facts=text)}]}).encode()
    req = urllib.request.Request(pv["base_url"].rstrip("/") + "/chat/completions", data=body,
                                 headers={"Content-Type": "application/json", "Authorization": f"Bearer {pv['api_key']}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            out = json.load(r)["choices"][0]["message"]["content"].strip()
    except Exception as e:                                  # no lede is fine; the facts stand alone
        print(f"lede skipped ({type(e).__name__})", flush=True)
        return None
    nums, caps = words(out)
    fnums, fcaps = words(title + "\n" + text)
    extra = (nums - fnums) | (caps - fcaps - {"The", "A", "An", "In", "On", "At", "After", "With", "Survivor", "Episode", "It", "He", "She", "They", "His", "Her", "Their"})
    result = None if extra or len(out.split()) > 60 else out
    if extra:
        print(f"lede rejected, not in the facts: {sorted(extra)}", flush=True)
    cache[key] = result
    return result


# ---- assembly ----------------------------------------------------------------------------------

def main() -> int:
    global OUT
    args = sys.argv[1:]
    if "--out" in args:
        OUT = Path(args[args.index("--out") + 1]).resolve()
    only = args[args.index("--season") + 1] if "--season" in args else None
    pv = provider() if "--lede" in args else None
    cache = json.loads(LEDE_CACHE.read_text()) if LEDE_CACHE.exists() else {}
    t = load(DB)
    ss = t["season_summary"].copy()
    ss["n"] = ss["version_season"].str[2:].astype(int)
    ss = ss.sort_values("n")
    season_name = dict(zip(ss["version_season"], ss["season_name"]))
    names = dict(zip(t["castaways"]["castaway_id"], t["castaways"]["castaway"]))
    C = councils(t["vote_history"])
    S = game_state(t, C)
    OUT.mkdir(parents=True, exist_ok=True)

    # winners' confessional index at each episode, per season (the band excludes the season being shown)
    win = t["castaways"][t["castaways"]["result"] == "Sole Survivor"][["version_season", "castaway_id"]]
    wstate = S.merge(win, on=["version_season", "castaway_id"])
    H = History()
    listing = []
    if only == "latest":
        only = [v for v in ss["version_season"] if not t["episodes"][t["episodes"].version_season == v].empty][-1]
    for vs in ss["version_season"]:
        eps = t["episodes"][t["episodes"].version_season == vs].sort_values("episode")
        if eps.empty:
            continue
        cast = t["castaways"][t["castaways"].version_season == vs]
        complete = bool((cast["result"] == "Sole Survivor").any())
        listing.append({"vs": vs, "n": int(vs[2:]), "name": season_name.get(vs, vs), "episodes": int(eps["episode"].max()), "complete": complete})
        doc = {"vs": vs, "n": int(vs[2:]), "name": season_name.get(vs, vs), "complete": complete,
               "cast": [{"id": r.castaway_id, "name": r.castaway, "full": r.full_name, "tribe": r.original_tribe, "result": r.result,
                         "place": int(r.place) if str(r.place).isdigit() else None, "out": int(r.episode) if pd.notna(r.episode) else None}
                        for r in cast.sort_values("castaways_order").itertuples()],
               "winners_band": {}, "episodes": []}
        others = wstate[wstate.version_season != vs]
        for e in eps["episode"]:
            w = others[others.episode == e]["conf_index"].dropna()
            if len(w) >= 5:
                doc["winners_band"][int(e)] = [round(float(w.quantile(q)), 2) for q in (0.25, 0.5, 0.75)] + [int(len(w))]
        for er in eps.itertuples():
            e = int(er.episode)
            facts = card(vs, e, t, C, S, H, names)
            se = S[(S.version_season == vs) & (S.episode == e)]
            alive = sorted(se.loc[se.exit_episode > e, "castaway_id"])
            cf = t["confessionals"][(t["confessionals"].version_season == vs) & (t["confessionals"].episode == e)].set_index("castaway_id")["confessional_count"]
            doc["episodes"].append({
                "episode": e, "title": er.episode_title, "date": er.episode_date, "viewers": None if pd.isna(er.viewers) else float(er.viewers),
                "imdb": None if pd.isna(er.imdb_rating) else float(er.imdb_rating),
                "card": {"facts": facts, "lede": lede(str(er.episode_title), facts, cache, pv) if pv and vs == only else None},
                "edit": [{"id": r.castaway_id, "now": None if pd.isna(cf.get(r.castaway_id, np.nan)) else int(cf.get(r.castaway_id)),
                          "total": int(r.confessional_count), "index": None if pd.isna(r.conf_index) else round(float(r.conf_index), 2),
                          "in": bool(r.exit_episode >= e)}
                         for r in se.itertuples() if r.exit_episode >= e],
                "blocs": blocs(vs, e, C, alive),
                "comps": comps(vs, e, S, names, season_name),
            })
        if complete:                                   # a season's immunity totals join the record once it has ended
            H.imm_season.extend(int(x) for x in S[(S.version_season == vs)].groupby("castaway_id")["imm_wins"].max())
        if only is None or only == vs:
            (OUT / f"hub_{vs}.json").write_text(json.dumps(doc, separators=(",", ":"), default=str))
    if pv is not None:
        LEDE_CACHE.parent.mkdir(exist_ok=True)
        LEDE_CACHE.write_text(json.dumps(cache, indent=1))
    (OUT / "seasons.json").write_text(json.dumps({"seasons": listing, "latest": listing[-1]["vs"] if listing else None,
                                                  "features": COMP_FEATURES}, separators=(",", ":")))
    print(f"wrote {OUT}: {len(listing) if only is None else 1} season file(s), latest {listing[-1]['vs'] if listing else None}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
