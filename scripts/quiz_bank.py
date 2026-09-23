#!/usr/bin/env python3
"""
The daily quiz's question bank: every question is a SQL query against the warehouse, and its answer is what the query returns.

    uv run python scripts/quiz_bank.py                  # from Gamebot/
    uv run python scripts/quiz_bank.py --out DIR        # somewhere other than the live directory

Reads gamebot_lite/data/gamebot.sqlite (US seasons). Writes bank.json: a list of questions, each with its template,
the question, the right answer, three wrong answers drawn from the same season or the same kind of thing, the SQL
that answers it (written with the warehouse page's table names, so it runs there), and a one-line explanation. A question is kept only if its query returns exactly one answer, so a
tie never becomes a trick question. The page picks five a day from the date, one per template, the same five for
everyone, and a player's score shares as a line of text.
"""

from __future__ import annotations

import collections
import hashlib
import json
import random
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "gamebot_lite/data/gamebot.sqlite"
if "--db" in sys.argv:
    DB = Path(sys.argv[sys.argv.index("--db") + 1])
OUT = ROOT.parent / "preferencespace/static/survivor/quiz/data"
SUBS = ROOT / "data_cache/subtitles/subtitles.sqlite"   # scripts/subtitle_mine.py; local only, optional
ERRATA = {   # questions whose source row is wrong: left out until survivoR fixes it (checked 2026-09-23)
    "location:US23": "survivoR lists San Juan del Sur, Nicaragua; South Pacific was filmed on Upolu, Samoa",
    "location:US24": "survivoR lists San Juan del Sur, Nicaragua; One World was filmed on Upolu, Samoa",
    "quit:US14": "Gary left for a medical reason (an allergic reaction); fans call it a medical quit, so 'who quit' is arguable",
    "quit:US25": "Dana left for a medical reason; the same argument",
    "quit:US37": "Bi left with a foot injury; the same argument",
}


SILVER = {"vote_dynamics", "jury_analysis", "challenge_performance", "edit_features", "advantage_strategy",
          "castaway_profile", "season_context", "social_positioning"}
GOLD = {"ml_features_hybrid", "ml_features_non_edit"}


def warehouse(sql: str) -> str:
    """The same query with the warehouse page's table names (bronze.castaways, silver.vote_dynamics, gold.ml_features_hybrid),
    so a reader can run it there. The lite SQLite keeps every layer's tables side by side without the prefix."""
    layer = lambda t: "silver" if t in SILVER else "gold" if t in GOLD else "bronze"
    return re.sub(r"\b(from|join)\s+(\w+)", lambda m: f"{m.group(1)} {layer(m.group(2))}.{m.group(2)}", sql)


def rows(con, sql, *args):
    return con.execute(sql, args).fetchall()


def pick(pool, answer, k=3, seed=""):
    """k distinct wrong answers from pool, stable for a given seed."""
    rest = sorted({p for p in pool if p is not None and str(p) != str(answer)}, key=str)
    random.Random(hashlib.sha1(f"{seed}|{answer}".encode()).hexdigest()).shuffle(rest)
    return rest[:k]


def ordinal(n: int) -> str:
    return f"{n}{'th' if 11 <= n % 100 <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')}"


def near(n, k=3, seed="", floor=1):
    """k plausible wrong numbers near n, none below floor."""
    rng = random.Random(hashlib.sha1(f"{seed}|{n}".encode()).hexdigest())
    cands = [x for x in range(max(floor, n - 4), n + 5) if x != n]
    rng.shuffle(cands)
    return sorted(cands[:k])


def main() -> int:
    global OUT
    if "--out" in sys.argv:
        OUT = Path(sys.argv[sys.argv.index("--out") + 1]).resolve()
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    seasons = rows(con, "select version_season, season, season_name, location, winner_id, winner from season_summary "
                        "where version = 'US' and winner_id is not null order by season")
    locations = [r[3] for r in seasons]
    qs = []

    def add(template, sid, q, answer, wrong, sql, why, params=()):
        if f"{template}:{sid}" in ERRATA:
            return
        wrong = list(dict.fromkeys(str(w) for w in wrong if w is not None and str(w) != str(answer)))[:3]
        got = rows(con, sql, *params)
        if len(got) != 1 or str(got[0][0]) != str(answer) or len(wrong) < 3:
            return
        qs.append({"id": f"{template}:{sid}", "template": template, "q": q, "answer": str(answer), "wrong": [str(w) for w in wrong],
                   "sql": warehouse(sql.replace("?", "'{}'").format(*[str(p).replace("'", "''") for p in params]) if params else sql), "why": why})

    # Returning-player twists (Redemption Island, Edge of Extinction) make "first juror" ambiguous.
    RETURN_TWIST = {"US22", "US23", "US27", "US38", "US40"}
    # Fiji hosted every season from 33 on, so a location question there is a giveaway.
    FIJI_ERA = 33
    early_locations = [r[3] for r in seasons if r[1] < FIJI_ERA]

    def ranked(sql, *params):
        """[(name, value)] best first; used for the answer and for near misses as wrong choices."""
        return [(r[0], r[1]) for r in rows(con, sql, *params)]

    def near_misses(top, answer, fill, seed, k=3):
        """The next names down the same ranking make the hardest wrong choices; top up from fill."""
        out = [x for x, _ in top if x != answer][:k]
        return out + pick([f for f in fill if f not in out], answer, k=k - len(out), seed=seed) if len(out) < k else out

    for vs, n, name, loc, wid, winner in seasons:
        title = name.replace("Survivor: ", "")
        label = f"Survivor: {title}" if not title.isdigit() else f"Survivor {title}"
        cast = [r[0] for r in rows(con, "select castaway from castaways where version_season = ? order by place", vs)]
        jury = [r[0] for r in rows(con, "select castaway from castaways where version_season = ? and jury = 1 order by castaways_order", vs)]
        finalists = [r[0] for r in rows(con, "select castaway from castaways where version_season = ? and finalist = 1", vs)]
        fv = rows(con, "select final_vote from season_summary where version_season = ?", vs)
        fv = fv[0][0] if fv and fv[0][0] else ""
        fv_nums = [int(x) for x in re.findall(r"\d+", fv)]

        # the finalist who lost (two-finalist seasons), checked against season_summary.runner_ups
        if len(finalists) == 2:
            ru = rows(con, "select castaway, full_name from castaways where version_season = ? and finalist = 1 and winner = 0", vs)
            listed = rows(con, "select runner_ups from season_summary where version_season = ?", vs)
            if len(ru) == 1 and listed and listed[0][0] and ru[0][1] == listed[0][0].strip():
                add("runner_up", vs, f"Who lost to {winner} at the final tribal council of {label}?", ru[0][0],
                    pick(jury[-5:], ru[0][0], seed=vs + "r"),
                    "select castaway from castaways where version_season = ? and finalist = 1 and winner = 0",
                    f"{ru[0][0]} lost {fv}." if fv else f"{ru[0][0]} was the runner-up.", (vs,))

        # second place in a three-finalist vote, when it is not tied with third; checked against final_vote
        if len(finalists) == 3 and len(fv_nums) == 3 and fv_nums[1] > fv_nums[2]:
            tally = ranked("""select c.castaway, count(*) filter (where j.vote = '1.0') as n from jury_votes j
                              join castaways c on c.castaway_id = j.finalist_id and c.version_season = j.version_season
                              where j.version_season = ? group by c.castaway order by n desc""", vs)
            if len(tally) == 3 and [t[1] for t in tally] == fv_nums:
                second, third = tally[1][0], tally[2][0]
                add("second_place", vs, f"Who finished second in the jury vote on {label}?", second,
                    [winner, third] + pick(jury[-4:], second, k=1, seed=vs + "s"),
                    """select c.castaway from bronze_jury_votes j
join castaways c on c.castaway_id = j.finalist_id and c.version_season = j.version_season
where j.version_season = ?
group by c.castaway order by count(*) filter (where j.vote = '1.0') desc limit 1 offset 1""".replace("bronze_jury_votes", "jury_votes"),
                    f"{second} got {tally[1][1]} votes to {winner}'s {tally[0][1]}, {fv}.", (vs,))

        # the first juror, checked against boot_order
        if vs not in RETURN_TWIST and len(jury) >= 4:
            by_boot = rows(con, """select b.castaway from boot_order b join castaways c using (version_season, castaway_id)
                                   where b.version_season = ? and c.jury = 1 order by b.boot_order_position limit 1""", vs)
            if by_boot and by_boot[0][0] == jury[0]:
                add("first_juror", vs, f"Who was the first member of the jury on {label}?", jury[0], jury[1:4],
                    "select castaway from castaways where version_season = ? and jury = 1 order by castaways_order limit 1",
                    f"{jury[0]} was the first juror; {', '.join(jury[1:3])} followed.", (vs,))

        # first boot: the wrong choices are the next people out
        fb = rows(con, "select castaway from castaways where version_season = ? and castaways_order = 1", vs)
        if fb:
            early = [r[0] for r in rows(con, "select castaway from castaways where version_season = ? and castaways_order between 2 and 5 order by castaways_order", vs)]
            add("first_boot", vs, f"Who was the first person voted out of {label}?", fb[0][0], early[:3] if len(early) >= 3 else pick(cast, fb[0][0], seed=vs),
                "select castaway from castaways where version_season = ? and castaways_order = 1 and result like '%voted out%'",
                f"{fb[0][0]} went home first; {early[0]} was next." if early else f"{fb[0][0]} went home first.", (vs,))

        # where, before the Fiji era
        if n < FIJI_ERA:
            add("location", vs, f"Where was {label} filmed?", loc, pick(early_locations, loc, seed=vs),
                "select location from season_summary where version_season = ?", f"Season {n} was filmed in {loc}.", (vs,))

        # the winner's jury votes: bronze.jury_votes has a row per juror and finalist, and vote = '1.0' marks the
        # ballot. The count must match the first number of season_summary.final_vote, a second source.
        jv = rows(con, "select count(*) from jury_votes where version_season = ? and finalist_id = ? and vote = '1.0'", vs, wid)
        if jv and jv[0][0] > 0 and fv_nums and fv_nums[0] == jv[0][0]:
            k = jv[0][0]
            add("jury_votes", vs, f"How many jury votes did {winner} get to win {label}?", k, near(k, seed=vs),
                f"select count(*) from jury_votes where version_season = ? and finalist_id = '{wid}' and vote = '1.0'",
                f"{winner} won with {k} of the jury's votes, {fv}.", (vs,))

        # most votes cast against a castaway over the season, checked against silver.vote_dynamics
        mv = ranked("""select c.castaway, count(*) as n from vote_history v
                       join castaways c on c.castaway_id = v.vote_id and c.version_season = v.version_season
                       where v.version_season = ? group by c.castaway order by n desc limit 4""", vs)
        chk = ranked("""select c.castaway, count(*) as n from vote_dynamics v
                        join castaways c on c.castaway_id = v.target_id and c.version_season = v.version_season
                        where v.version_season = ? group by c.castaway order by n desc limit 2""", vs)
        if len(mv) >= 4 and mv[0][1] > mv[1][1] and chk and chk[0] == mv[0]:
            add("most_votes", vs, f"Who had the most votes cast against them over {label}?", mv[0][0], near_misses(mv, mv[0][0], cast, vs + "v"),
                """select c.castaway from vote_history v
join castaways c on c.castaway_id = v.vote_id and c.version_season = v.version_season
where v.version_season = ?
group by c.castaway order by count(*) desc limit 1""",
                f"{mv[0][0]} drew {mv[0][1]} votes; {mv[1][0]} was next with {mv[1][1]}.", (vs,))

        # most confessionals, checked against silver.edit_features
        cf = ranked("""select castaway, sum(confessional_count) as n from confessionals where version_season = ?
                       group by castaway_id, castaway order by n desc limit 4""", vs)
        chk = ranked("""select c.castaway, sum(e.confessional_count) as n from edit_features e join castaways c using (version_season, castaway_id)
                        where e.version_season = ? group by c.castaway order by n desc limit 1""", vs)
        if len(cf) >= 4 and cf[0][1] and cf[0][1] > cf[1][1] and chk and chk[0][0] == cf[0][0]:
            add("confessionals", vs, f"Who had the most confessionals over {label}?", cf[0][0], near_misses(cf, cf[0][0], cast, vs + "c"),
                """select castaway from confessionals where version_season = ?
group by castaway_id, castaway order by sum(confessional_count) desc limit 1""",
                f"{cf[0][0]} had {int(cf[0][1])}; {cf[1][0]} was next with {int(cf[1][1])}.", (vs,))

        # the first individual immunity, when one person won it; checked against silver.challenge_performance
        IMM = """challenge_type like '%Immunity%' and outcome_type like '%Individual%' and result like 'Won%' and result <> 'Won (reward only)'"""
        fi = rows(con, f"""select castaway from challenge_results where version_season = ? and {IMM}
                           and episode = (select min(episode) from challenge_results where version_season = ? and {IMM})""", vs, vs)
        chk = rows(con, """select c.castaway from challenge_performance p join castaways c using (version_season, castaway_id)
                           where p.version_season = ? and p.challenge_format = 'individual' and p.challenge_type like 'Immunity%'
                             and p.result in ('won', 'won (immunity only)')
                             and p.episode = (select min(episode) from challenge_performance where version_season = ? and challenge_format = 'individual'
                                              and challenge_type like 'Immunity%' and result in ('won', 'won (immunity only)'))""", vs, vs)
        if len(fi) == 1 and [r[0] for r in chk] == [fi[0][0]]:
            pool = [x for x in jury + finalists if x != fi[0][0]]
            add("first_immunity", vs, f"Who won the first individual immunity challenge of {label}?", fi[0][0], pick(pool, fi[0][0], seed=vs + "f"),
                f"""select castaway from challenge_results
where version_season = ? and {IMM}
  and episode = (select min(episode) from challenge_results where version_season = ? and {IMM})""",
                f"{fi[0][0]} won the first one.", (vs, vs))

        # most individual immunity wins: the wrong choices are the next-best
        top = ranked("""select castaway, count(*) as wins from challenge_results
                        where version_season = ? and challenge_type like '%Immunity%' and outcome_type like '%Individual%'
                          and result like 'Won%' and result <> 'Won (reward only)'
                        group by castaway_id, castaway order by wins desc limit 4""", vs)
        if top and top[0][1] >= 2 and (len(top) == 1 or top[1][1] < top[0][1]):
            add("immunity", vs, f"Who won the most individual immunity challenges on {label}?", top[0][0], near_misses(top, top[0][0], jury or cast, vs),
                """select castaway from challenge_results
where version_season = ? and challenge_type like '%Immunity%' and outcome_type like '%Individual%'
  and result like 'Won%' and result <> 'Won (reward only)'
group by castaway_id, castaway order by count(*) desc limit 1""",
                f"{top[0][0]} won {top[0][1]}" + (f"; {top[1][0]} won {top[1][1]}." if len(top) > 1 else "."), (vs,))

        # the season's only quitter (someone who quit the Edge of Extinction after being voted out does not count)
        quit_ = rows(con, "select castaway from castaways where version_season = ? and result like '%Quit%'", vs)
        if len(quit_) == 1 and rows(con, "select result from castaways where version_season = ? and castaway = ?", vs, quit_[0][0])[0][0] == "Quit":
            add("quit", vs, f"Who quit {label}?", quit_[0][0], pick(cast, quit_[0][0], seed=vs + "q"),
                "select castaway from castaways where version_season = ? and result like '%Quit%'",
                f"{quit_[0][0]} was the only castaway to quit season {n}.", (vs,))

        # most idols found: the wrong choices are the other finders
        idol = ranked("""select castaway, count(*) as found from advantage_movement m
                         join advantage_details d using (version_season, advantage_id)
                         where m.version_season = ? and d.advantage_type = 'Hidden Immunity Idol' and m.event in ('Found', 'Found (beware)')
                         group by castaway_id, castaway order by found desc limit 4""", vs)
        if idol and idol[0][1] >= 2 and (len(idol) == 1 or idol[1][1] < idol[0][1]):
            add("idols", vs, f"Who found the most hidden immunity idols on {label}?", idol[0][0], near_misses(idol, idol[0][0], jury or cast, vs + "i"),
                """select castaway from advantage_movement m
join advantage_details d using (version_season, advantage_id)
where m.version_season = ? and d.advantage_type = 'Hidden Immunity Idol' and m.event in ('Found', 'Found (beware)')
group by castaway_id, castaway order by count(*) desc limit 1""",
                f"{idol[0][0]} found {idol[0][1]}.", (vs,))

        # ---- from every layer: episodes, tribes, the auction, jobs and hometowns (bronze); ballots and the
        # jury (silver, checked against bronze or gold); puzzles (silver, checked against bronze) --------------
        one_timers = {r[0] for r in rows(con, "select castaway_id from castaways where version = 'US' group by castaway_id having count(*) = 1")}
        people = rows(con, """select c.castaway, c.castaway_id, c.city, c.state, d.occupation, c.castaways_order, c.result, c.jury
                              from castaways c join castaway_details d using (castaway_id) where c.version_season = ?""", vs)
        rng = random.Random(vs)

        # the best-rated episode, with the next-best as wrong choices
        eps = rows(con, "select episode_title, imdb_rating from episodes where version_season = ? and imdb_rating is not null order by imdb_rating desc", vs)
        if len(eps) >= 4 and eps[0][1] > eps[1][1] and len({e[0] for e in eps}) == len(eps):
            add("best_episode", vs, f"Which episode of {label} has the highest IMDb rating?", eps[0][0], [e[0] for e in eps[1:4]],
                "select episode_title from episodes where version_season = ? order by imdb_rating desc limit 1",
                f"“{eps[0][0]}” is rated {eps[0][1]}; “{eps[1][0]}” is next at {eps[1][1]}.", (vs,))

        # an episode title, to place in its season
        GENERIC = re.compile(r"finale|reunion|recap|premiere|episode|part \d", re.I)
        for t, _ in eps + rows(con, "select episode_title, 0 from episodes where version_season = ? and imdb_rating is null", vs):
            if not t or GENERIC.search(t) or len(t) < 10 or rows(con, "select count(*) from episodes where version = 'US' and episode_title = ?", t)[0][0] != 1:
                continue
            near_seasons = [r[2] for r in seasons if r[1] != n and abs(r[1] - n) <= 4]
            add("episode_title", vs, f"Which season had an episode titled “{t}”?", name, pick(near_seasons, name, seed=vs + "t"),
                "select s.season_name from episodes e join season_summary s using (version_season) where e.version = 'US' and e.episode_title = ?",
                f"“{t}” aired on {label}.", (t,))
            break

        # the merged tribe's name, against this season's other tribe names and nearby seasons' merged names
        merged = [r[0] for r in rows(con, "select distinct tribe from tribe_mapping where version_season = ? and tribe_status = 'Merged' and tribe not in ('Merged', 'Mergatory')", vs)]
        if len(merged) == 1:
            others = [r[0] for r in rows(con, "select distinct tribe from tribe_mapping where version_season = ? and tribe_status in ('Original', 'Swapped', 'Swapped_2')", vs)]
            near_merged = [r[0] for r in rows(con, """select distinct tribe from tribe_mapping where version = 'US' and tribe_status = 'Merged'
                                                      and tribe not in ('Merged', 'Mergatory') and abs(season - ?) between 1 and 4""", n)]
            wrong = pick(others, merged[0], k=2, seed=vs + "m") + pick(near_merged, merged[0], k=3, seed=vs + "mm")
            add("merged_tribe", vs, f"What was the merged tribe called on {label}?", merged[0], wrong,
                "select distinct tribe from tribe_mapping where version_season = ? and tribe_status = 'Merged' and tribe not in ('Merged', 'Mergatory')",
                f"The merged tribe was {merged[0]}.", (vs,))

        # the auction: who paid the most for one item
        lots = rows(con, "select castaway, item_description, cost from auction_details where version_season = ? and cost is not null and castaway is not null order by cost desc", vs)
        if len(lots) >= 4 and lots[0][2] > lots[1][2]:
            bidders = list(dict.fromkeys(l[0] for l in lots))
            add("auction", vs, f"Who paid the most for a single item at the {label} auction?", lots[0][0], pick(bidders, lots[0][0], seed=vs + "a"),
                "select castaway from auction_details where version_season = ? and cost is not null order by cost desc limit 1",
                f"{lots[0][0]} paid ${int(lots[0][2])} for {str(lots[0][1]).lower()}.", (vs,))

        # a job and a hometown, each unique in the season, for someone who played once (castaway_details keeps one job per person)
        single = [p for p in people if p[1] in one_timers]
        jobs = [p for p in single if p[4] and sum(1 for x in people if x[4] == p[4]) == 1 and 4 <= len(p[4]) <= 40]
        if jobs:
            p = rng.choice(jobs)
            add("occupation", vs, f"Which {label} castaway listed their job as “{p[4]}”?", p[0], pick([x[0] for x in people], p[0], seed=vs + "o"),
                "select c.castaway from castaways c join castaway_details d using (castaway_id) where c.version_season = ? and d.occupation = ?",
                f"{p[0]} listed their job as {p[4]}.", (vs, p[4]))
        towns = [p for p in people if p[2] and sum(1 for x in people if (x[2], x[3]) == (p[2], p[3])) == 1]
        if towns:
            p = rng.choice(towns)
            add("hometown", vs, f"Which {label} castaway came from {p[2]}, {p[3]}?", p[0], pick([x[0] for x in people], p[0], seed=vs + "h"),
                "select castaway from castaways where version_season = ? and city = ? and state = ?",
                f"{p[0]} came from {p[2]}, {p[3]}.", (vs, p[2], p[3]))

        # odd one out: the last person out before the jury, among three jurors
        if vs not in RETURN_TWIST and len(jury) >= 3:
            j0 = rows(con, "select min(castaways_order) from castaways where version_season = ? and jury = 1", vs)[0][0]
            pre = rows(con, "select castaway, result from castaways where version_season = ? and castaways_order = ?", vs, (j0 or 0) - 1)
            if len(pre) == 1 and "voted out" in str(pre[0][1]):
                add("not_jury", vs, f"Which of these {label} castaways was not on the jury?", pre[0][0], jury[:3],
                    "select castaway from castaways where version_season = ? and castaways_order = (select min(castaways_order) from castaways where version_season = ? and jury = 1) - 1",
                    f"{pre[0][0]} went home one vote before the jury began; {jury[0]} was its first member.", (vs, vs))

        # boot order: of four castaways, who went first
        mid = [p for p in people if p[5] and "voted out" in str(p[6])]
        mid.sort(key=lambda p: p[5])
        # Pearl Islands' Outcasts and the Redemption Island and Edge of Extinction seasons sent people home twice
        if vs not in RETURN_TWIST | {"US07"} and len(mid) >= 10 and len({p[0] for p in mid}) == len(mid):
            four = sorted(rng.sample(mid[2:-1], 4), key=lambda p: p[5])
            if len({p[5] for p in four}) == 4:
                names4 = [p[0] for p in four]
                add("boot_order", vs, f"Of {', '.join(sorted(names4)[:3])} and {sorted(names4)[3]} on {label}, who was voted out first?", names4[0], names4[1:],
                    "select castaway from castaways where version_season = ? and castaway in (?, ?, ?, ?) order by castaways_order limit 1",
                    f"{names4[0]} went {ordinal(four[0][5])}; then {names4[1]}, {names4[2]} and {names4[3]}.", (vs, *names4))

        # the last boot before the merge (seasons with a clean merge), checked against the ballots' tribe status
        statuses = {r[0] for r in rows(con, "select distinct tribe_status from tribe_mapping where version_season = ?", vs)}
        if vs not in RETURN_TWIST and "Mergatory" not in statuses and "Merged" in statuses:
            merge_ep = rows(con, "select min(episode) from tribe_mapping where version_season = ? and tribe_status = 'Merged'", vs)[0][0]
            lp = rows(con, """select castaway from castaways where version_season = ? and episode < ? and result like '%voted out%'
                              order by castaways_order desc limit 1""", vs, merge_ep)
            chk = rows(con, """select c.castaway from vote_dynamics v join castaways c on c.castaway_id = v.voted_out_id and c.version_season = v.version_season
                               where v.version_season = ? and v.tribe_status <> 'Merged' order by c.castaways_order desc limit 1""", vs)
            if lp and chk and lp[0][0] == chk[0][0]:
                before = [r[0] for r in rows(con, "select castaway from castaways where version_season = ? and castaways_order between ? and ?",
                                             vs, *(lambda o: (o - 3, o + 3))(rows(con, "select castaways_order from castaways where version_season = ? and castaway = ?", vs, lp[0][0])[0][0]))]
                add("last_premerge", vs, f"Who was the last person voted out before the merge on {label}?", lp[0][0], pick(before, lp[0][0], seed=vs + "p"),
                    "select castaway from castaways where version_season = ? and result like '%voted out%' and episode < (select min(episode) from tribe_mapping where version_season = ? and tribe_status = 'Merged') order by castaways_order desc limit 1",
                    f"{lp[0][0]} was the last one out before the tribes merged.", (vs, vs))

        # silver: most votes cast on the losing side, checked against the raw ballots
        ls = ranked("""select c.castaway, sum(v.vote_incorrect) as n from vote_dynamics v join castaways c using (version_season, castaway_id)
                       where v.version_season = ? group by c.castaway order by n desc limit 4""", vs)
        raw = ranked("""select c.castaway, count(*) as n from vote_history h join castaways c using (version_season, castaway_id)
                        where h.version_season = ? and h.voted_out_id is not null and h.vote_id is not null and h.vote_id <> h.voted_out_id
                        group by c.castaway order by n desc limit 1""", vs)
        if len(ls) >= 4 and ls[0][1] and ls[0][1] >= 3 and ls[0][1] > ls[1][1] and raw and raw[0][0] == ls[0][0]:
            add("losing_side", vs, f"Who cast the most votes on the losing side over {label}?", ls[0][0], near_misses(ls, ls[0][0], cast, vs + "l"),
                """select c.castaway from vote_dynamics v join castaways c using (version_season, castaway_id)
where v.version_season = ? group by c.castaway order by sum(v.vote_incorrect) desc limit 1""",
                f"{ls[0][0]} voted for someone who stayed {int(ls[0][1])} times; {ls[1][0]} did {int(ls[1][1])}.", (vs,))

        # silver and gold: the winner's jury votes from their own original tribe
        jt = rows(con, """select count(*) from jury_analysis where version_season = ? and finalist_id = ? and voted_for_winner = 1 and same_original_tribe = 1""", vs, wid)
        gt = rows(con, "select jury_votes_from_original_tribe from ml_features_hybrid where version_season = ? and castaway_id = ?", vs, wid)
        if jt and gt and gt[0][0] is not None and jt[0][0] == gt[0][0] and fv_nums and fv_nums[0] >= 3:
            k = jt[0][0]
            add("jury_tribe", vs, f"How many of {winner}'s jury votes on {label} came from their own original tribe?", k, near(k, seed=vs + "j", floor=0),
                f"select count(*) from jury_analysis where version_season = ? and finalist_id = '{wid}' and voted_for_winner = 1 and same_original_tribe = 1",
                f"{k} of {winner}'s {fv_nums[0]} votes came from original tribemates.", (vs,))

        # silver: most individual challenge wins with a puzzle in them, checked against bronze challenge_description
        pz = ranked("""select c.castaway, sum(p.puzzle_win) as n from challenge_performance p join castaways c using (version_season, castaway_id)
                       where p.version_season = ? and p.challenge_format = 'individual' group by c.castaway order by n desc limit 4""", vs)
        chk = ranked("""select r.castaway, count(*) as n from challenge_results r
                        join challenge_description d on d.version_season = r.version_season and d.challenge_id = r.challenge_id
                        where r.version_season = ? and r.outcome_type like '%Individual%' and r.result like 'Won%' and d.puzzle = 1
                        group by r.castaway_id, r.castaway order by n desc limit 1""", vs)
        if len(pz) >= 4 and pz[0][1] and pz[0][1] >= 2 and pz[0][1] > pz[1][1] and chk and chk[0][0] == pz[0][0]:
            add("puzzles", vs, f"Who won the most individual challenges with a puzzle in them on {label}?", pz[0][0], near_misses(pz, pz[0][0], jury or cast, vs + "z"),
                """select c.castaway from challenge_performance p join castaways c using (version_season, castaway_id)
where p.version_season = ? and p.challenge_format = 'individual' group by c.castaway order by sum(p.puzzle_win) desc limit 1""",
                f"{pz[0][0]} won {int(pz[0][1])} of them.", (vs,))

    # returning players: seasons played and first season
    for cid, who, k, first in rows(con, """select castaway_id, full_name, count(*), min(season) from castaways
                                            where version = 'US' group by castaway_id having count(*) >= 2"""):
        if k < 3:                          # two-timers are most of them and make a dull question; their debut still counts
            pass
        else:
            add("seasons_played", cid, f"How many seasons of US Survivor has {who} played?", k, near(k, seed=cid, floor=2),
                f"select count(*) from castaways where version = 'US' and castaway_id = '{cid}'", f"{who} has played {k} seasons.")
        fname = next(r[2] for r in seasons if r[1] == first) if any(r[1] == first for r in seasons) else None
        if fname:
            others = [r[2] for r in seasons if abs(r[1] - first) <= 8]
            add("debut", cid, f"On which season did {who} first play?", fname, pick(others, fname, seed=cid),
                f"select season_name from season_summary where version = 'US' and season = (select min(season) from castaways where version = 'US' and castaway_id = '{cid}')",
                f"{who} debuted on {fname}.")

    # ---- who said that: lines the captioner attributed by name (S1-2, S5-9, S40-50), from subtitle_mine.py.
    # Kept only when they read like one person talking about the game: 10 to 25 words, no name that gives the
    # speaker away, no question (a question is half a conversation), at most one filler word, first person, and
    # either game talk or mostly off-camera voice-over (a confessional). Wrong choices are the speaker's tribemates
    # that episode, so the tribe is no clue. Ten a season, two a speaker, best first.
    if SUBS.exists():
        subs = sqlite3.connect(f"file:{SUBS}?mode=ro&immutable=1", uri=True)
        FILL = {"yeah", "oh", "okay", "ok", "um", "uh", "mm", "hmm", "mm-hmm", "wow", "huh", "ah"}
        GAME = re.compile(r"\b(vote|votes|voted|voting|alliance|idol|jury|merge|million|game|blindside|target|trust|win|"
                          r"strategy|numbers|tribal|immunity|advantage|swing)\b", re.I)
        FIRST = re.compile(r"\b(I|I'm|I've|I'll|me|my|we|we're|our)\b")
        by_season = collections.defaultdict(list)
        for vs, ep, start, sid, who, text, words, ncues, ital in subs.execute(
                """select version_season, episode, start_s, speaker_id, speaker, text, n_words, n_cues, italic_share from quotes
                   where speaker_id is not null and n_words between 10 and 25 and names_anyone = 0 and clean_start and clean_end and n_cues <= 3"""):
            w = re.findall(r"[a-z'-]+", text.lower())
            if re.search(r"(\.\.\.|--|…)\W*$", text):         # trails off: the speaker was cut off or the line runs on
                continue
            if "?" in text or sum(x in FILL for x in w) > 1 or not FIRST.search(text) or not (GAME.search(text) or ital >= 0.5):
                continue
            score = 2 * (ital >= 0.8) + (ital >= 0.5) + bool(GAME.search(text)) + (ncues <= 2)
            by_season[vs].append((score, start, ep, sid, who, text, ital))
        labels = {r[0]: (f"Survivor: {r[2].replace('Survivor: ', '')}" if not r[2].replace("Survivor: ", "").isdigit() else f"Survivor {r[2].replace('Survivor: ', '')}") for r in seasons}
        for vs, cands in by_season.items():
            per_speaker, kept = collections.Counter(), 0
            for score, start, ep, sid, who, text, ital in sorted(cands, key=lambda c: (-c[0], hashlib.sha1(c[5].encode()).hexdigest())):
                if kept >= 10 or per_speaker[sid] >= 2:
                    continue
                mates = [r[0] for r in rows(con, """select distinct t2.castaway from tribe_mapping t1 join tribe_mapping t2
                                                    on t2.version_season = t1.version_season and t2.episode = t1.episode and t2.tribe = t1.tribe
                                                    where t1.version_season = ? and t1.episode = ? and t1.castaway_id = ? and t2.castaway_id <> ?""", vs, ep, sid, sid)]
                if len(set(mates)) < 3:
                    mates += [r[0] for r in rows(con, "select distinct castaway from boot_mapping where version_season = ? and episode = ? and castaway_id <> ?", vs, ep, sid)]
                mates = [m for m in mates if not re.search(r"(?<![A-Za-z])" + re.escape(m.split(" ")[0].strip(".")) + r"(?![A-Za-z])", text)]
                wrong = pick(mates, who, seed=text)          # nobody the line talks about is offered as its speaker
                if len(wrong) < 3 or who in wrong:
                    continue
                qid = f"who_said:{vs}:{hashlib.sha1(text.encode()).hexdigest()[:8]}"
                if qid in ERRATA:
                    continue
                qs.append({"id": qid, "template": "who_said", "q": f"Who said this on {labels.get(vs, vs)}, episode {ep}? “{text}”",
                           "answer": who, "wrong": wrong, "sql": None,
                           "source": f"The episode's subtitles name the speaker ({int(start // 60)}:{int(start % 60):02d} into episode {ep}).",
                           "why": f"{who} said it{' in a confessional' if ital >= 0.8 else ''}."})
                per_speaker[sid] += 1
                kept += 1
        subs.close()

    # ---- firsts: when something first happened on the US show. Each is the earliest season a query finds; the
    # list keeps only firsts fans agree on (a coding quirk would make "the first idol played" arguable) ------------
    FIRSTS = [
        ("the first rock draw", "select min(season) from vote_history where version = 'US' and vote_event = 'Rock draw'"),
        ("the first tribe swap", "select min(season) from tribe_mapping where version = 'US' and tribe_status = 'Swapped'"),
        ("the first final tribal council with three finalists", "select min(season) from season_summary where version = 'US' and n_finalists = 3"),
        ("the first tie broken by previous votes", "select min(season) from vote_history where version = 'US' and vote_event = 'Countback'"),
        ("the first kidnapping", "select min(season) from vote_history where version = 'US' and vote_event = 'Kidnapped'"),
        ("the first steal-a-vote", "select min(season) from vote_history where version = 'US' and vote_event like 'Steal a vote%'"),
        ("the first fire-making challenge at final four", "select min(season) from vote_history where version = 'US' and vote_event = 'Fire challenge (f4)'"),
        ("the first shot in the dark", "select min(season) from vote_history where version = 'US' and vote_event = 'Shot in the dark'"),
        ("the first journey", "select min(season) from journeys where version = 'US'"),
    ]
    by_n = {r[1]: r[2] for r in seasons}
    for what, sql in FIRSTS:
        k = rows(con, sql)[0][0]
        if k in by_n:
            near_names = [by_n[x] for x in (k - 3, k - 2, k - 1, k + 1, k + 2, k + 3) if x in by_n]
            add("firsts", re.sub(r"\W+", "_", what), f"Which season had {what}?", by_n[k], pick(near_names, by_n[k], seed=what),
                sql.replace("select min(season)", "select season_name from season_summary where version = 'US' and season = (select min(season)", 1) + ")",
                f"{what[0].upper() + what[1:]} was on {by_n[k]}, season {k}.")
    # the first unanimous jury vote: every finalist but the winner got nothing (Earl's 9-0-0, not J.T.'s later 7-0)
    unan = next((r for r in rows(con, "select season, season_name, final_vote from season_summary where version = 'US' and final_vote is not null order by season")
                 if len(re.findall(r"\d+", r[2])) >= 2 and all(int(x) == 0 for x in re.findall(r"\d+", r[2])[1:])), None)
    if unan:
        near_names = [by_n[x] for x in (unan[0] - 2, unan[0] - 1, unan[0] + 1, unan[0] + 2, unan[0] + 3) if x in by_n]
        add("firsts", "unanimous", "Which season had the first unanimous jury vote?", unan[1], pick(near_names, unan[1], seed="unan"),
            f"select season_name from season_summary where version = 'US' and season = {unan[0]}",
            f"The first unanimous vote was {unan[2]}, on {unan[1]}.")
    # first people: the first to quit, the first medical evacuation, the first voted out holding an idol
    for what, sql, pool_sql in [
        ("the first castaway to quit US Survivor", "select castaway from castaways where version = 'US' and result = 'Quit' order by season limit 1",
         "select castaway from castaways where version = 'US' and result like '%Quit%'"),
        ("the first castaway medically evacuated from US Survivor", "select castaway from castaways where version = 'US' and result = 'Medically evacuated' order by season limit 1",
         "select castaway from castaways where version = 'US' and result = 'Medically evacuated'"),
        ("the first castaway voted out holding a hidden immunity idol", """select m.castaway from advantage_movement m join advantage_details d using (version_season, advantage_id)
where m.version = 'US' and m.event = 'Voted out with advantage' and d.advantage_type = 'Hidden Immunity Idol' order by m.season limit 1""",
         """select m.castaway from advantage_movement m join advantage_details d using (version_season, advantage_id)
where m.version = 'US' and m.event = 'Voted out with advantage' and d.advantage_type = 'Hidden Immunity Idol'"""),
    ]:
        who = rows(con, sql)
        if who:
            add("firsts", re.sub(r"\W+", "_", what), f"Who was {what}?", who[0][0], pick([r[0] for r in rows(con, pool_sql)], who[0][0], seed=what),
                sql, f"{who[0][0]} was {what}.")

    # ---- this week, years ago: a question for each episode, keyed by its air date, so the page can ask about
    # episodes that aired in the same week of the year. Who went home, or who won individual immunity. -----------
    history = []
    months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    for vs, n, name, *_ in seasons:
        title = name.replace("Survivor: ", "")
        label = f"Survivor: {title}" if not title.isdigit() else f"Survivor {title}"
        for ep, et, day in rows(con, "select episode, episode_title, episode_date from episodes where version_season = ? and episode_date is not null", vs):
            y, m, d = (int(x) for x in day[:10].split("-"))
            when = f"{months[m - 1]} {d}, {y}"
            base = {"md": f"{m:02d}-{d:02d}", "year": y}
            # who was voted out, when one person was, and the others at that council are the wrong choices
            boot = rows(con, "select castaway, castaway_id from castaways where version_season = ? and episode = ? and result like '%voted out%'", vs, ep)
            ballots = rows(con, "select distinct voted_out_id from vote_history where version_season = ? and episode = ? and voted_out_id is not null", vs, ep)
            if len(boot) == 1 and [b[0] for b in ballots] == [boot[0][1]]:
                present = [r[0] for r in rows(con, """select distinct c.castaway from vote_history v join castaways c using (version_season, castaway_id)
                                                      where v.version_season = ? and v.episode = ? and c.castaway_id <> ?""", vs, ep, boot[0][1])]
                before = len(qs)
                add("history_boot", f"{vs}e{ep}", f"On {when}, {label} aired “{et}”. Who was voted out?", boot[0][0], pick(present, boot[0][0], seed=f"{vs}{ep}"),
                    "select castaway from castaways where version_season = ? and episode = ? and result like '%voted out%'",
                    f"{boot[0][0]} went home in episode {ep}.", (vs, ep))
                if len(qs) > before:
                    history.append({**qs.pop(), **base})
            # who won individual immunity, when one person did, against the others in that challenge
            imm = rows(con, """select castaway, challenge_id from challenge_results where version_season = ? and episode = ?
                               and challenge_type like '%Immunity%' and outcome_type like '%Individual%' and result like 'Won%' and result <> 'Won (reward only)'""", vs, ep)
            if len(imm) == 1:
                rivals = [r[0] for r in rows(con, "select distinct castaway from challenge_results where version_season = ? and challenge_id = ? and castaway <> ?",
                                             vs, imm[0][1], imm[0][0])]
                before = len(qs)
                add("history_immunity", f"{vs}e{ep}", f"On {when}, {label} aired “{et}”. Who won individual immunity?", imm[0][0], pick(rivals, imm[0][0], seed=f"i{vs}{ep}"),
                    """select castaway from challenge_results where version_season = ? and episode = ?
  and challenge_type like '%Immunity%' and outcome_type like '%Individual%' and result like 'Won%' and result <> 'Won (reward only)'""",
                    f"{imm[0][0]} won it in episode {ep}.", (vs, ep))
                if len(qs) > before:
                    history.append({**qs.pop(), **base})

    con.close()
    OUT.mkdir(parents=True, exist_ok=True)
    by = {}
    for q in qs:
        by[q["template"]] = by.get(q["template"], 0) + 1
    (OUT / "bank.json").write_text(json.dumps({"questions": qs, "history": history, "templates": by, "errata": ERRATA}, separators=(",", ":")))
    print(f"wrote {OUT / 'bank.json'}: {len(qs)} questions and {len(history)} this-week-in-history questions, {by}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
