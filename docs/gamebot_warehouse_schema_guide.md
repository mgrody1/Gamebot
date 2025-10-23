# Survivor Warehouse: Plain‑English Guide to the Schema

_Last updated: 2025-10-23 18:02:23_

This warehouse follows a **Medallion** design:

- **Bronze** = raw but relational copies of the open-source `survivoR` datasets + ingestion metadata
- **Silver** = curated **dimensions** and **facts** with stable keys and analytics-friendly names
- **Gold** = JSON feature snapshots ready for ML (castaway × season / episode / whole season)

Below is a practical, plain-English map of what’s in each layer, how tables relate, and how to join them safely.
If you’re an analyst, start with **Silver**. If you’re building features or training models, look at **Gold**.

---

## Key ID Concepts (read this first)

- **`version_season`**: A canonical season identifier (e.g., `US43`). It ties everything in a given season together.
- **`castaway_id`**: Stable ID for a person across seasons. Use it to connect a person’s master profile to season episodes, votes, etc.
- **`challenge_id`**: Identifier for a challenge **within a season** (pair it with `version_season`).
- **`advantage_id`**: Identifier for an advantage **within a season** (pair it with `version_season`).
- **`episode` / `episode_in_season`**: Episode number within the season. In silver, the column is `episode_in_season` for clarity.
- **`sog_id`**: “State-of-game” marker used by the upstream dataset to align challenge/vote events with episode context.
- **Surrogate keys in silver** (e.g., `*_key` columns) are warehouse-generated integer keys for fast joins.
  Keep both around: the natural IDs (`castaway_id`, `version_season`, etc.) for readability and the surrogate keys for performance.

---

## Bronze (raw, schema-on-write)

**What it is:** One table per upstream dataset, plus a run-log. Each row mirrors source data with minimal massaging.
Use Bronze for provenance and completeness; prefer **Silver** for analysis.

### Ingestion metadata

- **`bronze.ingestion_runs`** — One row per load. Includes run UUID, environment, git branch/commit, source URL, start/finish times, status, and notes.
  - Useful for figuring out “how fresh” the warehouse is and for tying Gold snapshots back to a specific load.

### People and seasons

- **`bronze.castaway_details`** — One row per **person**. Names, demographics (gender, race/ethnicity flags, LGBT), occupation, personality type, hobbies, and other bio text.
  - PK: `castaway_id`.
- **`bronze.season_summary`** — One row per **season**. Names, numbering, location/country, cast/tribe counts, finalists/jury counts, premiere/ending/filming dates, winner info, and viewership stats.
  - PK: `version_season`. FK to winner in `castaway_details` via `winner_id`.

### Season participation

- **`bronze.castaways`** — One row per **person × season**. Age/city/state on entry, episode/day of exit, placement, jury/finalist/winner flags, original tribe, and some acknowledgement (on-screen) fields.
  - Uniqueness: (`castaway_id`, `version_season`). FK to season and castaway.
- **`bronze.episodes`** — One row per **season × episode**. Titles, air date, length, viewers, IMDb ratings, and summaries.
  - PK: (`version_season`, `episode`).

### Events & outcomes

- **`bronze.challenge_description`** — Challenge catalog for a season: type, names, reward, description, and boolean flags for the skill taxonomy (balance, endurance, puzzle, water, etc.).
  - PK: (`version_season`, `challenge_id`).
- **`bronze.challenge_results`** — Who participated and how they did in each challenge: outcome, team, sit-outs, order of finish, reward choice, and links to the challenge definition and episode context.
  - Uniqueness: (`castaway_id`, `challenge_id`, `sog_id`, `version_season`).
- **`bronze.advantage_details`** — Advantage inventory for a season: type, clue text, where it was found, and any conditions.
  - PK: (`version_season`, `advantage_id`).
- **`bronze.advantage_movement`** — The lifecycle of each advantage: who held it when, passes, plays, targets, outcomes, and whether votes were nullified.
  - Uniqueness: (`sequence_id`, `version_season`, `advantage_id`).
- **`bronze.vote_history`** — Round-by-round votes: who voted, who they targeted, tie/split/nullified flags, immunity, and links to relevant challenges.
- **`bronze.jury_votes`** — Final Tribal Council votes: juror → finalist.
- **`bronze.boot_mapping`** — Episode-level mapping of who left (or number of boots if multiple) with tribe/game status context.
- **`bronze.tribe_mapping`** — Day-by-day membership: which tribe a castaway was on, and tribe status if applicable.
- **`bronze.confessionals`** — For each castaway × episode: count and total time of confessionals, plus expected values (from the upstream methodology).

**Indexes** are provided on common join keys (`version_season`, episodes/challenges/advantages) and on `ingest_run_id` to trace data back to a specific load.

---

## Silver (curated — start here for analysis)

**What it is:** Clean dimensions with friendly names and one true “grain” (what one row represents), plus facts that point to those dimensions.
Silver tables introduce **surrogate keys** (`*_key`) for fast joins and keep the natural IDs for readability.

### Dimensions (who / what / when)

- **`silver.dim_castaway`** — One row per **person**. Cleaned name, gender, DOB/DOD, collar, occupation, personality type, race/ethnicity, and boolean identity flags.
  - Grain: 1 row per `castaway_id`. Key: `castaway_key`.
- **`silver.dim_season`** — One row per **season**. Version, name, season number, geography, counts (cast, tribes, finalists, jury), key dates, winner, and viewership.
  - Grain: 1 row per `version_season`. Key: `season_key`.
- **`silver.dim_episode`** — One row per **season × episode**. Episode title/label/date/length and viewers/IMDb/rating counts.
  - Grain: 1 row per (`version_season`, `episode_in_season`). Keys: `episode_key`, FK to `season_key`.
- **`silver.dim_advantage`** — One row per **season × advantage** with canonical attributes (type, where found, conditions).
  - Grain: 1 row per (`version_season`, `advantage_id`). Key: `advantage_key`.
- **`silver.dim_challenge`** — One row per **season × challenge** with names, type, description, reward/stipulations, and (via lookups) skill taxonomy.
  - Grain: 1 row per (`version_season`, `challenge_id`). Key: `challenge_key`.
- **`silver.challenge_skill_lookup`** — Lookup of challenge skills (e.g., “balance”, “puzzle”, “water”) and optional category.
- **`silver.challenge_skill_bridge`** — Bridge table mapping a given `challenge_key` to one or more `skill_key` rows.

### Bridges (linking people to seasons)

- **`silver.bridge_castaway_season`** — One row per **person × season**, with outcome flags (jury/finalist/winner), placement, original tribe, and the acknowledgement (on-screen) fields.
  - Grain: 1 row per (`castaway_key`, `season_key`). Think of this as the roster for a season with results.

### Facts (events over time)

Each fact table carries the natural IDs for clarity **and** the surrogate keys for performance. The usual join path is:
`fact → dim_episode (via episode_key) → dim_season (via season_key)` and `fact → dim_castaway` (and others as needed).

- **`silver.fact_confessionals`** — Counts and seconds of confessionals per **castaway × season × episode** (+ expected values).
  - Grain: 1 row per (`castaway_key`, `episode_key`). Links back to the bronze source ID for traceability.
- **`silver.fact_challenge_results`** — Individual/tribal performance per **castaway × challenge** (+ sit-outs, order of finish, chosen for reward, etc.). Optional crosswalk to the relevant advantage.
  - Grain: 1 row per (`castaway_key`, `challenge_key`, `sog_id`).
- **`silver.fact_vote_history`** — Voting actions per **castaway × episode**: who they targeted, who was eliminated, immunity flags, split/tie/nullified, and `vote_order` within the round.
  - Grain: 1 row per voting action; carries both `castaway_key` (the voter) and the target/eliminated natural IDs.
- **`silver.fact_advantage_movement`** — Advantage lifecycle events (found, transferred, played) with outcomes and any votes nullified.
  - Grain: 1 row per (`version_season`, `advantage_id`, `sequence_id`), with keys to castaway/target, season/episode, and the advantage itself.
- **`silver.fact_boot_mapping`** — Episode-level elimination context (including multi-boot episodes) with tribe/game status at the time.
  - Grain: typically 1 row per boot event per episode (nullable `castaway_key` when event is aggregate-only).
- **`silver.fact_tribe_membership`** — Day-by-day tribe membership per castaway with episode alignment.
  - Grain: 1 row per (`castaway`, `day`) within a season, keyed through `episode_key` when the event maps to an episode.

**Why Silver?** Consistent naming, surrogate keys, and enforced uniqueness tests make joins predictable and performant. All silver tables are built from bronze sources (dbt models) and retain source IDs for audits.

---

## Gold (ML-ready feature snapshots)

**What it is:** Frozen JSON **feature payloads** keyed by a snapshot. Use these to train/score models without repeatedly re-deriving features.

- **`gold.feature_snapshots`** — One row per snapshot with run metadata (ingestion run id, environment, git branch/commit, notes).
- **`gold.castaway_season_features`** — Feature JSON per **castaway × season** for the snapshot.
  - PK: (`snapshot_id`, `castaway_key`).
- **`gold.castaway_episode_features`** — Feature JSON per **castaway × episode** for the snapshot (includes `episode_in_season` for easy filtering).
  - PK: (`snapshot_id`, `castaway_key`, `episode_key`).
- **`gold.season_features`** — Feature JSON per **season** for the snapshot.
  - PK: (`snapshot_id`, `season_key`).

The `feature_payload` is flexible JSON (e.g., cumulative confessionals up to episode N, challenge win rates, social/tribal signals, etc.).
Use `feature_snapshots` to select the snapshot you want (latest, prod, specific commit, etc.).

---

## How to Join Things (safe patterns)

- **Person master data:** `silver.dim_castaway` on `castaway_key` (or `castaway_id` if staying in natural IDs).
- **Season context:** `silver.dim_episode` → `silver.dim_season` using `episode_key` → `season_key`.
- **Roster / season outcomes:** `silver.bridge_castaway_season` gives placement and jury/finalist flags.
- **Challenge context:** `silver.fact_challenge_results` → `silver.dim_challenge` (and optionally to `challenge_skill_*`).
- **Vote context:** `silver.fact_vote_history` has voter, target, and eliminated; join voters on `castaway_key` and targets/eliminated via natural IDs back to `dim_castaway` if you need names.
- **Advantages:** `silver.fact_advantage_movement` → `silver.dim_advantage` (and optionally voter/target castaway dims).

> Tip: Prefer surrogate keys (`*_key`) in Silver joins for speed; keep natural IDs in the SELECT for readability.

---

## Query Examples

### 1) Episode recap: who spoke, who won, who left

```sql
-- One episode across all castaways
with ep as (
  select e.episode_key
  from silver.dim_episode e
  join silver.dim_season s using (season_key)
  where s.version_season = 'US43' and e.episode_in_season = 5
)
select
  c.full_name,
  fc.confessional_count,
  fcr.result as challenge_result,
  fbm.tribe_status as status_at_boot
from ep
left join silver.fact_confessionals fc using (episode_key)
left join silver.fact_challenge_results fcr using (episode_key)
left join silver.fact_boot_mapping fbm using (episode_key)
left join silver.dim_castaway c on c.castaway_key = coalesce(fc.castaway_key, fcr.castaway_key, fbm.castaway_key)
order by c.full_name;
```

### 2) Season-level performance per castaway

```sql
select
  c.full_name,
  s.season_name,
  sum(case when fcr.result ilike '%win%' then 1 else 0 end) as wins,
  sum(fc.confessional_count) as confessionals,
  max(bcs.winner)::boolean as is_winner,
  bcs.place
from silver.dim_season s
join silver.dim_episode e using (season_key)
left join silver.fact_challenge_results fcr using (episode_key)
left join silver.fact_confessionals fc using (episode_key)
left join silver.bridge_castaway_season bcs using (season_key, castaway_id)
left join silver.dim_castaway c on c.castaway_key = coalesce(fcr.castaway_key, fc.castaway_key, bcs.castaway_key)
where s.version_season = 'US43'
group by 1,2,6;
```

### 3) Who voted for whom (with names)

```sql
select
  voter.full_name  as voter,
  target.full_name as voted_for,
  eliminated.full_name as voted_out,
  e.episode_in_season,
  s.season_name,
  fvh.vote_order,
  fvh.split_vote,
  fvh.nullified
from silver.fact_vote_history fvh
join silver.dim_episode e on e.episode_key = fvh.episode_key
join silver.dim_season s  on s.season_key  = fvh.season_key
join silver.dim_castaway voter on voter.castaway_key = fvh.castaway_key
left join silver.dim_castaway target on target.castaway_id = fvh.target_castaway_id
left join silver.dim_castaway eliminated on eliminated.castaway_id = fvh.voted_out_castaway_id
where s.version_season = 'US43'
order by e.episode_in_season, fvh.vote_order;
```

### 4) Advantage plays that nullified votes

```sql
select
  c.full_name as holder,
  t.full_name as target,
  a.advantage_type,
  e.episode_in_season,
  fam.votes_nullified
from silver.fact_advantage_movement fam
join silver.dim_advantage a on a.advantage_key = fam.advantage_key
join silver.dim_episode e on e.episode_key = fam.episode_key
left join silver.dim_castaway c on c.castaway_key = fam.castaway_key
left join silver.dim_castaway t on t.castaway_key = fam.target_castaway_key
where fam.success = true and fam.votes_nullified is not null
order by e.episode_in_season;
```

---

## Practical Tips

- **Episode alignment:** When in doubt, join via `episode_key` to ensure your counts line up with the recap order.
- **Targets vs voters:** In `fact_vote_history`, the **voter** is keyed (`castaway_key`), while **target** and **voted_out** are provided as natural IDs; join them to `dim_castaway` by `castaway_id` to get names.
- **Multi-boot episodes:** Use `fact_boot_mapping` + `vote_order` from `fact_vote_history` to understand sequencing.
- **Challenge skills:** Use `challenge_skill_bridge` → `challenge_skill_lookup` to group challenges by skill (balance/puzzle/water/etc.).
- **Audits:** Every fact table keeps the bronze source ID (e.g., `source_*_id`) so you can trace back to raw events.

---

## When to use each layer

- **Bronze:** provenance checks, low-level reconciliation, or when you need columns not yet curated.
- **Silver:** everyday analytics, dashboards, and ad-hoc questions — it has stable keys and friendly names.
- **Gold:** modeling features; pick a `snapshot_id` (or the latest in your environment) and read the JSON payloads.

---

## Data Freshness & Environments

- Loads are recorded in `bronze.ingestion_runs` with timestamps and git metadata.
- Gold snapshots reference an `ingest_run_id` so you can line up feature generation with the exact raw dataset state.
- The stack supports **dev** vs **prod** profiles; see the repo’s README for running via Docker/Airflow or the lite package.

---

### Column Naming Cheatsheet

- Natural IDs: `castaway_id`, `version_season`, `challenge_id`, `advantage_id`, `episode_in_season`.
- Surrogate keys (silver): `castaway_key`, `season_key`, `episode_key`, `challenge_key`, `advantage_key`.
- Fact grains:
  - **Confessionals:** castaway × episode
  - **Challenge results:** castaway × challenge (per sog_id)
  - **Vote history:** voter × episode (with target/eliminated IDs)
  - **Advantage movement:** advantage event sequence within season
  - **Boot mapping:** elimination events per episode
  - **Tribe membership:** castaway × day (episode-aligned)

---