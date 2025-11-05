# Survivor Warehouse: ML-Focused Schema Guide

_Last updated: 2025-11-05_

This warehouse follows a **Medallion** design optimized for **machine learning and winner prediction research**:

- **Bronze** = raw but relational copies of the open-source `survivoR` datasets + ingestion metadata
- **Silver** = ML-focused **feature engineering tables** organized by strategic categories (challenges, advantages, voting, social dynamics, edit analysis, etc.)
- **Gold** = Two **ML-ready feature tables** for different modeling approaches: non-edit gameplay vs hybrid gameplay+edit features

Need the upstream column glossary? Check `survivoR.pdf` in the repo root — it's the exported survivoR R documentation we align to.

Below is a practical guide to the ML-focused architecture, showing how each layer supports different aspects of Survivor winner prediction modeling.
If you're an analyst, start with **Silver**. If you're building ML models, look at **Gold**.

---

## Key ID Concepts (read this first)

- **`version_season`**: A canonical season identifier (e.g., `US43`). It ties everything in a given season together.
- **`castaway_id`**: Stable ID for a person across seasons. Use it to connect a person's master profile to their strategic actions, votes, challenges, etc.
- **`episode`**: Episode number within the season.
- **Surrogate keys in silver** (e.g., `*_key` columns) are warehouse-generated hash keys for uniqueness and traceability.
  Keep both around: the natural IDs (`castaway_id`, `version_season`, etc.) for readability and the surrogate keys for data integrity.

---

## Bronze (raw, schema-on-write)

**What it is:** One table per upstream survivoR dataset, plus ingestion metadata. Each row mirrors source data with minimal massaging.
Use Bronze for provenance and completeness; prefer **Silver** for ML feature engineering.

### Core tables used for ML features:
- **`bronze.castaway_details`** — Demographics, occupation, personality type, race/ethnicity, LGBT status
- **`bronze.castaways`** — Season participation, age, placement, winner/finalist/jury flags
- **`bronze.season_summary`** — Season metadata, cast size, twist detection
- **`bronze.challenge_results`** — Individual challenge performance and outcomes
- **`bronze.advantage_movement`** — Advantage finding, playing, and strategic decisions
- **`bronze.vote_history`** — Voting patterns and tribal council dynamics
- **`bronze.jury_votes`** — Final jury voting relationships
- **`bronze.confessionals`** — Edit presence via confessional counts and screen time
- **`bronze.tribe_mapping`** — Social positioning and tribe composition over time

---

## Silver (ML feature engineering — start here for modeling)

**What it is:** Strategic feature engineering tables organized by gameplay categories. Each table represents a different aspect of Survivor strategy and performance, designed specifically for ML model training.

### Strategic Feature Categories

#### **`silver.castaway_profile`** — Demographics & Background
Core demographic and background features for social positioning analysis:
- **Demographics**: age, gender, race, ethnicity, bipoc, lgbt status
- **Background**: occupation, personality_type
- **Season context**: season location, viewership, cast composition

*ML relevance: Social positioning, demographic minority/majority dynamics, occupation-based strategic tendencies*

#### **`silver.challenge_performance`** — Physical & Mental Game
Individual and team challenge performance across skill categories:
- **Performance**: challenges won, individual vs team wins, pre/post merge performance
- **Skills**: balance, endurance, puzzle, strength, water challenge performance
- **Participation**: reward selections, sit-outs, participation rates by skill
- **Context**: merge phase, challenge format analysis

*ML relevance: Physical threat level, challenge beast identification, reward leverage*

#### **`silver.advantage_strategy`** — Strategic Gameplay
Advantage finding, playing, and strategic decision-making:
- **Finding**: advantages found, idols found, discovery timing
- **Playing**: successful/unsuccessful plays, self vs others protection
- **Strategy**: timing analysis, merge phase strategy, risk management

*ML relevance: Strategic awareness, idol game mastery, protective vs aggressive play*

#### **`silver.vote_dynamics`** — Tribal Council Strategy
Voting behavior and tribal council strategic positioning:
- **Accuracy**: correct votes, vote alignment with majority
- **Strategy**: lone wolf vs alliance voting, merge vote accuracy
- **Receiving**: votes received, target status, vote distribution
- **Context**: pre/post merge patterns, tribal council attendance

*ML relevance: Strategic voting, alliance management, threat perception*

#### **`silver.social_positioning`** — Social Dynamics
Tribe composition and social minority/majority status tracking:
- **Demographics**: gender, racial, LGBT composition analysis
- **Positioning**: minority/majority status over time, demographic similarity ratios
- **Tribes**: original tribe strength post-merge, tribe swap dynamics
- **Context**: merge phase social repositioning

*ML relevance: Social threat assessment, alliance accessibility, jury appeal*

#### **`silver.edit_features`** — Production & Narrative
Edit presence and production narrative indicators:
- **Confessionals**: count, time, expected vs actual ratios
- **Visibility**: edit presence flags, over/under edited analysis
- **Narrative**: production choice indicators, storyline tracking

*ML relevance: Winner's edit detection, narrative arc analysis, production signals*

#### **`silver.jury_analysis`** — Endgame Relationships
Jury voting patterns and final tribal council dynamics:
- **Relationships**: original tribe connections, pre-jury relationships
- **Voting**: jury vote patterns, same-tribe loyalty, strategic voting

*ML relevance: Jury appeal, social bonds, endgame relationship management*

#### **`silver.season_context`** — Meta-Game Features
Season-level features and meta-game context:
- **Format**: cast size, new era vs old school, twist presence
- **Composition**: demographic diversity, returnee ratios, age distribution
- **Evolution**: season recency weights, format changes, meta shifts

*ML relevance: Game evolution context, format adaptation, meta-game awareness*

---

## Gold (ML-ready model features)

**What it is:** Two comprehensive ML feature tables that aggregate silver layer features into model-ready formats for different research approaches.

### **`gold.ml_features_non_edit`** — Pure Gameplay Analysis
Complete feature set focusing purely on gameplay without edit/production signals:
- **Target variables**: winner, finalist, jury, placement
- **Demographics**: All castaway profile features
- **Performance**: Challenge aggregations across all skill categories
- **Strategy**: Advantage and voting behavior aggregations
- **Social**: Social positioning and relationship features
- **Context**: Season meta-features and format context

*Use case: Understanding pure gameplay predictors, control for production bias*

### **`gold.ml_features_hybrid`** — Complete Predictive Model
All non-edit features PLUS edit/production features for maximum prediction accuracy:
- **Everything from non-edit table** PLUS
- **Edit features**: Confessional patterns, narrative presence, production signals
- **Hybrid indicators**: Edit + gameplay success combinations
- **Visibility metrics**: Screen time patterns, storyline tracking

*Use case: Maximum prediction accuracy, winner's edit analysis, production choice insights*

---

## ML Research Applications

### Winner Prediction Models
- **Training data**: Use `target_winner`, `target_finalist`, `target_jury` flags
- **Feature selection**: Compare non-edit vs hybrid approaches
- **Temporal analysis**: Use season recency weights for model training

### Strategic Archetype Analysis
- **Challenge performance** + **advantage strategy** = Physical vs strategic player types
- **Social positioning** + **vote dynamics** = Social vs strategic game styles
- **Edit features** = Production narrative archetypes

### Meta-Game Evolution
- **Season context** features track game evolution over time
- **Format changes** (new era, twists) impact strategic effectiveness
- **Demographic trends** in casting and winner characteristics

---

## Query Examples for ML Feature Engineering

### 1) Winner characteristics by era
```sql
SELECT
  season_era,
  AVG(challenges_won) as avg_challenge_wins,
  AVG(advantages_found) as avg_advantages,
  AVG(vote_accuracy_rate) as avg_vote_accuracy,
  AVG(is_bipoc) as bipoc_winner_rate
FROM gold.ml_features_non_edit
WHERE target_winner = 1
GROUP BY season_era
ORDER BY season_era;
```

### 2) Edit vs gameplay prediction comparison
```sql
-- Feature correlation with winning
SELECT
  'Non-edit' as model_type,
  CORR(challenges_won, target_winner) as challenge_correlation,
  CORR(vote_accuracy_rate, target_winner) as vote_correlation,
  NULL as edit_correlation
FROM gold.ml_features_non_edit

UNION ALL

SELECT
  'Hybrid' as model_type,
  CORR(challenges_won, target_winner) as challenge_correlation,
  CORR(vote_accuracy_rate, target_winner) as vote_correlation,
  CORR(total_confessional_count, target_winner) as edit_correlation
FROM gold.ml_features_hybrid;
```

### 3) Strategic archetype clustering features
```sql
SELECT
  castaway_id,
  version_season,
  -- Physical game
  individual_win_rate,
  strength_wins + endurance_wins as physical_challenges,
  -- Strategic game
  advantages_found,
  vote_accuracy_rate,
  -- Social game
  avg_same_gender_ratio,
  racial_minority_rate,
  -- Outcome
  target_placement
FROM gold.ml_features_non_edit
WHERE target_jury = 1 OR target_finalist = 1;
```

---

## ML Pipeline Integration

### Feature Engineering Workflow
1. **Bronze ingestion**: Raw survivoR data loaded via Airflow
2. **Silver transformation**: Strategic category tables built via dbt
3. **Gold aggregation**: ML-ready features compiled from silver sources
4. **Model training**: Use gold tables directly for sklearn/torch/etc.

### Model Versioning
- **Data snapshots**: Track via `ingested_at` timestamps
- **Feature evolution**: Silver tables track `created_at` for reproducibility
- **Model experiments**: Gold features support A/B testing different feature sets

### Production Deployment
- **Feature store**: Gold tables serve as feature store for real-time prediction
- **Batch scoring**: Process new seasons through bronze → silver → gold pipeline
- **Model monitoring**: Track feature distribution drift via silver layer statistics

---

## When to use each layer

- **Bronze:** Raw data exploration, source validation, upstream schema changes
- **Silver:** Feature engineering, strategic analysis, archetype research
- **Gold:** ML model training, prediction experiments, production scoring

---

## Data Freshness & Reproducibility

- **Ingestion tracking**: `bronze.ingestion_runs` records exact data vintage
- **Feature lineage**: Silver tables maintain `created_at` timestamps
- **Model reproducibility**: Gold features link back to specific bronze snapshots
- **Schema evolution**: dbt manages silver/gold transformations with version control

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
- **`bronze.challenge_results`** — Who participated and how they did in each challenge: outcome, team, sit-outs, order of finish, reward choice, and links to the challenge definition and episode context. Includes `sog_id` (stage-of-game) so challenge outcomes align with tribe swaps and returns.
  - Uniqueness: (`castaway_id`, `challenge_id`, `sog_id`, `version_season`).
- **`bronze.challenge_summary`** — Upstream “helper” rollup that tags every challenge outcome with multiple analytic categories (All, Tribal Immunity, Individual, Duel, etc.) per castaway. It is **intentionally non-unique** across (`version_season`, `challenge_id`, `castaway_id`) because a single row can appear once per category; downstream layers should aggregate on the category you care about rather than expect a primary key. Join back to `challenge_results` via (`version_season`, `challenge_id`, `castaway_id`) when you need detailed placement/order columns.
- **`bronze.advantage_details`** — Advantage inventory for a season: type, clue text, where it was found, and any conditions.
  - PK: (`version_season`, `advantage_id`).
- **`bronze.advantage_movement`** — The lifecycle of each advantage: who held it when, passes, plays, targets, outcomes, and whether votes were nullified.
  - Uniqueness: (`version_season`, `castaway_id`, `advantage_id`, `sequence_id`).
- **`bronze.vote_history`** — Round-by-round votes: who voted, who they targeted, tie/split/nullified flags, textual immunity context (e.g., “Hidden”, “Individual”), and links to relevant challenges. The `sog_id` column tracks the logical stage of the game to sync with boot/challenge tables.
- **`bronze.jury_votes`** — Final Tribal Council votes: juror → finalist; one row per (`version_season`, `castaway_id`, `vote`). Historical twists such as `UK02` include public votes with no `castaway_id`; those rows are still captured (the unique key tolerates the null juror ID).
- **`bronze.boot_mapping`** — Episode-level mapping of who left (or number of boots if multiple) with tribe/game status context; `sog_id` provides a shared stage-of-game key.
- **`bronze.boot_order`** — Elimination order per castaway (supports re-entry arcs; upstream `order` column is loaded as `boot_order_position` and is occasionally null when a player returns mid-season).
- **`bronze.tribe_mapping`** — Day-by-day membership: which tribe a castaway was on, and tribe status if applicable; uniqueness is (`castaway_id`, `version_season`, `episode`, `tribe`, `day`). Some historical rows omit `day` from the upstream export, so the key allows a null value there.
- **`bronze.confessionals`** — For each castaway × episode: count and total time of confessionals, plus expected values (from the upstream methodology).
- **`bronze.auction_details`** — Item-level Survivor auction purchases (who won, bid amount, covered/alternative offers, shared items, notes). On seasons with tribe-wide bidding (`US05`) or “no bid” allocations (`SA08`), `castaway_id` is intentionally null; the unique key tolerates those cases.
- **`bronze.survivor_auction`** — Castaway auction summary per episode (tribe status, total spend, currency, boots remaining).
- **`bronze.castaway_scores`** — Season-level composite scoring metrics per castaway (overall/outwit/outplay/outlast, challenge ranks, votes, advantages).
  - Uniqueness: (`version_season`, `castaway_id`).
- **`bronze.journeys`** — Per-journey participation records (episode, SoG id, lost-vote flag, reward details, decisions made).
  - Uniqueness: (`version_season`, `episode`, `sog_id`, `castaway_id`).

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
- **`silver.fact_challenge_results`** — Individual/tribal performance per **castaway × challenge** (+ sit-outs, order of finish, chosen for reward, etc.), retaining `sog_id` for stage-of-game joins back to boot/vote events. Optional crosswalk to the relevant advantage.
  - Grain: 1 row per (`castaway_key`, `challenge_key`, `sog_id`).
- **`silver.fact_journeys`** — Journey outcomes per **castaway × stage-of-game**: rewards earned, whether the vote was lost/regained, and optional narrative notes (`game_played`, `event`).
  - Grain: 1 row per (`version_season`, `castaway_id`, `sog_id`). Includes episode, season, and castaway keys for easy joins.
- **`silver.fact_vote_history`** — Voting actions per **castaway × episode**: who they targeted, who was eliminated, immunity context (text field carried from bronze), split details (comma-delimited list of names), tie/nullified indicators, `sog_id` for stage alignment, and `vote_order` within the round.
  - Grain: 1 row per voting action; carries both `castaway_key` (the voter) and the target/eliminated natural IDs.
- **`silver.fact_advantage_movement`** — Advantage lifecycle events (found, transferred, played) with outcomes and any votes nullified.
  - Grain: 1 row per (`version_season`, `advantage_id`, `sequence_id`), with keys to castaway/target, season/episode, and the advantage itself.
- **`silver.fact_boot_mapping`** — Episode-level elimination context (including multi-boot episodes) with tribe/game status at the time, keyed by `sog_id` to align with votes and challenge outcomes.
  - Grain: typically 1 row per boot event per episode (nullable `castaway_key` when event is aggregate-only).
- **`silver.fact_tribe_membership`** — Day-by-day tribe membership per castaway with episode alignment.
  - Grain: 1 row per (`castaway`, `day`) within a season, keyed through `episode_key` when the event maps to an episode.
- **`silver.castaway_season_scores`** — Season-level scoring metrics (challenge performance, advantage usage, vote success) aligned with castaway and season keys.
  - Grain: 1 row per (`version_season`, `castaway_id`). Joins directly to `bridge_castaway_season` or `dim_castaway`/`dim_season` for analytics.

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
where fam.success = 'yes' and fam.votes_nullified is not null
order by e.episode_in_season;
```

---

## Practical Tips

- **Episode alignment:** When in doubt, join via `episode_key` to ensure your counts line up with the recap order.
- **Targets vs voters:** In `fact_vote_history`, the **voter** is keyed (`castaway_key`), while **target** and **voted_out** are provided as natural IDs; join them to `dim_castaway` by `castaway_id` to get names.
- **Multi-target idols:** When an advantage protects multiple players, `fact_advantage_movement` surfaces one row per protected castaway (`played_for_id` is split and trimmed before load).
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
