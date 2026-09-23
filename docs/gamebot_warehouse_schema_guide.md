# Survivor Warehouse: ML-Focused Schema Guide

_Last updated: 2025-11-05_

This warehouse follows a **Medallion** design optimized for **machine learning and winner prediction research**:

- **Bronze** = raw but relational copies of the open-source `survivoR` datasets + ingestion metadata
- **Silver** = ML-focused **feature engineering tables** organized by strategic categories (challenges, advantages, voting, social dynamics, edit analysis, etc.)
- **Gold** = Two **ML-ready feature tables** for different modeling approaches: non-edit gameplay vs hybrid gameplay+edit features

Need the upstream column glossary? Check `survivoR.pdf` in the repo root — it's the exported survivoR R documentation we align to.

---

## Quick Schema Overview

```
bronze.*                    # 19 raw tables from survivoR package
├── castaways               # Demographics, background
├── episodes                # Season & episode metadata
├── confessionals           # Edit/narrative data
├── challenge_results       # Individual challenge performance
├── advantage_details       # Advantage definitions
├── advantage_movement      # Advantage lifecycle events
├── vote_history           # Tribal council voting
├── jury_votes             # Final tribal council votes
└── [11 more tables...]    # Complete survivoR dataset

silver.*                   # 8 ML feature category tables
├── castaway_profile       # Demographics & background features
├── challenge_performance  # Physical & mental game features
├── advantage_strategy     # Advantage gameplay features
├── vote_dynamics         # Voting behavior & alliance features
├── social_positioning    # Social dynamics & tribe features
├── edit_features         # Production narrative features
├── jury_analysis         # Endgame relationship features
└── season_context        # Season format & meta-game features

gold.*                     # 2 ML-ready feature tables
├── ml_features_non_edit   # Pure gameplay features (1,441 rows)
└── ml_features_hybrid     # Gameplay + edit features (1,441 rows)
```

---

## Bronze Layer: Raw Data Foundation

**What it is:** Direct, cleaned copies of the open-source `survivoR` R package datasets with added ingestion metadata.

**Key bronze tables:**
- **`bronze.castaways`** — Castaway demographics, background, outcomes (1 row per person × season)
- **`bronze.episodes`** — Episode metadata, viewership, ratings (1 row per episode)
- **`bronze.confessionals`** — Individual confessional counts and time per episode (1 row per castaway × episode)
- **`bronze.challenge_results`** — Challenge participation and outcomes (1 row per castaway × challenge)
- **`bronze.advantage_details`** — Advantage definitions and properties (1 row per season × advantage)
- **`bronze.advantage_movement`** — Advantage lifecycle events: found, played, transferred (1 row per event)
- **`bronze.vote_history`** — Tribal council voting records (1 row per vote cast)
- **`bronze.jury_votes`** — Final tribal council jury votes (1 row per juror × finalist)

**Schema features:**
- **Natural keys** like `castaway_id`, `version_season`, `episode` for easy joins
- **Audit columns** `ingest_run_id`, `ingested_at` for data lineage tracking
- **Indexes** on common join keys and version columns for performance

---

## Silver Layer: ML Feature Engineering

**What it is:** Strategic feature engineering tables organized by gameplay categories for machine learning analysis.

### Core Design Philosophy

The silver layer transforms raw data into **8 strategic feature categories** that correspond to different aspects of Survivor gameplay:

1. **Demographics & Background** → `castaway_profile`
2. **Physical & Mental Challenges** → `challenge_performance`
3. **Advantage Strategy** → `advantage_strategy`
4. **Voting & Alliance Behavior** → `vote_dynamics`
5. **Social Positioning** → `social_positioning`
6. **Production & Edit Analysis** → `edit_features`
7. **Jury Relationships** → `jury_analysis`
8. **Season Context & Format** → `season_context`

Every table except `castaway_profile` includes a **hash-based surrogate key** (`*_key`) for performance, and all include **natural IDs** for readability.

### Strategic Feature Tables

#### `silver.castaway_profile`
**Purpose:** Demographic and background features for understanding contestant archetypes and representation.

**Grain:** 1 row per castaway × season

**Key features:**
- **Demographics:** `age`, `gender`, `race`, `ethnicity`, `bipoc`, `lgbt`
- **Background:** `occupation`, `city`, `state`, `personality_type`
- **Meta-game:** `season_location`, `season_name`, `tribe_setup`

**Example:**
```sql
SELECT castaway_id, full_name, age, gender, race, occupation, bipoc, lgbt
FROM silver.castaway_profile
WHERE version_season = 'US47';
```

#### `silver.challenge_performance`
**Purpose:** Individual challenge performance across different skill categories and formats.

**Grain:** 1 row per castaway × episode × challenge

**Key features:**
- **Participation:** `sit_out`, `chosen_for_reward`, `balance_participated`, `puzzle_participated`
- **Skill wins:** `balance_win`, `endurance_win`, `puzzle_win`, `strength_win`, `water_win`
- **Performance:** `won_flag`, `order_of_finish`, `result`
- **Context:** `challenge_format` (`individual`/`team`), `merge_phase`

**Example:**
```sql
SELECT castaway_id, episode, challenge_format, won_flag,
       balance_win, puzzle_win, strength_win
FROM silver.challenge_performance
WHERE version_season = 'US47' AND challenge_format = 'individual';
```

#### `silver.advantage_strategy`
**Purpose:** Strategic advantage gameplay including finding, playing, and timing decisions.

**Grain:** 1 row per advantage event (found, played, transferred)

**Key features:**
- **Strategy:** `played_successfully`, `played_for_self`, `played_for_others`
- **Timing:** `episode`, `day`, `sequence_id`
- **Impact:** `votes_nullified`, `success_outcome`
- **Context:** `advantage_category`, `event_category`

**Example:**
```sql
SELECT castaway_id, episode, advantage_category, event_category,
       played_successfully, votes_nullified
FROM silver.advantage_strategy
WHERE version_season = 'US47' AND event_category = 'played';
```

#### `silver.vote_dynamics`
**Purpose:** Voting behavior and alliance positioning at tribal councils.

**Grain:** 1 row per vote cast

**Key features:**
- **Accuracy:** `vote_correct`, `in_majority_alliance`
- **Strategy:** `voting_alone`, `split_vote_scenario`
- **Context:** `merge_phase`, `vote_event`, `vote_order`
- **Targets:** `target_id`, `voted_out_id`

**Example:**
```sql
SELECT castaway_id, episode, vote_correct, in_majority_alliance,
       voting_alone, merge_phase
FROM silver.vote_dynamics
WHERE version_season = 'US47' AND merge_phase = 'post_merge';
```

#### `silver.social_positioning`
**Purpose:** Social dynamics and demographic composition within tribes and alliances.

**Grain:** 1 row per castaway × episode × tribe

**Key features:**
- **Demographics:** `same_gender_ratio`, `bipoc_similarity_ratio`, `lgbt_similarity_ratio`
- **Status:** `gender_status`, `racial_status`, `lgbt_status` (majority/minority)
- **Tribe:** `tribe`, `tribe_status`, `original_tribe`
- **Context:** `merge_phase`, `day`

**Example:**
```sql
SELECT castaway_id, episode, tribe, same_gender_ratio,
       racial_status, lgbt_status, tribe_status
FROM silver.social_positioning
WHERE version_season = 'US47' AND merge_phase = 'pre_merge';
```

#### `silver.edit_features`
**Purpose:** Production narrative and edit analysis for understanding winner's edit patterns.

**Grain:** 1 row per castaway × episode

**Key features:**
- **Screen time:** `confessional_count`, `confessional_time`
- **Edit ratios:** `confessional_count_ratio`, `over_edited_count`, `under_edited_count`
- **Presence:** `has_confessional`, `expected_confessional_time`

**Example:**
```sql
SELECT castaway_id, episode, confessional_count, confessional_time,
       confessional_count_ratio, over_edited_count
FROM silver.edit_features
WHERE version_season = 'US47' AND has_confessional = 1;
```

#### `silver.jury_analysis`
**Purpose:** Endgame relationship analysis for understanding jury voting patterns.

**Grain:** 1 row per jury vote (juror × finalist)

**Key features:**
- **Relationships:** `same_original_tribe`, `different_original_tribe`, `juror_original_tribe`, `finalist_original_tribe`
- **Votes:** `voted_for_finalist_name` (the ballot, `'1.0'`/`'0.0'`), `voted_for_winner`, `voted_against_winner`
- **IDs:** `castaway_id` (the juror), `finalist_id`, `actual_winner_id`

**Example:**
```sql
SELECT finalist_id, castaway_id AS juror_id, same_original_tribe,
       juror_original_tribe, finalist_original_tribe, voted_for_winner
FROM silver.jury_analysis
WHERE version_season = 'US47';
```

#### `silver.season_context`
**Purpose:** Season-level format and meta-game features for understanding strategic evolution.

**Grain:** 1 row per season

**Key features:**
- **Format:** `has_edge_of_extinction`, `has_tribe_swap`, `has_merge_twist`
- **Cast composition:** `cast_size`, `male_ratio`, `bipoc_ratio`, `returnee_ratio`
- **Meta-game:** `season_era`, `season_recency_weight`
- **Viewership:** `viewers_premiere`, `viewers_finale`, `viewers_mean`, `rank`

**Example:**
```sql
SELECT version_season, season_era, cast_size, bipoc_ratio,
       has_edge_of_extinction, season_recency_weight
FROM silver.season_context
WHERE season_number >= 40;
```

---

## Gold Layer: ML-Ready Features

**What it is:** Two pre-aggregated feature tables optimized for different machine learning approaches to winner prediction.

### ML Feature Tables

#### `gold.ml_features_non_edit`
**Purpose:** Pure gameplay features without production/edit data for testing if winners can be predicted from gameplay alone.

**Grain:** 1 row per castaway × season (1,441 total rows)

**Feature categories:**
- **Challenge performance:** `challenges_won`, `individual_win_rate`, `strength_wins`, `puzzle_wins`
- **Strategic gameplay:** `advantages_found`, `idols_played`, `idol_success_rate`
- **Social & voting:** `vote_accuracy_rate`, `majority_alliance_rate`, `pre_merge_tribals_attended`, `post_merge_tribals_attended`
- **Demographics:** `current_age`, `gender`, `race`, `is_bipoc`, `is_lgbt`
- **Targets:** `target_winner`, `target_finalist`, `target_jury`, `target_placement`

#### `gold.ml_features_hybrid`
**Purpose:** Combined gameplay and edit features for testing if production narrative improves prediction accuracy.

**Grain:** 1 row per castaway × season (1,441 total rows)

**Additional edit features:**
- **Screen time:** `total_confessional_count`, `avg_confessional_time_per_episode`, `confessional_presence_rate`
- **Edit patterns:** `over_edited_count_episodes`, `under_edited_count_episodes`, `over_edited_rate`
- **Edit flags:** `high_edit_presence`, `significantly_over_edited`, `significantly_under_edited`

### ML Pipeline Usage

```sql
-- Training data for winner prediction (non-edit approach)
SELECT castaway_id, version_season, target_winner,
       challenges_won, vote_accuracy_rate, advantages_found,
       is_bipoc, current_age, gender
FROM gold.ml_features_non_edit
WHERE target_placement IS NOT NULL;

-- Compare feature importance between approaches
SELECT
  'non_edit' as model_type,
  CORR(challenges_won, target_winner) as challenge_correlation,
  CORR(vote_accuracy_rate, target_winner) as vote_correlation
FROM gold.ml_features_non_edit
WHERE target_winner IS NOT NULL

UNION ALL

SELECT
  'hybrid' as model_type,
  CORR(challenges_won, target_winner) as challenge_correlation,
  CORR(total_confessional_count, target_winner) as edit_correlation
FROM gold.ml_features_hybrid
WHERE target_winner IS NOT NULL;
```

---

## Join Patterns & Query Examples

### Cross-category feature analysis
```sql
-- Combine multiple strategic dimensions
SELECT cp.castaway_id, cp.full_name, cp.gender, cp.bipoc,
       COUNT(DISTINCT ch.challenge_performance_key) as challenges_participated,
       COUNT(DISTINCT CASE WHEN ch.won_flag = 1 THEN ch.challenge_performance_key END) as challenges_won,
       COUNT(DISTINCT adv.advantage_strategy_key) as advantage_actions,
       AVG(vd.vote_correct::int) as vote_accuracy,
       AVG(ef.confessional_count) as avg_confessionals
FROM silver.castaway_profile cp
LEFT JOIN silver.challenge_performance ch USING (castaway_id, version_season)
LEFT JOIN silver.advantage_strategy adv USING (castaway_id, version_season)
LEFT JOIN silver.vote_dynamics vd USING (castaway_id, version_season)
LEFT JOIN silver.edit_features ef USING (castaway_id, version_season)
WHERE cp.version_season = 'US47'
GROUP BY cp.castaway_id, cp.full_name, cp.gender, cp.bipoc;
```

### Temporal analysis across episodes
```sql
-- Track strategic evolution over time (one row per castaway x episode)
WITH ch AS (
  SELECT castaway_id, version_season, episode, SUM(won_flag) AS challenges_won
  FROM silver.challenge_performance
  GROUP BY castaway_id, version_season, episode
),
vd AS (
  SELECT castaway_id, version_season, episode, AVG(vote_correct) AS vote_accuracy
  FROM silver.vote_dynamics
  GROUP BY castaway_id, version_season, episode
),
sp AS (
  SELECT castaway_id, version_season, episode, MAX(tribe_status) AS tribe_status
  FROM silver.social_positioning
  GROUP BY castaway_id, version_season, episode
)
SELECT ef.castaway_id, ef.episode,
       ef.confessional_count,
       SUM(COALESCE(ch.challenges_won, 0)) OVER (
         PARTITION BY ef.castaway_id ORDER BY ef.episode
       ) AS challenges_won_cumulative,
       vd.vote_accuracy,
       sp.tribe_status
FROM silver.edit_features ef
LEFT JOIN ch USING (castaway_id, version_season, episode)
LEFT JOIN vd USING (castaway_id, version_season, episode)
LEFT JOIN sp USING (castaway_id, version_season, episode)
WHERE ef.version_season = 'US47'
ORDER BY ef.castaway_id, ef.episode;
```

### Season format impact analysis
```sql
-- Compare performance by season format
SELECT sc.season_era, sc.has_edge_of_extinction,
       AVG(mf.individual_win_rate) as avg_challenge_performance,
       AVG(mf.vote_accuracy_rate) as avg_vote_accuracy,
       AVG(mf.total_confessional_count) as avg_screen_time
FROM silver.season_context sc
JOIN gold.ml_features_hybrid mf USING (version_season)
WHERE mf.target_placement <= 3  -- Top 3 finishers
GROUP BY sc.season_era, sc.has_edge_of_extinction
ORDER BY sc.season_era;
```

---

## Data Lineage & Quality

- **Sources:** All silver tables reference bronze sources via `dbt` models
- **Testing:** Unique keys, not null constraints, and referential integrity tests
- **Freshness:** Weekly refresh via Airflow DAG (Mondays at 4 AM UTC by default, `GAMEBOT_DAG_SCHEDULE`)
- **Auditing:** `ingest_run_id` tracks data lineage back to specific loads

**Row counts (packaged gamebot-lite snapshot):**
- `castaway_profile`: 1,441 rows (one per castaway × season)
- `challenge_performance`: 22,103 rows (individual challenge records)
- `advantage_strategy`: 979 rows (advantage events)
- `vote_dynamics`: 9,167 rows (votes cast)
- `social_positioning`: 15,133 rows (tribe membership records)
- `edit_features`: 14,055 rows (episode edit data)
- `jury_analysis`: 1,652 rows (juror × finalist)
- `season_context`: 76 rows (season metadata)

---

## Research Applications

This ML-focused schema supports various Survivor analytics and machine learning research:

1. **Winner Prediction:** Use gold tables to train models predicting winners from early-season features
2. **Edit Analysis:** Compare winner's edit patterns using `edit_features` and `ml_features_hybrid`
3. **Strategic Archetype Classification:** Cluster players by challenge/advantage/voting patterns
4. **Representation Analysis:** Study demographic patterns and outcomes using `castaway_profile`
5. **Format Impact:** Analyze how rule changes affect gameplay using `season_context`
6. **Social Dynamics:** Model alliance formation and voting behavior using `social_positioning` and `vote_dynamics`

For code examples and notebooks, see the `examples/` and `notebooks/` directories in the repository.
