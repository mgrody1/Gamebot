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
├── ml_features_non_edit   # Pure gameplay features (3,133 rows)
└── ml_features_hybrid     # Gameplay + edit features (3,133 rows)
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
- **Audit columns** `ingest_run_id`, `ingest_time` for data lineage tracking
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

Each table includes **hash-based surrogate keys** (`*_key`) for performance and **natural IDs** for readability.

### Strategic Feature Tables

#### `silver.castaway_profile`
**Purpose:** Demographic and background features for understanding contestant archetypes and representation.

**Grain:** 1 row per castaway (across all seasons they played)

**Key features:**
- **Demographics:** `age`, `gender`, `race`, `ethnicity`, `bipoc`, `lgbt`
- **Background:** `occupation`, `hometown`, `personality_type`
- **Meta-game:** `season_location`, `returner_status`

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
- **Participation:** `sit_out`, `chosen_for_reward`, `individual_challenge`
- **Skill wins:** `balance_win`, `endurance_win`, `puzzle_win`, `strength_win`, `water_win`
- **Performance:** `won_flag`, `order_of_finish`, `team_win`
- **Context:** `challenge_format`, `merge_phase`

**Example:**
```sql
SELECT castaway_id, episode, challenge_format, won_flag,
       balance_win, puzzle_win, strength_win
FROM silver.challenge_performance
WHERE version_season = 'US47' AND individual_challenge = 1;
```

#### `silver.advantage_strategy`
**Purpose:** Strategic advantage gameplay including finding, playing, and timing decisions.

**Grain:** 1 row per advantage event (found, played, transferred)

**Key features:**
- **Strategy:** `played_successfully`, `played_for_self`, `played_for_others`
- **Timing:** `episode`, `merge_phase`, `sequence_id`
- **Impact:** `votes_nullified`, `outcome`
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
- **Context:** `merge_phase`, `tribal_council_number`
- **Targets:** `vote`, `voted_out_id`

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
- **Status:** `gender_status`, `racial_status`, `lgbt_status` (majority/minority/alone)
- **Tribe:** `tribe`, `tribe_status`, `original_tribe`
- **Context:** `merge_phase`, `game_status`

**Example:**
```sql
SELECT castaway_id, episode, tribe, same_gender_ratio,
       racial_status, lgbt_status, game_status
FROM silver.social_positioning
WHERE version_season = 'US47' AND merge_phase = 'pre_merge';
```

#### `silver.edit_features`
**Purpose:** Production narrative and edit analysis for understanding winner's edit patterns.

**Grain:** 1 row per castaway × episode

**Key features:**
- **Screen time:** `confessional_count`, `confessional_time`
- **Edit ratios:** `confessional_count_ratio`, `over_edited_count`, `under_edited_count`
- **Presence:** `has_confessional`, `confessional_time_expected`

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
- **Relationships:** `same_original_tribe`, `same_gender`, `similar_age`
- **Social bonds:** `pre_jury_relationship`, `alliance_history`
- **Performance:** `final_tribal_performance`, `vote_value`

**Example:**
```sql
SELECT finalist_id, juror_id, same_original_tribe, same_gender,
       pre_jury_relationship, final_tribal_performance
FROM silver.jury_analysis
WHERE version_season = 'US47';
```

#### `silver.season_context`
**Purpose:** Season-level format and meta-game features for understanding strategic evolution.

**Grain:** 1 row per season

**Key features:**
- **Format:** `has_edge_of_extinction`, `has_tribe_swap`, `has_merge_feast`
- **Cast composition:** `cast_size`, `male_ratio`, `bipoc_ratio`, `returnee_ratio`
- **Meta-game:** `season_era`, `season_recency_weight`
- **Viewership:** `viewers_premiere`, `viewers_finale`, `season_popularity`

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

**Grain:** 1 row per castaway × season (3,133 total rows)

**Feature categories:**
- **Challenge performance:** `challenges_won`, `individual_win_rate`, `strength_wins`, `puzzle_wins`
- **Strategic gameplay:** `advantages_found`, `idols_played`, `idol_success_rate`
- **Social & voting:** `vote_accuracy_rate`, `majority_alliance_rate`, `tribal_councils_attended`
- **Demographics:** `age`, `gender`, `race`, `is_bipoc`, `is_lgbt`
- **Targets:** `target_winner`, `target_finalist`, `target_jury`, `target_placement`

#### `gold.ml_features_hybrid`
**Purpose:** Combined gameplay and edit features for testing if production narrative improves prediction accuracy.

**Grain:** 1 row per castaway × season (3,133 total rows)

**Additional edit features:**
- **Screen time:** `total_confessional_count`, `avg_confessional_time`, `confessional_episode_ratio`
- **Edit patterns:** `over_edited_episodes`, `under_edited_episodes`, `edit_consistency`
- **Narrative arc:** `early_season_presence`, `late_season_presence`, `finale_edit_score`

### ML Pipeline Usage

```sql
-- Training data for winner prediction (non-edit approach)
SELECT castaway_id, version_season, target_winner,
       challenges_won, vote_accuracy_rate, advantages_found,
       is_bipoc, age, gender
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
       COUNT(ch.challenge_performance_key) as challenges_participated,
       SUM(ch.won_flag::int) as challenges_won,
       COUNT(adv.advantage_strategy_key) as advantage_actions,
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
-- Track strategic evolution over time
SELECT ef.castaway_id, ef.episode,
       ef.confessional_count,
       ch.challenges_won_cumulative,
       vd.vote_accuracy_to_date,
       sp.tribe_status
FROM silver.edit_features ef
JOIN silver.challenge_performance ch USING (castaway_id, version_season, episode)
JOIN silver.vote_dynamics vd USING (castaway_id, version_season, episode)
JOIN silver.social_positioning sp USING (castaway_id, version_season, episode)
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
- **Freshness:** Weekly refresh via Airflow DAG (Sundays at 7 AM UTC)
- **Auditing:** `ingest_run_id` tracks data lineage back to specific loads

**Row counts (as of latest run):**
- `castaway_profile`: 3,133 rows (all contestants across all seasons)
- `challenge_performance`: 21,231 rows (individual challenge records)
- `advantage_strategy`: 923 rows (advantage events)
- `vote_dynamics`: 8,769 rows (votes cast)
- `social_positioning`: 14,575 rows (tribe membership records)
- `edit_features`: 13,503 rows (episode edit data)
- `jury_analysis`: 1,577 rows (jury votes)
- `season_context`: 75 rows (season metadata)

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
