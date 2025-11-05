# Gamebot Warehouse — ML Feature Schema Cheat Sheet

_Last updated: 2025-11-05_

## Connect with an external SQL IDE

With the Docker stack running (`make up`), you can attach DBeaver, DataGrip, or any Postgres client to explore the warehouse and generate ERDs.

| Setting | Value (default) |
| --- | --- |
| Host | `localhost`
| Port | `5433` (configurable via `WAREHOUSE_DB_PORT` in `.env`)
| Database | `DB_NAME` (e.g., `survivor_dw_dev`)
| Username | `DB_USER` (e.g., `survivor_dev`)
| Password | `DB_PASSWORD`

This one-pager lists **grains**, **keys**, and **join patterns** for the ML-focused Silver feature engineering tables and Gold model-ready tables.

---

## Keys at a Glance

- **Natural IDs**: `castaway_id`, `version_season`, `episode`
- **Surrogate keys (Silver)**: `*_key` columns (hash-based unique identifiers)

---

## Silver Layer: ML Feature Categories

### Strategic Feature Tables

- **castaway_profile** — *Demographics & background* → **Grain:** 1 row per castaway (across all seasons they played)
- **challenge_performance** — *Physical & mental game* → **Grain:** 1 row per castaway × episode × challenge
- **advantage_strategy** — *Advantage gameplay* → **Grain:** 1 row per advantage event
- **vote_dynamics** — *Tribal council strategy* → **Grain:** 1 row per vote cast
- **social_positioning** — *Social dynamics* → **Grain:** 1 row per castaway × episode × tribe
- **edit_features** — *Production & narrative* → **Grain:** 1 row per castaway × episode
- **jury_analysis** — *Endgame relationships* → **Grain:** 1 row per jury vote
- **season_context** — *Meta-game features* → **Grain:** 1 row per season

### Common Join Patterns

#### Castaway-centric analysis
```sql
-- Join multiple feature categories for a castaway
SELECT cp.full_name, cp.gender, cp.occupation,
       COUNT(ch.challenge_performance_key) as challenges_participated,
       COUNT(adv.advantage_strategy_key) as advantage_actions,
       AVG(vd.vote_correct) as vote_accuracy
FROM silver.castaway_profile cp
LEFT JOIN silver.challenge_performance ch ON ch.castaway_id = cp.castaway_id
LEFT JOIN silver.advantage_strategy adv ON adv.castaway_id = cp.castaway_id
LEFT JOIN silver.vote_dynamics vd ON vd.castaway_id = cp.castaway_id
WHERE cp.version_season = 'US47'
GROUP BY cp.castaway_id, cp.full_name, cp.gender, cp.occupation;
```

#### Episode-level analysis
```sql
-- Combine edit and gameplay features per episode
SELECT ef.castaway_id, ef.episode, ef.confessional_count,
       ch.challenges_won, vd.votes_cast, sp.tribe_status
FROM silver.edit_features ef
LEFT JOIN silver.challenge_performance ch
  ON ch.castaway_id = ef.castaway_id AND ch.episode = ef.episode
LEFT JOIN silver.vote_dynamics vd
  ON vd.castaway_id = ef.castaway_id AND vd.episode = ef.episode
LEFT JOIN silver.social_positioning sp
  ON sp.castaway_id = ef.castaway_id AND sp.episode = ef.episode
WHERE ef.version_season = 'US47';
```

#### Season-level aggregation
```sql
-- Season context with cast composition
SELECT sc.version_season, sc.season_name, sc.cast_size,
       sc.male_ratio, sc.bipoc_ratio, sc.returnee_ratio,
       sc.has_edge_of_extinction, sc.season_era
FROM silver.season_context sc
WHERE sc.season_number >= 40;
```

---

## Gold Layer: ML-Ready Features

### Model-Ready Tables

- **ml_features_non_edit** — *Pure gameplay features* → **Grain:** 1 row per castaway × season
- **ml_features_hybrid** — *Gameplay + edit features* → **Grain:** 1 row per castaway × season

### ML Feature Usage

#### Winner prediction training data
```sql
-- Get training data for winner prediction
SELECT castaway_id, version_season,
       target_winner, target_finalist, target_jury,
       challenges_won, individual_win_rate,
       advantages_found, idol_success_rate,
       vote_accuracy_rate, majority_alliance_rate,
       is_bipoc, is_lgbt, age as current_age
FROM gold.ml_features_non_edit
WHERE target_placement IS NOT NULL;
```

#### Feature correlation analysis
```sql
-- Compare feature importance between non-edit and hybrid models
SELECT
  CORR(challenges_won, target_winner) as challenge_correlation,
  CORR(vote_accuracy_rate, target_winner) as vote_correlation,
  CORR(advantages_found, target_winner) as advantage_correlation
FROM gold.ml_features_non_edit
WHERE target_winner IS NOT NULL

UNION ALL

SELECT
  CORR(challenges_won, target_winner) as challenge_correlation,
  CORR(vote_accuracy_rate, target_winner) as vote_correlation,
  CORR(total_confessional_count, target_winner) as edit_correlation
FROM gold.ml_features_hybrid
WHERE target_winner IS NOT NULL;
```

#### Strategic archetype clustering
```sql
-- Features for player archetype analysis
SELECT castaway_id, version_season,
       -- Physical game strength
       individual_win_rate,
       strength_wins + endurance_wins as physical_score,
       -- Strategic game strength
       advantages_found,
       vote_accuracy_rate,
       idol_success_rate,
       -- Social game strength
       avg_same_gender_ratio,
       majority_alliance_rate,
       original_tribe_jury_support_rate
FROM gold.ml_features_non_edit
WHERE target_jury = 1 OR target_finalist = 1;
```

---

## Quick Reference: Key Relationships

### Core Identifiers
- **castaway_id** + **version_season** = unique person × season combination
- **episode** = episode number within season (1-based)
- **merge_phase** = 'pre_merge' or 'post_merge' strategic context

### Feature Categories
- **Demographics**: castaway_profile (age, gender, race, occupation, etc.)
- **Physical**: challenge_performance (wins, skills, participation)
- **Strategic**: advantage_strategy (finding, playing, timing)
- **Social**: social_positioning (tribe dynamics, minority/majority status)
- **Political**: vote_dynamics (accuracy, alliances, tribal councils)
- **Narrative**: edit_features (confessionals, screen time, story arc)
- **Relationships**: jury_analysis (endgame social bonds)
- **Context**: season_context (format, cast composition, meta-game)

### ML Pipeline
1. **Bronze** → Raw survivoR data ingestion
2. **Silver** → Strategic feature engineering by category
3. **Gold** → Aggregated ML-ready feature vectors
4. **Modeling** → Train on gold tables, predict future seasons

---

## Silver Table Details

### castaway_profile
**Key features**: Demographics, occupation, personality, season context
```sql
SELECT castaway_id, full_name, gender, race, ethnicity, occupation,
       age, personality_type, bipoc, lgbt, season_location
FROM silver.castaway_profile;
```

### challenge_performance
**Key features**: Challenge wins by type and skill, participation rates
```sql
SELECT castaway_id, episode, challenge_format, won_flag,
       balance_win, endurance_win, puzzle_win, strength_win,
       chosen_for_reward, sit_out
FROM silver.challenge_performance;
```

### advantage_strategy
**Key features**: Finding, playing, and strategic timing of advantages
```sql
SELECT castaway_id, episode, advantage_category, event_category,
       played_successfully, played_for_self, played_for_others
FROM silver.advantage_strategy;
```

### vote_dynamics
**Key features**: Voting accuracy, alliance behavior, tribal councils
```sql
SELECT castaway_id, episode, vote_correct, in_majority_alliance,
       voting_alone, merge_phase, tribal_council_number
FROM silver.vote_dynamics;
```

### social_positioning
**Key features**: Tribe composition, demographic dynamics, social status
```sql
SELECT castaway_id, episode, tribe, merge_phase,
       same_gender_ratio, bipoc_similarity_ratio, lgbt_similarity_ratio,
       gender_status, racial_status, lgbt_status
FROM silver.social_positioning;
```

### edit_features
**Key features**: Confessional counts, screen time, edit presence
```sql
SELECT castaway_id, episode, confessional_count, confessional_time,
       confessional_count_ratio, has_confessional, over_edited_count
FROM silver.edit_features;
```

### jury_analysis
**Key features**: Jury voting patterns, endgame relationships
```sql
SELECT finalist_id, juror_id, same_original_tribe,
       pre_jury_relationship, final_tribal_performance
FROM silver.jury_analysis;
```

### season_context
**Key features**: Season format, cast composition, meta-game evolution
```sql
SELECT version_season, season_era, cast_size, male_ratio, bipoc_ratio,
       has_edge_of_extinction, has_tribe_swap, season_recency_weight
FROM silver.season_context;
```

---

## Performance Tips

- Use `castaway_id` + `version_season` for season-specific analysis
- Use `episode` + `merge_phase` for temporal analysis
- Filter by `season_era` or `season_number` for meta-game studies
- Join silver tables on `castaway_id`, `version_season`, `episode` as needed
- Gold tables are pre-aggregated for ML - use them for model training
- Silver tables are for feature engineering and strategic analysis
