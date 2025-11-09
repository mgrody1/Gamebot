# Gamebot for Data Analysts & Scientists

**Quick Start**: [Install gamebot-lite](#installation) → [Load data](#basic-usage) → [Explore tables](#available-data) → [Advanced queries](#advanced-analytics)

This guide covers everything analysts need to get productive with Survivor data analysis using Gamebot Lite.

---

## Installation

**Recommended**: Install with pandas for immediate data access:
```bash
pip install gamebot-lite
```

**For SQL Analytics**: Add DuckDB for complex queries on larger datasets:
```bash
pip install gamebot-lite[duckdb]
```

**Requirements**: Python 3.8+, no other dependencies needed.

---

## Basic Usage

### Load Any Table
```python
from gamebot_lite import load_table

# Load as pandas DataFrame
vote_history = load_table("vote_history_curated")
jury_votes = load_table("jury_votes")
castaways = load_table("castaways")

# Explore structure
print(vote_history.columns.tolist())
print(f"Rows: {len(vote_history)}")
```

### SQL-Style Queries (Optional)
If you installed the `duckdb` extra, you can run complex SQL queries:

```python
from gamebot_lite import duckdb_query

# Find seasons with the most split votes
results = duckdb_query("""
    SELECT
      version_season,
      COUNT(episode) as count_split_vote_tribals
    FROM bronze.vote_history
    WHERE split_vote IS NOT NULL
      AND split_vote != 'No'
    GROUP BY version_season
    ORDER BY version_season
""")
```

---

## Available Data

Gamebot Lite contains 21 bronze tables, 8 silver feature tables, and 2 gold ML-ready matrices, organized in three layers:

### Bronze Layer (Raw Data)
Direct mirrors of the survivoR dataset with minimal processing:

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `ingestion_runs` | Ingestion run metadata | `run_id`, `environment`, `run_started_at` |
| `dataset_versions` | Upstream dataset versioning | `dataset_name`, `signature`, `committed_at` |
| `castaway_details` | Contestant demographics & background | `castaway_id`, `version_season`, `full_name` |
| `season_summary` | Season-level metadata | `version_season`, `season`, `winner_id` |
| `advantage_details` | Advantage metadata | `version_season`, `advantage_id`, `advantage_type` |
| `challenge_description` | Challenge metadata | `version_season`, `challenge_id`, `challenge_type` |
| `challenge_summary` | Challenge outcomes summary | `version_season`, `challenge_id`, `castaway_id` |
| `episodes` | Season & episode metadata | `version_season`, `episode`, `air_date` |
| `castaways` | Contestant season participation | `castaway_id`, `version_season`, `full_name` |
| `advantage_movement` | Advantage movement and play events | `castaway_id`, `advantage_id`, `event` |
| `boot_mapping` | Boot order mapping by episode | `version_season`, `episode`, `castaway_id` |
| `boot_order` | Order of elimination | `version_season`, `boot_order_position`, `castaway_id` |
| `auction_details` | Survivor auction item details | `version_season`, `auction_num`, `item`, `castaway_id` |
| `survivor_auction` | Survivor auction summary | `version_season`, `episode`, `castaway_id` |
| `castaway_scores` | Castaway scoring metrics | `version_season`, `castaway_id`, `score_overall` |
| `journeys` | Journey and risk events | `version_season`, `episode`, `castaway_id` |
| `tribe_mapping` | Tribe membership over time | `castaway_id`, `version_season`, `episode`, `tribe` |
| `confessionals` | Edit/narrative content analysis | `castaway_id`, `version_season`, `episode` |
| `challenge_results` | Individual challenge performance | `castaway_id`, `challenge_id`, `result` |
| `vote_history` | Tribal council voting records | `castaway_id`, `vote`, `voted_out` |
| `jury_votes` | Final tribal council votes | `castaway_id`, `finalist_id`, `vote` |

### Silver Layer (Feature Engineering)
Curated feature engineering tables designed for analysis and modeling:

| Table | Description | Use Cases |
|-------|-------------|-----------|
| `advantage_strategy` | Advantage play/strategy features | Strategic gameplay analysis |
| `season_context` | Season-level context features | Season-level modeling |
| `vote_dynamics` | Voting dynamics and alliances | Strategic voting, betrayal analysis |
| `edit_features` | Edit/narrative content features | Winner prediction, edit theory |
| `jury_analysis` | Jury voting and outcome features | Jury prediction, endgame analysis |
| `castaway_profile` | Demographics + background features | Winner prediction, casting analysis |
| `social_positioning` | Social network and tribe features | Social network analysis |
| `challenge_performance` | Aggregated challenge performance | Performance analysis, challenge design |

**These tables are engineered specifically for ML and advanced analytics.**

### Gold Layer (ML-Ready Matrices)
Production-ready feature matrices for machine learning:

| Table | Description | Features |
|-------|-------------|----------|
| `ml_features_hybrid` | Season-level features per castaway (gameplay + edit) | Challenge, voting, advantage, social, edit |
| `ml_features_non_edit` | Season-level features per castaway (gameplay only) | Challenge, voting, advantage, social |

Each row in the gold tables represents one castaway-season combination with comprehensive features for winner prediction modeling.

---

## Common Analysis Patterns

### Winner Analysis
```python
# Load castaway data with winner information
castaways = load_table("castaways")

# Basic winner analysis
winners = castaways[castaways['winner'] == True]
print(f"Winners: {len(winners)} across {castaways['version_season'].nunique()} seasons")
print(f"Average winner age: {winners['age'].mean():.1f}")
```

### Voting Pattern Analysis
```python
# Load voting records
vote_history = load_table("vote_history")

# Analyze voting patterns
vote_stats = vote_history.groupby('castaway_id').agg({
    'vote': 'count',           # Total votes cast
    'nullified': 'sum',        # Votes nullified by advantages
    'tie': 'sum'               # Votes resulting in ties
}).rename(columns={'vote': 'total_votes_cast'})

# Find players with most nullified votes
print(vote_stats.sort_values('nullified', ascending=False).head(10))
```

### Challenge Performance Analysis
```python
# Load challenge results
challenge_results = load_table("challenge_results")

# Performance by challenge type
challenge_wins = challenge_results[challenge_results['result'] == 'Won'].groupby([
    'castaway_id', 'challenge_type'
]).size().reset_index(name='wins')

# Individual immunity winners
immunity_wins = challenge_wins[
    challenge_wins['challenge_type'] == 'Individual Immunity'
].sort_values('wins', ascending=False)

print("Top individual immunity challenge performers:")
print(immunity_wins.head(10))
```

---

## Advanced Analytics

### Time Series Analysis
```python
# Episode-by-episode confessional tracking
confessionals = load_table("confessionals")

# Track screen time prominence over a season
season_confessionals = confessionals[
    confessionals['version_season'] == 'US47'
].groupby(['castaway', 'episode']).agg({
    'confessional_count': 'sum',
    'confessional_time': 'sum'
}).reset_index()

# Plot confessional trajectory for specific players
import matplotlib.pyplot as plt
for castaway in ['Genevieve', 'Sam', 'Teeny']:
    player_data = season_confessionals[season_confessionals['castaway'] == castaway]
    plt.plot(player_data['episode'], player_data['confessional_count'], label=castaway)
plt.legend()
plt.xlabel('Episode')
plt.ylabel('Confessional Count')
plt.title('US47 Confessional Counts Over Time')
plt.show()
```

### Complex SQL Analytics (with DuckDB)
```python
from gamebot_lite import duckdb_query

# Multi-table analysis: jury votes vs confessional presence
jury_analysis = duckdb_query("""
WITH finalist_confessionals AS (
  SELECT
    c.castaway_id,
    c.version_season,
    c.castaway,
    SUM(conf.confessional_count) as total_confessionals,
    SUM(conf.confessional_time) as total_screen_time
  FROM bronze.castaways c
  JOIN bronze.confessionals conf
    ON c.castaway_id = conf.castaway_id
    AND c.version_season = conf.version_season
  WHERE c.finalist = TRUE
  GROUP BY c.castaway_id, c.version_season, c.castaway
),
jury_vote_counts AS (
  SELECT
    finalist_id,
    version_season,
    COUNT(*) as votes_received
  FROM bronze.jury_votes
  GROUP BY finalist_id, version_season
)
SELECT
  fc.version_season,
  fc.castaway,
  fc.total_confessionals,
  fc.total_screen_time,
  COALESCE(jv.votes_received, 0) as jury_votes,
  CASE WHEN c.winner THEN 'Winner' ELSE 'Runner-up' END as result
FROM finalist_confessionals fc
LEFT JOIN jury_vote_counts jv
  ON fc.castaway_id = jv.finalist_id
  AND fc.version_season = jv.version_season
JOIN bronze.castaways c
  ON fc.castaway_id = c.castaway_id
  AND fc.version_season = c.version_season
ORDER BY fc.version_season, jury_votes DESC
""")

print("Correlation between edit presence and jury votes:")
print(jury_analysis.corr())
```

---

## Schema Reference

### Key Relationships
- **Join Key**: `castaway_id` connects most tables
- **Season Filter**: `version_season` (format: "US43", "AUS07", etc.)
- **Episode Tracking**: `episode` for temporal analysis

### Data Quality Notes
- **Coverage**: Seasons 1-47 (US), plus international seasons
- **Missing Data**: Early seasons have limited confessional/edit data
- **Updates**: Data refreshed when new survivoR releases are available

### Data Dictionary
For complete column descriptions and data types, see the [official survivoR documentation](https://cran.r-project.org/web/packages/survivoR/survivoR.pdf).

---

## Getting Help

### Troubleshooting
- **Import Error**: Ensure `pip install gamebot-lite` completed successfully
- **Missing Table**: Check table name spelling (case-sensitive)
- **DuckDB Error**: Install with `pip install gamebot-lite[duckdb]`

### Advanced Usage
- **Custom Analysis**: Use pandas operations on loaded DataFrames
- **Large Datasets**: Use DuckDB queries for memory efficiency
- **Export Results**: Save with `df.to_csv()`, `df.to_excel()`, etc.

### Community & Support
- **Issues**: [GitHub Issues](https://github.com/mgrody1/Gamebot/issues)
- **Examples**: See `examples/` directory in the repository
- **Updates**: Follow releases for new data and features

---

## Example Analyses

### Research Applications
- **Winner Prediction**: Use bronze + silver layer features with scikit-learn
- **Game Theory**: Analyze voting coalitions using `vote_history` and `tribe_mapping`
- **Edit Analysis**: Study narrative construction vs actual gameplay using `confessionals`
- **Challenge Design**: Examine `challenge_description` and `challenge_results` patterns
- **Casting Analysis**: Demographics vs success using `castaway_details` and `castaways`
- **Advantage Strategy**: Track advantage usage with `advantage_details` and `advantage_movement`

### Academic Research
The dataset supports research in:
- Social network analysis (tribe dynamics, voting coalitions)
- Game theory and strategic decision-making (vote history, advantage play)
- Media narrative construction (confessional counts vs outcomes)
- Competitive psychology (challenge performance under pressure)
- Group dynamics and leadership (tribe mapping, jury votes)

---

## Upgrading to Full Warehouse

For teams needing:
- **Real-time updates** from survivoR releases
- **Custom feature engineering** pipelines
- **Team collaboration** via shared database
- **BI tool integration** (Tableau, PowerBI)

Consider upgrading to [Gamebot Warehouse](../README.md#gamebot-warehouse---production-deployment) for a full PostgreSQL deployment with Airflow orchestration.
