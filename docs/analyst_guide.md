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
    FROM vote_history
    WHERE split_vote IS NOT NULL
      AND split_vote != 'No'
    GROUP BY version_season
    ORDER BY version_season
""")
```

---

## Available Data

Gamebot Lite contains 30+ tables organized in three layers:

### Bronze Layer (Raw Data)
Direct mirrors of the survivoR dataset with minimal processing:

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `castaways` | Contestant demographics & background | `castaway_id`, `version_season`, `full_name` |
| `episodes` | Season & episode metadata | `version_season`, `episode`, `air_date` |
| `vote_history` | Tribal council voting records | `castaway_id`, `vote`, `voted_out` |
| `jury_votes` | Final tribal council votes | `castaway_id`, `finalist`, `vote` |
| `challenge_results` | Individual challenge performance | `castaway_id`, `challenge_id`, `result` |
| `confessionals` | Edit/narrative content analysis | `castaway_id`, `confessional_count`, `index_count` |

**+ 15 more bronze tables** covering advantages, tribe mapping, season summaries, and detailed challenge data.

### Silver Layer (ML Features)
Curated feature engineering tables designed for analysis and modeling:

| Table | Description | Use Cases |
|-------|-------------|-----------|
| `castaway_profile` | Demographics + background features | Winner prediction, casting analysis |
| `challenge_results_curated` | Aggregated challenge performance | Performance analysis, challenge design |
| `vote_history_curated` | Enhanced voting records with context | Strategic voting, betrayal analysis |
| `tribe_membership_curated` | Tribe relationships over time | Social network analysis |
| `advantage_movement_curated` | Advantage usage with outcomes | Strategic gameplay analysis |
| `confessional_summary` | Aggregated edit metrics | Winner prediction, edit theory |

**These tables don't exist in the original survivoR dataset** - they're engineered specifically for ML and advanced analytics.

### Gold Layer (ML-Ready Matrices)
Production-ready feature matrices for machine learning:

| Table | Description | Observations | Features |
|-------|-------------|--------------|----------|
| `features_castaway_season` | Season-level features per castaway | 4,248 | Challenge, voting, advantage, social, edit |
| `features_castaway_episode` | Episode-level features per castaway | 50,000+ | Temporal gameplay progression |

Each row in the season table represents one castaway-season combination with comprehensive features for winner prediction modeling.

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
  FROM castaways c
  JOIN confessionals conf
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
  FROM jury_votes
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
JOIN castaways c
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
